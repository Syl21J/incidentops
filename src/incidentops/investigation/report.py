"""Deterministic incident report assembly and Markdown rendering."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from incidentops.investigation.models import (
    EvidenceAvailability,
    EvidenceBundle,
    IncidentReport,
    IncidentRequest,
    IncidentStatus,
    InvestigationArtifactPaths,
    InvestigationTraceEvent,
    LogEvidence,
    MetricEvidence,
    ModelProposalArtifact,
    ModelProviderKind,
    ProposalKnowledgeMode,
    RecommendedAction,
    RecommendedActionCode,
    RootCauseCode,
    RootCauseHypothesis,
    VerificationDecision,
)
from incidentops.investigation.state import InvestigationState

MODEL_ERROR_CATEGORIES = frozenset(
    {
        "quota_exceeded",
        "timeout",
        "request_rejected",
        "invalid_structured_response",
        "request_failed",
        "call_limit",
    }
)
PROVIDER_UNAVAILABLE_CATEGORIES = MODEL_ERROR_CATEGORIES - {"invalid_structured_response"}


def _report_status(state: InvestigationState) -> IncidentStatus:
    terminal_status = state.get("terminal_status")
    if terminal_status is not None:
        return terminal_status
    verification = state.get("verification_result")
    if verification is None:
        return IncidentStatus.PIPELINE_ERROR
    if verification.decision == VerificationDecision.ACCEPTED:
        return IncidentStatus.DIAGNOSED
    if any("conflict" in issue.lower() for issue in verification.issues):
        return IncidentStatus.CONFLICTING_EVIDENCE
    return IncidentStatus.INSUFFICIENT_EVIDENCE


def _actions_for_cause(
    cause_code: RootCauseCode,
    evidence_ids: list[str],
) -> list[RecommendedAction]:
    if cause_code == RootCauseCode.SLOW_CONSUMER_PROCESSING:
        return [
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_CONSUMER_PROCESSING,
                reason="Inspect the consumer processing path identified by verified evidence.",
                supporting_evidence_ids=evidence_ids,
            ),
            RecommendedAction(
                action_code=RecommendedActionCode.REDUCE_PROCESSING_LATENCY,
                reason="Reduce latency in the verified slow processing path.",
                supporting_evidence_ids=evidence_ids,
            ),
            RecommendedAction(
                action_code=RecommendedActionCode.TEMPORARILY_SCALE_CONSUMERS,
                reason="Consider temporary consumer scaling after human review.",
                supporting_evidence_ids=evidence_ids,
            ),
        ]
    if cause_code == RootCauseCode.DATABASE_LATENCY:
        return [
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_DATABASE_LATENCY,
                reason="Inspect database latency using the verified database evidence.",
                supporting_evidence_ids=evidence_ids,
            )
        ]
    if cause_code == RootCauseCode.KAFKA_BROKER_FAILURE:
        return [
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_KAFKA_HEALTH,
                reason="Inspect Kafka health using the verified broker evidence.",
                supporting_evidence_ids=evidence_ids,
            )
        ]
    if cause_code == RootCauseCode.TRAFFIC_SPIKE:
        return [
            RecommendedAction(
                action_code=RecommendedActionCode.TEMPORARILY_SCALE_CONSUMERS,
                reason="Consider temporary consumer scaling after confirming the observed surge.",
                supporting_evidence_ids=evidence_ids,
            )
        ]
    if cause_code == RootCauseCode.MALFORMED_EVENT:
        return [
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_INVALID_EVENTS,
                reason="Inspect the bounded invalid-event evidence and producer contract.",
                supporting_evidence_ids=evidence_ids,
            )
        ]
    return [
        RecommendedAction(
            action_code=RecommendedActionCode.COLLECT_MORE_EVIDENCE,
            reason="Collect another bounded evidence window before taking action.",
            supporting_evidence_ids=evidence_ids,
        )
    ]


def _diagnosis_scope_limitation(cause_code: RootCauseCode) -> str | None:
    """State what the accepted cause category still does not establish."""

    limitations = {
        RootCauseCode.SLOW_CONSUMER_PROCESSING: (
            "The evidence identifies slow consumer processing but not the exact code path or "
            "operation responsible."
        ),
        RootCauseCode.DATABASE_LATENCY: (
            "The evidence localizes latency to database operations but not a specific query, "
            "lock, or resource constraint."
        ),
        RootCauseCode.KAFKA_BROKER_FAILURE: (
            "The evidence identifies Kafka failures but not the failing broker component."
        ),
        RootCauseCode.TRAFFIC_SPIKE: (
            "The evidence establishes a producer traffic surge but not its upstream trigger."
        ),
        RootCauseCode.MALFORMED_EVENT: (
            "The evidence establishes malformed input but not the producing client or source."
        ),
    }
    return limitations.get(cause_code)


def _sanitize_hypothesis(
    hypothesis: RootCauseHypothesis,
    existing_evidence_ids: set[str],
    existing_knowledge_ids: set[str],
    *,
    verified: bool,
) -> RootCauseHypothesis | None:
    supporting = [
        evidence_id
        for evidence_id in hypothesis.supporting_evidence_ids
        if evidence_id in existing_evidence_ids
    ]
    contradicting = [
        evidence_id
        for evidence_id in hypothesis.contradicting_evidence_ids
        if evidence_id in existing_evidence_ids
    ]
    if hypothesis.cause_code != RootCauseCode.INSUFFICIENT_EVIDENCE and not supporting:
        return None
    reasoning_summary = (
        "Deterministic verification accepted the cited structured evidence."
        if verified
        else "This bounded alternative was not selected by deterministic verification."
    )
    return hypothesis.model_copy(
        update={
            "supporting_evidence_ids": supporting,
            "contradicting_evidence_ids": contradicting,
            "knowledge_reference_ids": [
                reference_id
                for reference_id in hypothesis.knowledge_reference_ids
                if reference_id in existing_knowledge_ids
            ],
            "reasoning_summary": reasoning_summary,
        }
    )


def assemble_incident_report(
    state: InvestigationState,
    *,
    completed_at: datetime | None = None,
) -> IncidentReport:
    """Build a factual report exclusively from verified state fields."""

    completed = (completed_at or datetime.now(UTC)).astimezone(UTC)
    status = _report_status(state)
    verification = state.get("verification_result")
    hypotheses = state.get("hypotheses", [])
    existing_evidence_ids = {
        item.evidence_id
        for item in [
            *state.get("metric_evidence", []),
            *state.get("log_evidence", []),
            *state.get("negative_evidence", []),
        ]
    }
    knowledge_references = list(state.get("knowledge_references", []))[:10]
    existing_knowledge_ids = {item.knowledge_reference_id for item in knowledge_references}
    primary = (
        _sanitize_hypothesis(
            hypotheses[0],
            existing_evidence_ids,
            existing_knowledge_ids,
            verified=True,
        )
        if status == IncidentStatus.DIAGNOSED and hypotheses
        else None
    )
    alternative_candidates = hypotheses[1:] if primary is not None else hypotheses
    alternatives = [
        sanitized
        for hypothesis in alternative_candidates
        if (
            sanitized := _sanitize_hypothesis(
                hypothesis,
                existing_evidence_ids,
                existing_knowledge_ids,
                verified=False,
            )
        )
        is not None
    ][:2]

    verified_ids = set(verification.verified_evidence_ids if verification is not None else [])
    positive_evidence: list[MetricEvidence | LogEvidence] = [
        item
        for item in [*state.get("metric_evidence", []), *state.get("log_evidence", [])]
        if item.evidence_id in verified_ids and item.availability == EvidenceAvailability.AVAILABLE
    ]
    positive_evidence.sort(key=lambda item: item.evidence_id)
    negative_evidence = sorted(
        (
            item
            for item in state.get("negative_evidence", [])
            if item.availability == EvidenceAvailability.AVAILABLE
        ),
        key=lambda item: item.evidence_id,
    )

    action_evidence_ids = [item.evidence_id for item in positive_evidence]
    cause_code = (
        verification.selected_cause
        if verification is not None and verification.selected_cause is not None
        else RootCauseCode.INSUFFICIENT_EVIDENCE
    )
    actions = _actions_for_cause(cause_code, action_evidence_ids)
    verification_issues = list(
        dict.fromkeys(verification.issues if verification is not None else [])
    )[:20]

    limitations = [*state.get("errors", []), *state.get("knowledge_errors", [])]
    unavailable = [
        item.evidence_id
        for item in [
            *state.get("metric_evidence", []),
            *state.get("log_evidence", []),
            *state.get("negative_evidence", []),
        ]
        if item.availability == EvidenceAvailability.UNAVAILABLE
    ]
    if unavailable:
        limitations.append("Some required bounded evidence remained unavailable.")
    if state.get("investigation_attempts", 1) > 1:
        limitations.append("The workflow used its single targeted recheck.")
    if status == IncidentStatus.DIAGNOSED:
        if scope_limitation := _diagnosis_scope_limitation(cause_code):
            limitations.append(scope_limitation)
    else:
        limitations.append("No root cause passed deterministic verification.")
    limitations = list(dict.fromkeys(limitations))[:20]

    incident_summary = (
        f"The bounded investigation diagnosed {cause_code.value.replace('_', ' ')}."
        if status == IncidentStatus.DIAGNOSED
        else "The bounded investigation did not establish a verified root cause."
    )
    return IncidentReport(
        investigation_id=state.get("investigation_id", "investigation-unknown"),
        status=status,
        incident_summary=incident_summary,
        primary_root_cause=primary,
        alternative_hypotheses=alternatives,
        supporting_evidence=positive_evidence,
        negative_evidence=negative_evidence,
        knowledge_references=knowledge_references,
        recommended_actions=actions,
        verification_issues=verification_issues,
        limitations=limitations,
        tool_call_count=state.get("tool_call_count", 0),
        model_call_count=state.get("model_call_count", 0),
        knowledge_retrieval_count=state.get("knowledge_retrieval_count", 0),
        investigation_attempts=state.get("investigation_attempts", 1),
        started_at=state.get("workflow_started_at", completed),
        completed_at=completed,
    )


def render_report_markdown(report: IncidentReport) -> str:
    """Render a stable human-readable view of the validated report."""

    root_cause = (
        report.primary_root_cause.cause_code.value
        if report.primary_root_cause is not None
        else "not established"
    )
    lines = [
        f"# Incident investigation {report.investigation_id}",
        "",
        f"- Status: `{report.status.value}`",
        f"- Accepted cause category: `{root_cause}`",
        f"- Tool calls: {report.tool_call_count}",
        f"- Model calls: {report.model_call_count}",
        f"- Knowledge retrievals: {report.knowledge_retrieval_count}",
        f"- Investigation attempts: {report.investigation_attempts}",
        "",
        "## Summary",
        "",
        report.incident_summary,
        "",
        "## Supporting evidence",
        "",
    ]
    if report.supporting_evidence:
        for item in report.supporting_evidence:
            raw = json.dumps(item.raw_value_summary, sort_keys=True, ensure_ascii=True)
            lines.append(f"- `{item.evidence_id}`: {item.observation} `{raw}`")
    else:
        lines.append("- No evidence passed deterministic verification.")
    lines.extend(["", "## Negative evidence", ""])
    if report.negative_evidence:
        for item in report.negative_evidence:
            lines.append(f"- `{item.evidence_id}`: {item.observation}")
    else:
        lines.append("- No negative evidence was available.")
    lines.extend(["", "## Knowledge references", ""])
    if report.knowledge_references:
        for item in report.knowledge_references:
            lines.append(
                f"- `{item.knowledge_reference_id}`: {item.title} "
                f"(`{item.document_id}`, `{item.chunk_id}`)"
            )
    else:
        lines.append("- No knowledge context was retrieved.")
    lines.extend(["", "## Recommended actions", ""])
    for action in report.recommended_actions:
        lines.append(f"- `{action.action_code.value}`: {action.reason}")
    lines.extend(["", "## Verification issues", ""])
    if report.verification_issues:
        lines.extend(f"- {item}" for item in report.verification_issues)
    else:
        lines.append("- None recorded.")
    lines.extend(["", "## Limitations", ""])
    if report.limitations:
        lines.extend(f"- {item}" for item in report.limitations)
    else:
        lines.append("- None recorded for this bounded investigation.")
    return "\n".join(lines) + "\n"


def render_report_summary(report: IncidentReport) -> str:
    """Render a compact terminal summary without exposing raw model responses."""

    root_cause = (
        report.primary_root_cause.cause_code.value
        if report.primary_root_cause is not None
        else "not_established"
    )
    rows = [
        ("Status", report.status.value),
        ("Diagnosis", root_cause),
        (
            "Evidence",
            f"{len(report.supporting_evidence)} positive, "
            f"{len(report.negative_evidence)} negative",
        ),
        ("Knowledge", str(len(report.knowledge_references))),
        ("Calls", f"{report.model_call_count} model, {report.tool_call_count} tool"),
    ]
    width = max(len(label) for label, _ in rows)
    lines = ["Investigation result", *(f"{label:<{width}} | {value}" for label, value in rows)]
    if report.verification_issues:
        lines.extend(
            ["Verification issues:", *(f"- {issue}" for issue in report.verification_issues)]
        )
    model_errors = [
        item
        for item in report.limitations
        if any(item.startswith(f"{category}:") for category in (
            "quota_exceeded",
            "timeout",
            "request_rejected",
            "invalid_structured_response",
            "request_failed",
            "call_limit",
        ))
    ]
    if model_errors:
        lines.extend(["Model errors:", *(f"- {error}" for error in model_errors)])
    return "\n".join(lines) + "\n"


def _write_text_atomic(path: Path, content: str) -> None:
    """Atomically replace one generated local artifact."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def build_evidence_bundle(
    state: InvestigationState,
    *,
    created_at: datetime | None = None,
) -> EvidenceBundle:
    """Capture model-visible observations without evaluator-only scenario data."""

    request = state.get("incident_request")
    start_time = state.get("start_time")
    end_time = state.get("end_time")
    investigation_id = state.get("investigation_id")
    if request is None or start_time is None or end_time is None or investigation_id is None:
        raise ValueError("validated investigation state is required for an evidence bundle")
    normalized_request = IncidentRequest(
        description=request.description,
        start_time=start_time,
        end_time=end_time,
        affected_services=state.get("affected_services", request.affected_services),
        run_id=state.get("run_id"),
    )
    content = {
        "incident_request": normalized_request.model_dump(mode="json"),
        "completed_tasks": [item.value for item in state.get("completed_tasks", [])],
        "metric_evidence": [
            item.model_dump(mode="json") for item in state.get("metric_evidence", [])
        ],
        "log_evidence": [
            item.model_dump(mode="json") for item in state.get("log_evidence", [])
        ],
        "negative_evidence": [
            item.model_dump(mode="json") for item in state.get("negative_evidence", [])
        ],
        "knowledge_references": [
            item.model_dump(mode="json") for item in state.get("knowledge_references", [])
        ],
    }
    digest = sha256(json.dumps(content, sort_keys=True).encode("utf-8")).hexdigest()[:20]
    return EvidenceBundle(
        bundle_id=f"evidence-{digest}",
        source_investigation_id=investigation_id,
        created_at=(created_at or datetime.now(UTC)),
        incident_request=normalized_request,
        completed_tasks=state.get("completed_tasks", []),
        metric_evidence=state.get("metric_evidence", []),
        log_evidence=state.get("log_evidence", []),
        negative_evidence=state.get("negative_evidence", []),
        knowledge_references=state.get("knowledge_references", []),
        collection_tool_calls=state.get("tool_call_count", 0),
        collection_attempts=state.get("investigation_attempts", 1),
    )


def build_model_proposal_artifact(
    state: InvestigationState,
    evidence_bundle: EvidenceBundle,
    *,
    model_provider: ModelProviderKind,
    knowledge_mode: ProposalKnowledgeMode,
    created_at: datetime | None = None,
) -> ModelProposalArtifact:
    """Capture only validated model fields and safe errors before report filtering."""

    investigation_id = state.get("investigation_id")
    if investigation_id is None:
        raise ValueError("investigation identifier is required for a model proposal")
    model_errors = [
        item
        for item in state.get("errors", [])
        if item.split(":", maxsplit=1)[0] in MODEL_ERROR_CATEGORIES
    ][:4]
    error_categories = {item.split(":", maxsplit=1)[0] for item in model_errors}
    recorded_call_count = state.get("model_call_count", 0)
    model_invoked = model_provider != "deterministic-test" and recorded_call_count > 0
    model_call_count = recorded_call_count if model_invoked else 0
    provider_available = (
        not bool(error_categories & PROVIDER_UNAVAILABLE_CATEGORIES)
        if model_invoked
        else None
    )
    hypotheses = state.get("hypotheses", [])
    return ModelProposalArtifact(
        proposal_id=f"{investigation_id}-proposal",
        investigation_id=investigation_id,
        evidence_bundle_id=evidence_bundle.bundle_id,
        created_at=(created_at or datetime.now(UTC)),
        model_provider=model_provider,
        knowledge_mode=knowledge_mode,
        model_invoked=model_invoked,
        provider_available=provider_available,
        structured_response_valid=bool(hypotheses),
        hypotheses=hypotheses,
        model_errors=model_errors,
        model_call_count=model_call_count,
    )


def persist_investigation_artifacts(
    report: IncidentReport,
    trace_events: Sequence[InvestigationTraceEvent],
    directory: Path,
    *,
    evidence_bundle: EvidenceBundle | None = None,
    model_proposal: ModelProposalArtifact | None = None,
) -> InvestigationArtifactPaths:
    """Persist validated report, trace, and optional replay/audit artifacts."""

    report_path = directory / f"{report.investigation_id}.report.json"
    trace_path = directory / f"{report.investigation_id}.trace.jsonl"
    evidence_path = (
        directory / f"{report.investigation_id}.evidence.json"
        if evidence_bundle is not None
        else None
    )
    proposal_path = (
        directory / f"{report.investigation_id}.proposal.json"
        if model_proposal is not None
        else None
    )
    report_payload = report.model_dump_json(indent=2) + "\n"
    trace_payload = "".join(f"{event.model_dump_json()}\n" for event in trace_events)
    _write_text_atomic(report_path, report_payload)
    _write_text_atomic(trace_path, trace_payload)
    if evidence_path is not None and evidence_bundle is not None:
        _write_text_atomic(evidence_path, evidence_bundle.model_dump_json(indent=2) + "\n")
    if proposal_path is not None and model_proposal is not None:
        _write_text_atomic(proposal_path, model_proposal.model_dump_json(indent=2) + "\n")
    return InvestigationArtifactPaths(
        report_path=report_path,
        trace_path=trace_path,
        evidence_path=evidence_path,
        proposal_path=proposal_path,
    )


def write_report_output(path: Path, content: str) -> None:
    """Write an explicitly requested CLI report using atomic replacement."""

    _write_text_atomic(path, content)
