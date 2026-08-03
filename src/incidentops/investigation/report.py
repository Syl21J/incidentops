"""Deterministic incident report assembly and Markdown rendering."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from incidentops.investigation.models import (
    EvidenceAvailability,
    IncidentReport,
    IncidentStatus,
    InvestigationArtifactPaths,
    InvestigationTraceEvent,
    LogEvidence,
    MetricEvidence,
    RecommendedAction,
    RecommendedActionCode,
    RootCauseCode,
    RootCauseHypothesis,
    VerificationDecision,
)
from incidentops.investigation.state import InvestigationState


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
    return [
        RecommendedAction(
            action_code=RecommendedActionCode.COLLECT_MORE_EVIDENCE,
            reason="Collect another bounded evidence window before taking action.",
            supporting_evidence_ids=evidence_ids,
        )
    ]


def _sanitize_hypothesis(
    hypothesis: RootCauseHypothesis,
    existing_evidence_ids: set[str],
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
    primary = (
        _sanitize_hypothesis(hypotheses[0], existing_evidence_ids, verified=True)
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

    limitations = list(state.get("errors", []))
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
    if status != IncidentStatus.DIAGNOSED:
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
        recommended_actions=actions,
        limitations=limitations,
        tool_call_count=state.get("tool_call_count", 0),
        model_call_count=state.get("model_call_count", 0),
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
        f"- Root cause: `{root_cause}`",
        f"- Tool calls: {report.tool_call_count}",
        f"- Model calls: {report.model_call_count}",
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
    lines.extend(["", "## Recommended actions", ""])
    for action in report.recommended_actions:
        lines.append(f"- `{action.action_code.value}`: {action.reason}")
    lines.extend(["", "## Limitations", ""])
    if report.limitations:
        lines.extend(f"- {item}" for item in report.limitations)
    else:
        lines.append("- None recorded for this bounded investigation.")
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


def persist_investigation_artifacts(
    report: IncidentReport,
    trace_events: Sequence[InvestigationTraceEvent],
    directory: Path,
) -> InvestigationArtifactPaths:
    """Persist one validated JSON report and its safe JSONL trace."""

    report_path = directory / f"{report.investigation_id}.report.json"
    trace_path = directory / f"{report.investigation_id}.trace.jsonl"
    report_payload = report.model_dump_json(indent=2) + "\n"
    trace_payload = "".join(f"{event.model_dump_json()}\n" for event in trace_events)
    _write_text_atomic(report_path, report_payload)
    _write_text_atomic(trace_path, trace_payload)
    return InvestigationArtifactPaths(
        report_path=report_path,
        trace_path=trace_path,
    )


def write_report_output(path: Path, content: str) -> None:
    """Write an explicitly requested CLI report using atomic replacement."""

    _write_text_atomic(path, content)
