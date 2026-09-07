"""Replay model proposals and deterministic verification from saved evidence."""

from __future__ import annotations

import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import AwareDatetime, Field, model_validator

from incidentops.benchmark.models import BenchmarkCaseResult
from incidentops.config import Settings
from incidentops.investigation.model import StructuredModelError, create_model_provider
from incidentops.investigation.models import (
    EvidenceBundle,
    HypothesisSet,
    IncidentReport,
    IncidentStatus,
    ModelProposalArtifact,
    StrictModel,
)
from incidentops.investigation.nodes import build_deterministic_plan, build_hypothesis_messages
from incidentops.investigation.report import assemble_incident_report, build_model_proposal_artifact
from incidentops.investigation.state import InvestigationState
from incidentops.investigation.verifier import verify_investigation_state
from incidentops.scenarios import ScenarioManifest

ReplayApproach = Literal["rules", "llm", "llm-rag", "rescore"]


class EvidenceReplayResult(StrictModel):
    """One proposal and verification result produced from an immutable evidence bundle."""

    schema_version: Literal[1] = 1
    replay_id: str = Field(pattern=r"^replay-[a-f0-9]{16}$")
    approach: ReplayApproach
    evidence_bundle_id: str
    created_at: AwareDatetime
    proposal: ModelProposalArtifact
    report: IncidentReport

    @model_validator(mode="after")
    def validate_bundle_link(self) -> EvidenceReplayResult:
        """Require the nested proposal to reference the replayed evidence."""

        if self.proposal.evidence_bundle_id != self.evidence_bundle_id:
            raise ValueError("replay proposal does not reference the evidence bundle")
        return self


class EvidenceComparisonResult(StrictModel):
    """Evaluator-side comparison of rules, LLM, and LLM with saved RAG context."""

    schema_version: Literal[1] = 1
    evidence_bundle_id: str
    scenario_id: str
    created_at: AwareDatetime
    cases: list[BenchmarkCaseResult] = Field(min_length=3, max_length=3)
    replays: list[EvidenceReplayResult] = Field(min_length=3, max_length=3)


def load_evidence_bundle(path: Path) -> EvidenceBundle:
    """Load one strict replay bundle from local JSON."""

    return EvidenceBundle.model_validate_json(path.read_text(encoding="utf-8"))


def load_model_proposal(path: Path) -> ModelProposalArtifact:
    """Load one strict parsed proposal from local JSON."""

    return ModelProposalArtifact.model_validate_json(path.read_text(encoding="utf-8"))


def _base_state(
    bundle: EvidenceBundle,
    *,
    investigation_id: str,
    include_knowledge: bool,
    started_at: datetime,
) -> InvestigationState:
    request = bundle.incident_request
    if request.start_time is None or request.end_time is None:
        raise ValueError("replay evidence requires an explicit incident window")
    return {
        "investigation_id": investigation_id,
        "incident_request": request,
        "workflow_started_at": started_at,
        "start_time": request.start_time,
        "end_time": request.end_time,
        "affected_services": request.affected_services,
        "run_id": request.run_id,
        "plan": build_deterministic_plan(),
        "completed_tasks": bundle.completed_tasks,
        "metric_evidence": bundle.metric_evidence,
        "log_evidence": bundle.log_evidence,
        "negative_evidence": bundle.negative_evidence,
        "knowledge_references": bundle.knowledge_references if include_knowledge else [],
        "knowledge_errors": [],
        "hypotheses": [],
        "tool_call_count": 0,
        "model_call_count": 0,
        "knowledge_retrieval_count": 0,
        "investigation_attempts": bundle.collection_attempts,
        "recheck_requested": False,
        "errors": [],
        "trace_events": [],
        "terminal_status": None,
    }


def _verify_and_report(state: InvestigationState, *, completed_at: datetime) -> IncidentReport:
    state["verification_result"] = verify_investigation_state(state)
    return assemble_incident_report(state, completed_at=completed_at)


def replay_bundle(
    bundle: EvidenceBundle,
    settings: Settings,
    *,
    approach: Literal["rules", "llm", "llm-rag"],
) -> EvidenceReplayResult:
    """Generate and verify a fresh proposal without querying telemetry backends."""

    started_at = datetime.now(UTC)
    investigation_id = f"investigation-{uuid4().hex[:16]}"
    include_knowledge = approach == "llm-rag"
    state = _base_state(
        bundle,
        investigation_id=investigation_id,
        include_knowledge=include_knowledge,
        started_at=started_at,
    )
    provider_name = "deterministic-test" if approach == "rules" else "openai-compatible"
    run_settings = settings.model_copy(update={"llm_provider": provider_name})
    provider = create_model_provider(run_settings)
    calls_before = provider.call_count
    try:
        response = provider.invoke_structured(HypothesisSet, build_hypothesis_messages(state))
        state["hypotheses"] = sorted(
            response.hypotheses,
            key=lambda item: (-item.confidence, item.cause_code.value),
        )
    except StructuredModelError as error:
        state["terminal_status"] = IncidentStatus.PIPELINE_ERROR
        state["errors"] = [str(error)]
    state["model_call_count"] = 0 if approach == "rules" else provider.call_count - calls_before
    completed_at = datetime.now(UTC)
    report = _verify_and_report(state, completed_at=completed_at)
    proposal = build_model_proposal_artifact(
        state,
        bundle,
        model_provider=provider_name,
        knowledge_mode="required" if include_knowledge else "disabled",
        created_at=completed_at,
    )
    return EvidenceReplayResult(
        replay_id=f"replay-{uuid4().hex[:16]}",
        approach=approach,
        evidence_bundle_id=bundle.bundle_id,
        created_at=completed_at,
        proposal=proposal,
        report=report,
    )


def rescore_proposal(
    bundle: EvidenceBundle,
    proposal: ModelProposalArtifact,
) -> EvidenceReplayResult:
    """Run deterministic verification again without invoking any model."""

    if proposal.evidence_bundle_id != bundle.bundle_id:
        raise ValueError("proposal and evidence bundle identifiers do not match")
    started_at = datetime.now(UTC)
    investigation_id = f"investigation-{uuid4().hex[:16]}"
    state = _base_state(
        bundle,
        investigation_id=investigation_id,
        include_knowledge=proposal.knowledge_mode == "required",
        started_at=started_at,
    )
    state["hypotheses"] = proposal.hypotheses
    if not proposal.structured_response_valid:
        state["terminal_status"] = IncidentStatus.PIPELINE_ERROR
        state["errors"] = proposal.model_errors or ["saved model proposal is not valid"]
    completed_at = datetime.now(UTC)
    report = _verify_and_report(state, completed_at=completed_at)
    return EvidenceReplayResult(
        replay_id=f"replay-{uuid4().hex[:16]}",
        approach="rescore",
        evidence_bundle_id=bundle.bundle_id,
        created_at=completed_at,
        proposal=proposal,
        report=report,
    )


def compare_bundle(
    bundle: EvidenceBundle,
    manifest: ScenarioManifest,
    settings: Settings,
) -> EvidenceComparisonResult:
    """Run all three approaches on one bundle, then evaluate outside the model boundary."""

    from incidentops.benchmark.runner import InvestigationExecution, build_case_result

    replays = [
        replay_bundle(bundle, settings, approach="rules"),
        replay_bundle(bundle, settings, approach="llm"),
        replay_bundle(bundle, settings, approach="llm-rag"),
    ]
    cases = [
        build_case_result(
            InvestigationExecution(
                report=replay.report,
                evidence_bundle=bundle,
                model_proposal=replay.proposal,
            ),
            manifest,
            "required" if replay.approach == "llm-rag" else "disabled",
        )
        for replay in replays
    ]
    return EvidenceComparisonResult(
        evidence_bundle_id=bundle.bundle_id,
        scenario_id=manifest.id,
        created_at=datetime.now(UTC),
        cases=cases,
        replays=replays,
    )


def write_replay_result(
    path: Path,
    result: EvidenceReplayResult | EvidenceComparisonResult,
) -> None:
    """Atomically persist one local replay result."""

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
            handle.write(result.model_dump_json(indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            temporary_path = Path(handle.name)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()
