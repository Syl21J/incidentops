"""Typed LangGraph state and deterministic parallel-branch reducers."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, TypedDict

from incidentops.investigation.models import (
    IncidentReport,
    IncidentRequest,
    IncidentStatus,
    InvestigationPlan,
    InvestigationTaskType,
    InvestigationTraceEvent,
    LogEvidence,
    MetricEvidence,
    NegativeEvidence,
    RootCauseHypothesis,
    ServiceName,
    VerificationResult,
)


def merge_completed_tasks(
    left: list[InvestigationTaskType],
    right: list[InvestigationTaskType],
) -> list[InvestigationTaskType]:
    """Merge task completion from parallel branches without duplicates."""

    return sorted(set(left) | set(right), key=lambda item: item.value)


def _merge_evidence_by_id[T: MetricEvidence | LogEvidence | NegativeEvidence](
    left: list[T],
    right: list[T],
) -> list[T]:
    merged = {item.evidence_id: item for item in left}
    for item in right:
        existing = merged.get(item.evidence_id)
        if existing is not None and existing != item:
            if item.collection_attempt > existing.collection_attempt:
                merged[item.evidence_id] = item
                continue
            if item.collection_attempt < existing.collection_attempt:
                continue
            raise ValueError(f"conflicting evidence for identifier {item.evidence_id}")
        merged[item.evidence_id] = item
    return [merged[evidence_id] for evidence_id in sorted(merged)]


def merge_metric_evidence(
    left: list[MetricEvidence],
    right: list[MetricEvidence],
) -> list[MetricEvidence]:
    """Merge Prometheus evidence by stable identifier."""

    return _merge_evidence_by_id(left, right)


def merge_log_evidence(
    left: list[LogEvidence],
    right: list[LogEvidence],
) -> list[LogEvidence]:
    """Merge positive Elasticsearch evidence by stable identifier."""

    return _merge_evidence_by_id(left, right)


def merge_negative_evidence(
    left: list[NegativeEvidence],
    right: list[NegativeEvidence],
) -> list[NegativeEvidence]:
    """Merge explicit zero-result checks by stable identifier."""

    return _merge_evidence_by_id(left, right)


def add_counts(left: int, right: int) -> int:
    """Add independent branch-local tool-call increments."""

    return left + right


def maximum_count(left: int, right: int) -> int:
    """Preserve the highest global attempt number reported by any branch."""

    return max(left, right)


def merge_boolean(left: bool, right: bool) -> bool:
    """Preserve a recheck request emitted by either branch."""

    return left or right


def merge_terminal_status(
    left: IncidentStatus | None,
    right: IncidentStatus | None,
) -> IncidentStatus | None:
    """Merge terminal branch outcomes with pipeline errors taking precedence."""

    priority = {
        None: 0,
        IncidentStatus.INSUFFICIENT_EVIDENCE: 1,
        IncidentStatus.CONFLICTING_EVIDENCE: 2,
        IncidentStatus.OUT_OF_SCOPE: 3,
        IncidentStatus.PIPELINE_ERROR: 4,
        IncidentStatus.DIAGNOSED: 5,
    }
    return left if priority[left] >= priority[right] else right


def merge_errors(left: list[str], right: list[str]) -> list[str]:
    """Merge safe error summaries deterministically."""

    return sorted(set(left) | set(right))


def merge_trace_events(
    left: list[InvestigationTraceEvent],
    right: list[InvestigationTraceEvent],
) -> list[InvestigationTraceEvent]:
    """Merge local trace events without depending on branch completion order."""

    merged = {item.event_id: item for item in left}
    for item in right:
        existing = merged.get(item.event_id)
        if existing is not None and existing != item:
            raise ValueError(f"conflicting trace event for identifier {item.event_id}")
        merged[item.event_id] = item
    return sorted(merged.values(), key=lambda item: (item.timestamp, item.event_id))


class InvestigationState(TypedDict, total=False):
    """Shared bounded state consumed by the first LangGraph workflow."""

    investigation_id: str
    incident_request: IncidentRequest
    workflow_started_at: datetime
    start_time: datetime
    end_time: datetime
    affected_services: list[ServiceName]
    run_id: str | None

    plan: InvestigationPlan
    completed_tasks: Annotated[list[InvestigationTaskType], merge_completed_tasks]

    metric_evidence: Annotated[list[MetricEvidence], merge_metric_evidence]
    log_evidence: Annotated[list[LogEvidence], merge_log_evidence]
    negative_evidence: Annotated[list[NegativeEvidence], merge_negative_evidence]

    hypotheses: list[RootCauseHypothesis]
    verification_result: VerificationResult
    final_report: IncidentReport
    terminal_status: Annotated[IncidentStatus | None, merge_terminal_status]

    tool_call_count: Annotated[int, add_counts]
    model_call_count: Annotated[int, add_counts]
    investigation_attempts: Annotated[int, maximum_count]
    recheck_requested: Annotated[bool, merge_boolean]
    errors: Annotated[list[str], merge_errors]
    trace_events: Annotated[list[InvestigationTraceEvent], merge_trace_events]
