"""Unit coverage for deterministic LangGraph state reducers."""

from datetime import UTC, datetime

import pytest

from incidentops.investigation.models import (
    EvidenceAvailability,
    InvestigationTaskType,
    InvestigationTraceEvent,
    InvestigationTraceEventType,
    MetricEvidence,
    MetricEvidenceType,
    TraceStatus,
)
from incidentops.investigation.state import (
    add_counts,
    maximum_count,
    merge_boolean,
    merge_completed_tasks,
    merge_errors,
    merge_metric_evidence,
    merge_trace_events,
)

START = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
END = datetime(2026, 8, 1, 13, 10, tzinfo=UTC)


def build_metric(evidence_id: str, maximum: float) -> MetricEvidence:
    """Build one state-reducer fixture."""

    return MetricEvidence(
        evidence_id=evidence_id,
        metric_type=MetricEvidenceType.CONSUMER_LAG,
        observation="A bounded metric summary was collected.",
        start_time=START,
        end_time=END,
        raw_value_summary={"maximum": maximum},
        availability=EvidenceAvailability.AVAILABLE,
    )


def test_parallel_task_and_evidence_merges_are_unique_and_deterministic() -> None:
    first = build_metric("metric-z-summary", 10.0)
    second = build_metric("metric-a-summary", 20.0)

    tasks = merge_completed_tasks(
        [InvestigationTaskType.FIND_KAFKA_ERRORS],
        [
            InvestigationTaskType.CHECK_CONSUMER_LAG,
            InvestigationTaskType.FIND_KAFKA_ERRORS,
        ],
    )
    evidence = merge_metric_evidence([first], [second, first])

    assert tasks == [
        InvestigationTaskType.CHECK_CONSUMER_LAG,
        InvestigationTaskType.FIND_KAFKA_ERRORS,
    ]
    assert [item.evidence_id for item in evidence] == ["metric-a-summary", "metric-z-summary"]


def test_evidence_merge_rejects_conflicting_stable_identifiers() -> None:
    with pytest.raises(ValueError, match="conflicting evidence"):
        merge_metric_evidence(
            [build_metric("metric-lag-summary", 10.0)],
            [build_metric("metric-lag-summary", 20.0)],
        )


def test_later_recheck_evidence_replaces_the_same_stable_identifier() -> None:
    first = build_metric("metric-lag-summary", 10.0)
    second = build_metric("metric-lag-summary", 20.0).model_copy(update={"collection_attempt": 2})

    merged = merge_metric_evidence([first], [second])

    assert merged == [second]


def test_counter_boolean_and_error_reducers_preserve_bounded_state() -> None:
    assert add_counts(2, 3) == 5
    assert maximum_count(1, 2) == 2
    assert merge_boolean(False, True) is True
    assert merge_errors(["z", "a"], ["a", "b"]) == ["a", "b", "z"]


def test_trace_merge_uses_event_identifier_and_timestamp_order() -> None:
    later = InvestigationTraceEvent(
        event_id="trace-002",
        timestamp=END,
        event_type=InvestigationTraceEventType.NODE_COMPLETED,
        investigation_id="investigation-001",
        status=TraceStatus.COMPLETED,
        node="collect_metrics",
    )
    earlier = InvestigationTraceEvent(
        event_id="trace-001",
        timestamp=START,
        event_type=InvestigationTraceEventType.NODE_STARTED,
        investigation_id="investigation-001",
        status=TraceStatus.STARTED,
        node="collect_metrics",
    )

    merged = merge_trace_events([later], [earlier, later])

    assert [item.event_id for item in merged] == ["trace-001", "trace-002"]
