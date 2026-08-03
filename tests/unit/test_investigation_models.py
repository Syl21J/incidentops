"""Unit coverage for strict investigation domain models."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from incidentops.investigation.models import (
    EvaluationResult,
    EvidenceAvailability,
    IncidentReport,
    IncidentRequest,
    IncidentStatus,
    InvestigationPlan,
    InvestigationTask,
    InvestigationTaskType,
    InvestigationTraceEvent,
    InvestigationTraceEventType,
    InvestigationWindow,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidence,
    NegativeEvidenceType,
    RecommendedAction,
    RecommendedActionCode,
    RootCauseCode,
    RootCauseHypothesis,
    ServiceName,
    TraceStatus,
    VerificationDecision,
    VerificationResult,
    evidence_id_for_task,
)

START = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
END = datetime(2026, 8, 1, 13, 10, tzinfo=UTC)


def metric_evidence() -> MetricEvidence:
    """Build one reusable valid metric evidence item."""

    return MetricEvidence(
        evidence_id="metric-consumer-lag-summary",
        metric_type=MetricEvidenceType.CONSUMER_LAG,
        observation="Consumer lag increased.",
        start_time=START,
        end_time=END,
        raw_value_summary={"maximum": 59.0, "trend": "increasing"},
        availability=EvidenceAvailability.AVAILABLE,
    )


def negative_evidence() -> NegativeEvidence:
    """Build one reusable valid negative evidence item."""

    return NegativeEvidence(
        evidence_id="negative-no-database-errors",
        negative_type=NegativeEvidenceType.NO_DATABASE_ERRORS,
        observation="No database errors matched.",
        start_time=START,
        end_time=END,
        raw_value_summary={"matching_log_count": 0},
        availability=EvidenceAvailability.AVAILABLE,
    )


def hypothesis() -> RootCauseHypothesis:
    """Build one reusable valid root-cause hypothesis."""

    return RootCauseHypothesis(
        cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
        confidence=0.95,
        supporting_evidence_ids=["metric-consumer-lag-summary"],
        contradicting_evidence_ids=["negative-no-database-errors"],
        reasoning_summary="Collected evidence supports slow consumer processing.",
    )


def test_investigation_window_normalizes_to_utc_and_enforces_hard_cap() -> None:
    paris = timezone(timedelta(hours=2))
    window = InvestigationWindow(
        start_time=datetime(2026, 8, 1, 15, 0, tzinfo=paris),
        end_time=datetime(2026, 8, 1, 15, 10, tzinfo=paris),
    )

    assert window.start_time == START
    assert window.end_time == END
    assert window.start_time.tzinfo == UTC

    with pytest.raises(ValidationError, match="six hours"):
        InvestigationWindow(start_time=START, end_time=START + timedelta(hours=6, seconds=1))
    with pytest.raises(ValidationError, match="earlier"):
        InvestigationWindow(start_time=START, end_time=START)
    with pytest.raises(ValidationError):
        InvestigationWindow(start_time=datetime(2026, 8, 1, 13), end_time=END)


def test_incident_request_requires_a_complete_window_and_unique_known_services() -> None:
    request = IncidentRequest(
        description="Orders are delayed.",
        start_time=START,
        end_time=END,
        affected_services=[ServiceName.ORDER_CONSUMER],
        run_id="slow-consumer-001",
    )

    assert request.affected_services[0].value == "order-consumer"
    assert IncidentRequest(description="Orders are delayed.").affected_services == [
        ServiceName.ORDER_CONSUMER
    ]
    with pytest.raises(ValidationError, match="supplied together"):
        IncidentRequest(
            description="Orders are delayed.",
            start_time=START,
            affected_services=[ServiceName.ORDER_CONSUMER],
        )
    with pytest.raises(ValidationError):
        IncidentRequest.model_validate(
            {
                "description": "Orders are delayed.",
                "affected_services": ["database"],
            }
        )


def test_plan_uses_closed_unique_task_types() -> None:
    plan = InvestigationPlan(
        tasks=[
            InvestigationTask(
                task_type=InvestigationTaskType.CHECK_CONSUMER_LAG,
                reason="Measure backlog.",
            ),
            InvestigationTask(
                task_type=InvestigationTaskType.FIND_DATABASE_ERRORS,
                reason="Check a competing explanation.",
            ),
        ]
    )

    assert len(plan.tasks) == 2
    with pytest.raises(ValidationError):
        InvestigationTask.model_validate({"task_type": "run_shell", "reason": "Unsafe."})
    with pytest.raises(ValidationError, match="duplicate"):
        InvestigationPlan(tasks=[plan.tasks[0], plan.tasks[0]])


def test_evidence_models_are_strict_traceable_and_window_bounded() -> None:
    metric = metric_evidence()
    log = LogEvidence(
        evidence_id="log-slow-processing-summary",
        log_type=LogEvidenceType.SLOW_PROCESSING,
        observation="Seven slow-processing events matched.",
        start_time=START,
        end_time=END,
        raw_value_summary={"matching_log_count": 7},
        availability=EvidenceAvailability.AVAILABLE,
        matching_log_count=7,
        timeline=[END, START],
    )
    negative = negative_evidence()

    assert metric.source.value == "prometheus"
    assert log.timeline == [START, END]
    assert negative.matching_log_count == 0
    with pytest.raises(ValidationError, match="outside"):
        LogEvidence(
            **{
                **log.model_dump(exclude={"timeline"}),
                "timeline": [END + timedelta(seconds=1)],
            }
        )
    with pytest.raises(ValidationError):
        MetricEvidence(**{**metric.model_dump(), "unexpected": True})


def test_hypothesis_references_are_unique_and_disjoint() -> None:
    valid = hypothesis()

    assert valid.confidence == 0.95
    insufficient = RootCauseHypothesis(
        cause_code=RootCauseCode.INSUFFICIENT_EVIDENCE,
        confidence=0.0,
        reasoning_summary="Required evidence is unavailable.",
    )
    assert insufficient.supporting_evidence_ids == []
    with pytest.raises(ValidationError, match="positive diagnosis"):
        RootCauseHypothesis(
            cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
            confidence=0.5,
            reasoning_summary="No supporting evidence was cited.",
        )
    with pytest.raises(ValidationError, match="both support and contradict"):
        RootCauseHypothesis(
            cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
            confidence=0.8,
            supporting_evidence_ids=["metric-consumer-lag-summary"],
            contradicting_evidence_ids=["metric-consumer-lag-summary"],
            reasoning_summary="The reference is contradictory.",
        )
    with pytest.raises(ValidationError, match="numerical claims"):
        RootCauseHypothesis(
            cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
            confidence=0.8,
            supporting_evidence_ids=["metric-consumer-lag-summary"],
            reasoning_summary="P95 latency was elevated.",
        )
    with pytest.raises(ValidationError):
        RootCauseHypothesis(
            cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
            confidence=1.1,
            supporting_evidence_ids=["metric-consumer-lag-summary"],
            reasoning_summary="Confidence is invalid.",
        )


def test_action_enum_rejects_forbidden_or_destructive_codes() -> None:
    action = RecommendedAction(
        action_code=RecommendedActionCode.INSPECT_CONSUMER_PROCESSING,
        reason="Inspect the bounded processing path.",
        supporting_evidence_ids=["metric-consumer-lag-summary"],
    )

    assert action.action_code.value == "inspect_consumer_processing"
    for forbidden in ("delete_kafka_topic", "reset_consumer_offsets", "delete_database"):
        with pytest.raises(ValidationError):
            RecommendedAction.model_validate(
                {"action_code": forbidden, "reason": "Forbidden action."}
            )


def test_report_trace_verification_and_evaluation_models_validate() -> None:
    verification = VerificationResult(
        decision=VerificationDecision.ACCEPTED,
        selected_cause=RootCauseCode.SLOW_CONSUMER_PROCESSING,
        verified_evidence_ids=["metric-consumer-lag-summary"],
    )
    report = IncidentReport(
        investigation_id="investigation-001",
        status=IncidentStatus.DIAGNOSED,
        incident_summary="The order consumer accumulated lag.",
        primary_root_cause=hypothesis(),
        supporting_evidence=[metric_evidence()],
        negative_evidence=[negative_evidence()],
        recommended_actions=[
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_CONSUMER_PROCESSING,
                reason="Inspect processing latency.",
                supporting_evidence_ids=["metric-consumer-lag-summary"],
            )
        ],
        limitations=["Only the bounded incident window was inspected."],
        tool_call_count=6,
        investigation_attempts=1,
        started_at=START,
        completed_at=END,
    )
    trace = InvestigationTraceEvent(
        event_id="trace-001",
        timestamp=START,
        event_type=InvestigationTraceEventType.INVESTIGATION_STARTED,
        investigation_id="investigation-001",
        status=TraceStatus.STARTED,
    )
    evaluation = EvaluationResult(
        root_cause_exact_match=True,
        root_cause_rank=1,
        expected_metric_evidence_recall=1.0,
        expected_log_evidence_recall=1.0,
        negative_evidence_recall=1.0,
        unsupported_evidence_reference_count=0,
        forbidden_action_count=0,
        tool_call_count=6,
        investigation_attempt_count=1,
        workflow_duration_seconds=10.0,
    )

    assert verification.decision == VerificationDecision.ACCEPTED
    assert report.supporting_evidence[0].evidence_kind == "metric"
    assert trace.timestamp.tzinfo == UTC
    assert evaluation.root_cause_exact_match is True


def test_stable_evidence_identifiers_cover_the_closed_toolset() -> None:
    assert evidence_id_for_task(InvestigationTaskType.CHECK_CONSUMER_LAG) == (
        "metric-consumer-lag-summary"
    )
    assert (
        evidence_id_for_task(
            InvestigationTaskType.FIND_DATABASE_ERRORS,
            negative=True,
        )
        == "negative-no-database-errors"
    )
    with pytest.raises(ValueError, match="does not support"):
        evidence_id_for_task(InvestigationTaskType.CHECK_PROCESSING_LATENCY, negative=True)
