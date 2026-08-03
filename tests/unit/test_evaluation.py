"""Unit coverage for scenario evaluation and local investigation artifacts."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from incidentops.evaluation.evaluator import evaluate_incident_report
from incidentops.investigation.models import (
    EvidenceAvailability,
    IncidentReport,
    IncidentStatus,
    InvestigationTraceEvent,
    InvestigationTraceEventType,
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
    TraceStatus,
)
from incidentops.investigation.report import persist_investigation_artifacts
from incidentops.scenarios import load_scenario_manifest

START = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
END = START + timedelta(minutes=10)


def _report() -> IncidentReport:
    metric_common = {
        "start_time": START,
        "end_time": END,
        "availability": EvidenceAvailability.AVAILABLE,
    }
    evidence = [
        MetricEvidence(
            evidence_id="metric-consumer-lag-summary",
            metric_type=MetricEvidenceType.CONSUMER_LAG,
            observation="Consumer lag increased.",
            raw_value_summary={"trend": "increasing", "maximum": 59.0},
            **metric_common,
        ),
        MetricEvidence(
            evidence_id="metric-processing-latency-p95",
            metric_type=MetricEvidenceType.PROCESSING_LATENCY,
            observation="Processing latency was collected.",
            raw_value_summary={"duration_seconds": 0.975},
            **metric_common,
        ),
        MetricEvidence(
            evidence_id="metric-producer-consumer-rate-comparison",
            metric_type=MetricEvidenceType.PRODUCER_CONSUMER_RATES,
            observation="Producer throughput exceeded consumer throughput.",
            raw_value_summary={"consumer_is_slower": True},
            **metric_common,
        ),
        LogEvidence(
            evidence_id="log-slow-processing-summary",
            log_type=LogEvidenceType.SLOW_PROCESSING,
            observation="Slow-processing events were found.",
            raw_value_summary={"matching_log_count": 8},
            matching_log_count=8,
            timeline=[START + timedelta(minutes=1)],
            **metric_common,
        ),
    ]
    negative = [
        NegativeEvidence(
            evidence_id="negative-no-database-errors",
            negative_type=NegativeEvidenceType.NO_DATABASE_ERRORS,
            observation="No database errors matched.",
            raw_value_summary={"matching_log_count": 0},
            **metric_common,
        ),
        NegativeEvidence(
            evidence_id="negative-no-kafka-errors",
            negative_type=NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS,
            observation="No Kafka errors matched.",
            raw_value_summary={"matching_log_count": 0},
            **metric_common,
        ),
    ]
    evidence_ids = [item.evidence_id for item in [*evidence, *negative]]
    hypothesis = RootCauseHypothesis(
        cause_code=RootCauseCode.SLOW_CONSUMER_PROCESSING,
        confidence=0.9,
        supporting_evidence_ids=evidence_ids,
        reasoning_summary="The structured signals support slow consumer processing.",
    )
    return IncidentReport(
        investigation_id="investigation-evaluation",
        status=IncidentStatus.DIAGNOSED,
        incident_summary="The bounded workflow diagnosed slow consumer processing.",
        primary_root_cause=hypothesis,
        supporting_evidence=evidence,
        negative_evidence=negative,
        recommended_actions=[
            RecommendedAction(
                action_code=RecommendedActionCode.INSPECT_CONSUMER_PROCESSING,
                reason="Inspect consumer processing.",
                supporting_evidence_ids=evidence_ids,
            )
        ],
        tool_call_count=6,
        model_call_count=2,
        investigation_attempts=1,
        started_at=START,
        completed_at=END,
    )


def test_evaluator_recovers_all_expected_slow_consumer_evidence() -> None:
    manifest = load_scenario_manifest(Path("scenarios/slow_consumer.yaml"))

    result = evaluate_incident_report(_report(), manifest)

    assert result.root_cause_exact_match is True
    assert result.root_cause_rank == 1
    assert result.expected_metric_evidence_recall == 1.0
    assert result.expected_log_evidence_recall == 1.0
    assert result.negative_evidence_recall == 1.0
    assert result.unsupported_evidence_reference_count == 0
    assert result.forbidden_action_count == 0
    assert result.tool_call_count == 6
    assert result.investigation_attempt_count == 1
    assert result.workflow_duration_seconds == 600.0


def test_evaluator_counts_unsupported_evidence_references() -> None:
    report = _report()
    assert report.primary_root_cause is not None
    report.primary_root_cause.supporting_evidence_ids.append("metric-invented-evidence")
    manifest = load_scenario_manifest(Path("scenarios/slow_consumer.yaml"))

    result = evaluate_incident_report(report, manifest)

    assert result.unsupported_evidence_reference_count == 1


def test_artifact_persistence_writes_valid_json_and_jsonl(tmp_path: Path) -> None:
    report = _report()
    trace = InvestigationTraceEvent(
        event_id="investigation-evaluation:1:investigation_completed",
        timestamp=END,
        event_type=InvestigationTraceEventType.INVESTIGATION_COMPLETED,
        investigation_id=report.investigation_id,
        status=TraceStatus.COMPLETED,
        tool_call_count=6,
    )

    paths = persist_investigation_artifacts(report, [trace], tmp_path)

    persisted = IncidentReport.model_validate_json(paths.report_path.read_text(encoding="utf-8"))
    trace_lines = paths.trace_path.read_text(encoding="utf-8").splitlines()
    assert persisted == report
    assert len(trace_lines) == 1
    assert InvestigationTraceEvent.model_validate_json(trace_lines[0]) == trace
