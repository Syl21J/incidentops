"""Deterministic comparison of reports with scenario ground truth."""

from __future__ import annotations

from incidentops.investigation.models import (
    EvaluationResult,
    IncidentReport,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidenceType,
)
from incidentops.scenarios import (
    ExpectedLog,
    ExpectedMetric,
    ScenarioManifest,
    TelemetryBehavior,
    TelemetrySignal,
)


def _metric_expectation_found(
    expectation: ExpectedMetric,
    evidence: list[MetricEvidence],
) -> bool:
    if expectation.signal == TelemetrySignal.CONSUMER_LAG:
        return any(
            item.metric_type == MetricEvidenceType.CONSUMER_LAG
            and item.raw_value_summary.get("trend") == expectation.behavior.value
            for item in evidence
        )
    if expectation.signal in {
        TelemetrySignal.PROCESSING_LATENCY,
        TelemetrySignal.DATABASE_LATENCY,
    }:
        key = (
            "processing_state"
            if expectation.signal == TelemetrySignal.PROCESSING_LATENCY
            else "database_state"
        )
        return any(
            item.metric_type == MetricEvidenceType.PROCESSING_LATENCY
            and item.raw_value_summary.get(key) == expectation.behavior.value
            for item in evidence
        )
    if expectation.signal == TelemetrySignal.PRODUCER_RATE_CHANGE:
        expected = expectation.behavior == TelemetryBehavior.SURGING
        return any(
            item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES
            and item.raw_value_summary.get("producer_surge") is expected
            for item in evidence
        )
    if expectation.signal == TelemetrySignal.PROCESSING_ERRORS:
        expected = expectation.behavior == TelemetryBehavior.PRESENT
        return any(
            item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES
            and item.raw_value_summary.get("processing_errors_present") is expected
            for item in evidence
        )
    expected = expectation.behavior == TelemetryBehavior.PRESENT
    return any(
        item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES
        and item.raw_value_summary.get("valid_processing_present") is expected
        for item in evidence
    )


_LOG_COUNT_FIELDS = {
    "slow_processing": "slow_processing_count",
    "database_operation_slow": "database_operation_slow_count",
    "invalid_event_skipped": "invalid_event_count",
}


def _log_expectation_found(expectation: ExpectedLog, evidence: list[LogEvidence]) -> bool:
    count_field = _LOG_COUNT_FIELDS[expectation.event_type]
    for item in evidence:
        count = item.raw_value_summary.get(count_field)
        if (
            item.log_type == LogEvidenceType.SLOW_PROCESSING
            and isinstance(count, int | float)
            and not isinstance(count, bool)
            and count > 0
        ):
            return True
    return False


def _recall(found: int, expected: int) -> float:
    return 1.0 if expected == 0 else found / expected


def _root_cause_rank(report: IncidentReport, expected_code: str) -> int | None:
    ranked = [report.primary_root_cause, *report.alternative_hypotheses]
    for rank, hypothesis in enumerate(ranked, start=1):
        if hypothesis is not None and hypothesis.cause_code.value == expected_code:
            return rank
    return None


def _unsupported_reference_count(report: IncidentReport) -> int:
    known_ids = {
        item.evidence_id for item in [*report.supporting_evidence, *report.negative_evidence]
    }
    references: list[str] = []
    hypotheses = [report.primary_root_cause, *report.alternative_hypotheses]
    for hypothesis in hypotheses:
        if hypothesis is not None:
            references.extend(hypothesis.supporting_evidence_ids)
            references.extend(hypothesis.contradicting_evidence_ids)
    for action in report.recommended_actions:
        references.extend(action.supporting_evidence_ids)
    return sum(reference not in known_ids for reference in references)


def _unsupported_knowledge_reference_count(report: IncidentReport) -> int:
    known_ids = {item.knowledge_reference_id for item in report.knowledge_references}
    references = [
        reference_id
        for hypothesis in [report.primary_root_cause, *report.alternative_hypotheses]
        if hypothesis is not None
        for reference_id in hypothesis.knowledge_reference_ids
    ]
    return sum(reference not in known_ids for reference in references)


def evaluate_incident_report(
    report: IncidentReport,
    manifest: ScenarioManifest,
) -> EvaluationResult:
    """Evaluate a completed report without exposing ground truth to its graph."""

    metric_evidence = [
        item for item in report.supporting_evidence if isinstance(item, MetricEvidence)
    ]
    log_evidence = [item for item in report.supporting_evidence if isinstance(item, LogEvidence)]
    metric_expectations = [
        item for item in manifest.expected_metrics if isinstance(item, ExpectedMetric)
    ]
    expected_metrics_found = sum(
        _metric_expectation_found(item, metric_evidence) for item in metric_expectations
    )
    expected_logs_found = sum(
        _log_expectation_found(item, log_evidence) for item in manifest.expected_logs
    )
    negative_types = {item.negative_type for item in report.negative_evidence}
    negative_mapping = {
        "no_database_errors": NegativeEvidenceType.NO_DATABASE_ERRORS,
        "no_kafka_broker_errors": NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS,
    }
    negative_found = sum(
        negative_mapping.get(expected) in negative_types for expected in manifest.negative_evidence
    )
    primary_code = (
        report.primary_root_cause.cause_code.value
        if report.primary_root_cause is not None
        else None
    )
    action_codes = {item.action_code.value for item in report.recommended_actions}
    duration = (report.completed_at - report.started_at).total_seconds()
    return EvaluationResult(
        root_cause_exact_match=primary_code == manifest.root_cause.code,
        root_cause_rank=_root_cause_rank(report, manifest.root_cause.code),
        expected_metric_evidence_recall=_recall(
            expected_metrics_found,
            len(metric_expectations),
        ),
        expected_log_evidence_recall=_recall(
            expected_logs_found,
            len(manifest.expected_logs),
        ),
        negative_evidence_recall=_recall(
            negative_found,
            len(manifest.negative_evidence),
        ),
        unsupported_evidence_reference_count=_unsupported_reference_count(report),
        unsupported_knowledge_reference_count=_unsupported_knowledge_reference_count(report),
        knowledge_reference_count=len(report.knowledge_references),
        forbidden_action_count=sum(action in action_codes for action in manifest.forbidden_actions),
        tool_call_count=report.tool_call_count,
        investigation_attempt_count=report.investigation_attempts,
        workflow_duration_seconds=duration,
    )
