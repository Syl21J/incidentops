"""Deterministic evidence and hypothesis verification without scenario ground truth."""

from __future__ import annotations

from collections.abc import Iterable
from typing import cast

from incidentops.investigation.models import (
    EvidenceAvailability,
    InvestigationTaskType,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidence,
    RootCauseCode,
    VerificationDecision,
    VerificationResult,
    evidence_id_for_task,
)
from incidentops.investigation.state import InvestigationState


def _all_evidence(
    state: InvestigationState,
) -> list[MetricEvidence | LogEvidence | NegativeEvidence]:
    return [
        *state.get("metric_evidence", []),
        *state.get("log_evidence", []),
        *state.get("negative_evidence", []),
    ]


def _task_evidence_ids(task_type: InvestigationTaskType) -> set[str]:
    if task_type in {
        InvestigationTaskType.FIND_DATABASE_ERRORS,
        InvestigationTaskType.FIND_KAFKA_ERRORS,
    }:
        return {
            evidence_id_for_task(task_type),
            evidence_id_for_task(task_type, negative=True),
        }
    return {evidence_id_for_task(task_type)}


def find_missing_tasks(state: InvestigationState) -> list[InvestigationTaskType]:
    """Return tasks with no available result, including unavailable prior attempts."""

    evidence = _all_evidence(state)
    available_ids = {
        item.evidence_id for item in evidence if item.availability == EvidenceAvailability.AVAILABLE
    }
    return [
        task_type
        for task_type in InvestigationTaskType
        if not (_task_evidence_ids(task_type) & available_ids)
    ]


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _validate_metric_summary(item: MetricEvidence) -> list[str]:
    issues: list[str] = []
    raw = item.raw_value_summary
    if item.availability == EvidenceAvailability.UNAVAILABLE:
        return issues
    if item.metric_type == MetricEvidenceType.CONSUMER_LAG:
        required = {"start_value", "end_value", "minimum", "maximum", "trend", "sample_count"}
        if not required <= raw.keys():
            issues.append(f"consumer lag evidence {item.evidence_id} has an incomplete summary")
        numeric = {
            key: _finite_number(raw.get(key))
            for key in ("start_value", "end_value", "minimum", "maximum")
        }
        if any(value is None or value < 0 for value in numeric.values()):
            issues.append(f"consumer lag evidence {item.evidence_id} has invalid values")
        elif (
            cast(float, numeric["minimum"]) > cast(float, numeric["maximum"])
            or not cast(float, numeric["minimum"])
            <= cast(float, numeric["start_value"])
            <= cast(float, numeric["maximum"])
            or not cast(float, numeric["minimum"])
            <= cast(float, numeric["end_value"])
            <= cast(float, numeric["maximum"])
        ):
            issues.append(f"consumer lag evidence {item.evidence_id} is internally inconsistent")
        if raw.get("trend") not in {"increasing", "stable", "decreasing"}:
            issues.append(f"consumer lag evidence {item.evidence_id} has an invalid trend")
        sample_count = raw.get("sample_count")
        if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count <= 0:
            issues.append(f"consumer lag evidence {item.evidence_id} has no samples")
    elif item.metric_type == MetricEvidenceType.PROCESSING_LATENCY:
        duration = _finite_number(raw.get("duration_seconds"))
        if duration is None or duration < 0 or raw.get("percentile") != 0.95:
            issues.append(f"processing latency evidence {item.evidence_id} has invalid values")
        sample_count = raw.get("sample_count")
        if isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count <= 0:
            issues.append(f"processing latency evidence {item.evidence_id} has no samples")
    elif item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES:
        producer = _finite_number(raw.get("producer_windowed_rate_per_second"))
        consumer = _finite_number(raw.get("consumer_windowed_rate_per_second"))
        difference = _finite_number(raw.get("windowed_rate_difference_per_second"))
        slower = raw.get("consumer_is_slower")
        if producer is None or producer < 0 or consumer is None or consumer < 0:
            issues.append(f"rate evidence {item.evidence_id} has invalid values")
        if not isinstance(slower, bool):
            issues.append(f"rate evidence {item.evidence_id} has an invalid comparison")
        if (
            producer is not None
            and consumer is not None
            and difference is not None
            and (abs(difference - (producer - consumer)) > 1e-9 or slower is not (difference > 0))
        ):
            issues.append(f"rate evidence {item.evidence_id} is internally inconsistent")
    return issues


def _validate_log_summary(item: LogEvidence | NegativeEvidence) -> list[str]:
    issues: list[str] = []
    raw_count = item.raw_value_summary.get("matching_log_count")
    if item.availability == EvidenceAvailability.AVAILABLE and raw_count != item.matching_log_count:
        issues.append(f"log count does not match evidence {item.evidence_id}")
    if isinstance(item, LogEvidence) and len(item.timeline) > item.matching_log_count:
        issues.append(f"log timeline exceeds the count in evidence {item.evidence_id}")
    return issues


def _integrity_issues(
    state: InvestigationState,
    evidence: list[MetricEvidence | LogEvidence | NegativeEvidence],
) -> list[str]:
    issues: list[str] = []
    identifiers = [item.evidence_id for item in evidence]
    if len(set(identifiers)) != len(identifiers):
        issues.append("evidence identifiers are not unique")

    start_time = state.get("start_time")
    end_time = state.get("end_time")
    if start_time is None or end_time is None:
        issues.append("the validated investigation window is missing")
    else:
        for item in evidence:
            if item.start_time != start_time or item.end_time != end_time:
                issues.append(
                    f"evidence {item.evidence_id} does not match the investigation window"
                )

    for item in evidence:
        if isinstance(item, MetricEvidence):
            issues.extend(_validate_metric_summary(item))
        else:
            issues.extend(_validate_log_summary(item))
    return issues


def _available_by_id(
    evidence: Iterable[MetricEvidence | LogEvidence | NegativeEvidence],
) -> dict[str, MetricEvidence | LogEvidence | NegativeEvidence]:
    return {
        item.evidence_id: item
        for item in evidence
        if item.availability == EvidenceAvailability.AVAILABLE
    }


def _slow_consumer_issues(
    supporting_ids: set[str],
    evidence_by_id: dict[str, MetricEvidence | LogEvidence | NegativeEvidence],
) -> list[str]:
    required_support = {
        evidence_id_for_task(InvestigationTaskType.CHECK_CONSUMER_LAG),
        evidence_id_for_task(InvestigationTaskType.CHECK_PROCESSING_LATENCY),
        evidence_id_for_task(InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES),
        evidence_id_for_task(InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS),
        evidence_id_for_task(InvestigationTaskType.FIND_DATABASE_ERRORS, negative=True),
        evidence_id_for_task(InvestigationTaskType.FIND_KAFKA_ERRORS, negative=True),
    }
    issues: list[str] = []
    if not required_support <= supporting_ids:
        issues.append("slow consumer diagnosis does not cite every required evidence check")

    lag = cast(MetricEvidence, evidence_by_id["metric-consumer-lag-summary"])
    latency = cast(MetricEvidence, evidence_by_id["metric-processing-latency-p95"])
    rates = cast(MetricEvidence, evidence_by_id["metric-producer-consumer-rate-comparison"])
    slow_logs = cast(LogEvidence, evidence_by_id["log-slow-processing-summary"])
    if lag.raw_value_summary.get("trend") != "increasing":
        issues.append("consumer lag did not increase")
    if (_finite_number(latency.raw_value_summary.get("duration_seconds")) or 0.0) <= 0:
        issues.append("processing latency has no positive observation")
    if rates.raw_value_summary.get("consumer_is_slower") is not True:
        issues.append("consumer throughput was not lower than producer throughput")
    if slow_logs.matching_log_count <= 0:
        issues.append("no slow-processing log evidence was found")

    positive_error_types = {
        item.log_type
        for item in evidence_by_id.values()
        if isinstance(item, LogEvidence) and item.matching_log_count > 0
    }
    if LogEvidenceType.DATABASE_ERRORS in positive_error_types:
        issues.append("database errors conflict with the slow consumer diagnosis")
    if LogEvidenceType.KAFKA_ERRORS in positive_error_types:
        issues.append("Kafka errors conflict with the slow consumer diagnosis")
    return issues


def _alternative_cause_issues(
    cause_code: RootCauseCode,
    supporting_ids: set[str],
    evidence_by_id: dict[str, MetricEvidence | LogEvidence | NegativeEvidence],
) -> list[str]:
    if cause_code == RootCauseCode.DATABASE_LATENCY:
        evidence_id = evidence_id_for_task(InvestigationTaskType.FIND_DATABASE_ERRORS)
        item = evidence_by_id.get(evidence_id)
        if evidence_id not in supporting_ids or not isinstance(item, LogEvidence):
            return ["database latency lacks positive database evidence"]
        if item.matching_log_count <= 0:
            return ["database latency lacks positive database evidence"]
        latency_id = evidence_id_for_task(InvestigationTaskType.CHECK_PROCESSING_LATENCY)
        if latency_id not in supporting_ids:
            return ["database latency lacks processing latency evidence"]
        return []
    if cause_code == RootCauseCode.KAFKA_BROKER_FAILURE:
        evidence_id = evidence_id_for_task(InvestigationTaskType.FIND_KAFKA_ERRORS)
        item = evidence_by_id.get(evidence_id)
        if evidence_id not in supporting_ids or not isinstance(item, LogEvidence):
            return ["Kafka broker failure lacks positive Kafka evidence"]
        if item.matching_log_count <= 0:
            return ["Kafka broker failure lacks positive Kafka evidence"]
        return []
    if cause_code == RootCauseCode.TRAFFIC_SPIKE:
        return ["traffic spike cannot be distinguished without producer target-rate evidence"]
    return ["insufficient evidence is not a positive diagnosis"]


def verify_investigation_state(state: InvestigationState) -> VerificationResult:
    """Verify evidence, references, alternatives, and recheck requirements deterministically."""

    evidence = _all_evidence(state)
    integrity_issues = _integrity_issues(state, evidence)
    evidence_by_id = _available_by_id(evidence)
    existing_ids = {item.evidence_id for item in evidence}
    available_ids = set(evidence_by_id)
    missing_tasks = find_missing_tasks(state)
    attempts = state.get("investigation_attempts", 1)

    hypotheses = state.get("hypotheses", [])
    cited_ids = {
        evidence_id
        for hypothesis in hypotheses
        for evidence_id in (
            *hypothesis.supporting_evidence_ids,
            *hypothesis.contradicting_evidence_ids,
        )
    }
    unknown_references = sorted(cited_ids - existing_ids)
    if unknown_references:
        integrity_issues.append("one or more hypothesis evidence references are unavailable")

    if integrity_issues:
        return VerificationResult(
            decision=VerificationDecision.REJECTED,
            selected_cause=RootCauseCode.INSUFFICIENT_EVIDENCE,
            verified_evidence_ids=sorted(cited_ids & available_ids),
            missing_tasks=missing_tasks,
            issues=integrity_issues[:20],
        )

    if missing_tasks:
        decision = (
            VerificationDecision.NEEDS_MORE_EVIDENCE
            if attempts < 2
            else VerificationDecision.REJECTED
        )
        return VerificationResult(
            decision=decision,
            selected_cause=RootCauseCode.INSUFFICIENT_EVIDENCE,
            verified_evidence_ids=sorted(cited_ids & available_ids),
            missing_tasks=missing_tasks,
            issues=["required bounded evidence is unavailable"],
        )

    if not hypotheses:
        return VerificationResult(
            decision=VerificationDecision.REJECTED,
            selected_cause=RootCauseCode.INSUFFICIENT_EVIDENCE,
            issues=["no structured root-cause hypothesis was generated"],
        )

    primary = hypotheses[0]
    supporting_ids = set(primary.supporting_evidence_ids)
    all_primary_references = supporting_ids | set(primary.contradicting_evidence_ids)
    available_negative_ids = {
        item.evidence_id for item in evidence_by_id.values() if isinstance(item, NegativeEvidence)
    }
    if primary.cause_code != RootCauseCode.INSUFFICIENT_EVIDENCE and not (
        all_primary_references & available_negative_ids
    ):
        return VerificationResult(
            decision=VerificationDecision.REJECTED,
            selected_cause=RootCauseCode.INSUFFICIENT_EVIDENCE,
            verified_evidence_ids=sorted(cited_ids & available_ids),
            issues=["the primary hypothesis did not cite checked negative evidence"],
        )
    if primary.cause_code == RootCauseCode.SLOW_CONSUMER_PROCESSING:
        diagnosis_issues = _slow_consumer_issues(supporting_ids, evidence_by_id)
    else:
        diagnosis_issues = _alternative_cause_issues(
            primary.cause_code,
            supporting_ids,
            evidence_by_id,
        )
    if diagnosis_issues:
        return VerificationResult(
            decision=VerificationDecision.REJECTED,
            selected_cause=RootCauseCode.INSUFFICIENT_EVIDENCE,
            verified_evidence_ids=sorted(cited_ids & available_ids),
            issues=diagnosis_issues[:20],
        )

    return VerificationResult(
        decision=VerificationDecision.ACCEPTED,
        selected_cause=primary.cause_code,
        verified_evidence_ids=sorted(cited_ids),
    )
