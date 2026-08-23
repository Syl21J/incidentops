"""Evidence-driven classification coverage for all Stage Seven incident causes."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from langchain_core.messages import HumanMessage

from incidentops.investigation.model import EvidenceDrivenModelProvider
from incidentops.investigation.models import (
    EvidenceAvailability,
    HypothesisSet,
    IncidentRequest,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidence,
    NegativeEvidenceType,
    RootCauseCode,
    VerificationDecision,
)
from incidentops.investigation.state import InvestigationState
from incidentops.investigation.verifier import verify_investigation_state

START = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
END = START + timedelta(minutes=5)


def _profile(cause: RootCauseCode) -> list[MetricEvidence | LogEvidence | NegativeEvidence]:
    slow = cause == RootCauseCode.SLOW_CONSUMER_PROCESSING
    database = cause == RootCauseCode.DATABASE_LATENCY
    traffic = cause == RootCauseCode.TRAFFIC_SPIKE
    malformed = cause == RootCauseCode.MALFORMED_EVENT
    lag_increasing = cause != RootCauseCode.MALFORMED_EVENT
    processing_elevated = slow or database
    database_elevated = database
    common = {
        "start_time": START,
        "end_time": END,
        "availability": EvidenceAvailability.AVAILABLE,
    }
    return [
        MetricEvidence(
            evidence_id="metric-consumer-lag-summary",
            metric_type=MetricEvidenceType.CONSUMER_LAG,
            observation="Bounded lag profile.",
            raw_value_summary={
                "start_value": 0.0,
                "end_value": 40.0 if lag_increasing else 0.0,
                "minimum": 0.0,
                "maximum": 40.0 if lag_increasing else 0.0,
                "trend": "increasing" if lag_increasing else "stable",
                "sample_count": 8,
            },
            **common,
        ),
        MetricEvidence(
            evidence_id="metric-processing-latency-p95",
            metric_type=MetricEvidenceType.PROCESSING_LATENCY,
            observation="Bounded latency profile.",
            raw_value_summary={
                "percentile": 0.95,
                "duration_seconds": 0.9 if processing_elevated else 0.05,
                "sample_count": 8,
                "database_duration_seconds": 0.9 if database_elevated else 0.02,
                "database_sample_count": 8,
                "processing_state": "elevated" if processing_elevated else "normal",
                "database_state": "elevated" if database_elevated else "normal",
            },
            **common,
        ),
        MetricEvidence(
            evidence_id="metric-producer-consumer-rate-comparison",
            metric_type=MetricEvidenceType.PRODUCER_CONSUMER_RATES,
            observation="Bounded throughput and health profile.",
            raw_value_summary={
                "producer_windowed_rate_per_second": 20.0 if traffic else 2.0,
                "consumer_windowed_rate_per_second": 1.0,
                "windowed_rate_difference_per_second": 19.0 if traffic else 1.0,
                "consumer_is_slower": True,
                "producer_baseline_rate_per_second": 2.0,
                "producer_recent_rate_per_second": 20.0 if traffic else 2.0,
                "producer_rate_change_ratio": 10.0 if traffic else 1.0,
                "producer_surge": traffic,
                "processing_error_rate_per_second": 1.0 if malformed else 0.0,
                "processing_errors_present": malformed,
                "valid_processing_present": True,
            },
            **common,
        ),
        LogEvidence(
            evidence_id="log-slow-processing-summary",
            log_type=LogEvidenceType.SLOW_PROCESSING,
            observation="Bounded application signal profile.",
            raw_value_summary={
                "matching_log_count": int(slow) + int(database) + int(malformed),
                "timeline_entries_returned": 0,
                "slow_processing_count": 3 if slow else 0,
                "database_operation_slow_count": 3 if database else 0,
                "invalid_event_count": 3 if malformed else 0,
            },
            matching_log_count=int(slow) + int(database) + int(malformed),
            **common,
        ),
        NegativeEvidence(
            evidence_id="negative-no-database-errors",
            negative_type=NegativeEvidenceType.NO_DATABASE_ERRORS,
            observation="No database errors.",
            raw_value_summary={"matching_log_count": 0},
            **common,
        ),
        NegativeEvidence(
            evidence_id="negative-no-kafka-errors",
            negative_type=NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS,
            observation="No Kafka broker errors.",
            raw_value_summary={"matching_log_count": 0},
            **common,
        ),
    ]


def _diagnose(
    evidence: list[MetricEvidence | LogEvidence | NegativeEvidence],
) -> tuple[RootCauseCode, VerificationDecision]:
    provider = EvidenceDrivenModelProvider()
    payload = {"evidence": [item.model_dump(mode="json") for item in evidence]}
    hypotheses = provider.invoke_structured(
        HypothesisSet,
        [HumanMessage(content=json.dumps(payload))],
    )
    state: InvestigationState = {
        "incident_request": IncidentRequest(
            description="Orders appear to be falling behind.",
            start_time=START,
            end_time=END,
            run_id="neutral-run-001",
        ),
        "start_time": START,
        "end_time": END,
        "run_id": "neutral-run-001",
        "metric_evidence": [item for item in evidence if isinstance(item, MetricEvidence)],
        "log_evidence": [item for item in evidence if isinstance(item, LogEvidence)],
        "negative_evidence": [item for item in evidence if isinstance(item, NegativeEvidence)],
        "hypotheses": hypotheses.hypotheses,
        "tool_call_count": 6,
        "model_call_count": 2,
        "investigation_attempts": 1,
        "errors": [],
    }
    result = verify_investigation_state(state)
    return hypotheses.hypotheses[0].cause_code, result.decision


@pytest.mark.parametrize(
    "cause",
    [
        RootCauseCode.SLOW_CONSUMER_PROCESSING,
        RootCauseCode.DATABASE_LATENCY,
        RootCauseCode.TRAFFIC_SPIKE,
        RootCauseCode.MALFORMED_EVENT,
    ],
)
def test_model_visible_evidence_distinguishes_each_incident(cause: RootCauseCode) -> None:
    diagnosed, decision = _diagnose(_profile(cause))

    assert diagnosed == cause
    assert decision == VerificationDecision.ACCEPTED


def test_ambiguous_normal_telemetry_returns_insufficient_evidence() -> None:
    evidence = _profile(RootCauseCode.MALFORMED_EVENT)
    rates = next(
        item
        for item in evidence
        if isinstance(item, MetricEvidence)
        and item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES
    )
    logs = next(item for item in evidence if isinstance(item, LogEvidence))
    rates.raw_value_summary["processing_error_rate_per_second"] = 0.0
    rates.raw_value_summary["processing_errors_present"] = False
    logs.raw_value_summary["matching_log_count"] = 0
    logs.raw_value_summary["invalid_event_count"] = 0
    logs.matching_log_count = 0

    diagnosed, decision = _diagnose(evidence)

    assert diagnosed == RootCauseCode.INSUFFICIENT_EVIDENCE
    assert decision == VerificationDecision.REJECTED


def test_conflicting_malformed_and_producer_surge_signals_are_not_forced() -> None:
    evidence = _profile(RootCauseCode.MALFORMED_EVENT)
    rates = next(
        item
        for item in evidence
        if isinstance(item, MetricEvidence)
        and item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES
    )
    rates.raw_value_summary.update(
        {
            "producer_windowed_rate_per_second": 20.0,
            "windowed_rate_difference_per_second": 19.0,
            "producer_recent_rate_per_second": 20.0,
            "producer_rate_change_ratio": 10.0,
            "producer_surge": True,
        }
    )

    diagnosed, decision = _diagnose(evidence)

    assert diagnosed == RootCauseCode.INSUFFICIENT_EVIDENCE
    assert decision == VerificationDecision.REJECTED
