"""Unit coverage for evidence bundles, parsed proposals, and offline replay."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from incidentops.benchmark import replay as replay_module
from incidentops.benchmark.cli import main as benchmark_main
from incidentops.benchmark.replay import replay_bundle, rescore_proposal
from incidentops.config import Settings
from incidentops.investigation.model import EvidenceDrivenModelProvider
from incidentops.investigation.models import (
    EvidenceBundle,
    IncidentRequest,
    IncidentStatus,
    InvestigationTaskType,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidence,
    NegativeEvidenceType,
    ServiceName,
)
from incidentops.investigation.report import persist_investigation_artifacts
from incidentops.scenarios import load_scenario_manifest

START = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
END = START + timedelta(minutes=10)


def _bundle() -> EvidenceBundle:
    common = {
        "start_time": START,
        "end_time": END,
        "availability": "available",
        "collection_attempt": 1,
    }
    return EvidenceBundle(
        bundle_id="evidence-11111111111111111111",
        source_investigation_id="investigation-replay-source",
        created_at=END,
        incident_request=IncidentRequest(
            description="Orders appear to be falling behind.",
            start_time=START,
            end_time=END,
            run_id="run-replay",
            affected_services=[ServiceName.ORDER_CONSUMER],
        ),
        completed_tasks=list(InvestigationTaskType),
        metric_evidence=[
            MetricEvidence(
                evidence_id="metric-consumer-lag-summary",
                metric_type=MetricEvidenceType.CONSUMER_LAG,
                observation="Consumer lag increased in the bounded window.",
                raw_value_summary={
                    "start_value": 5.0,
                    "end_value": 50.0,
                    "minimum": 5.0,
                    "maximum": 50.0,
                    "trend": "increasing",
                    "sample_count": 5,
                },
                **common,
            ),
            MetricEvidence(
                evidence_id="metric-processing-latency-p95",
                metric_type=MetricEvidenceType.PROCESSING_LATENCY,
                observation="Processing latency was elevated while database latency was normal.",
                raw_value_summary={
                    "percentile": 0.95,
                    "duration_seconds": 0.9,
                    "sample_count": 5,
                    "processing_state": "elevated",
                    "database_duration_seconds": 0.02,
                    "database_sample_count": 5,
                    "database_state": "normal",
                },
                **common,
            ),
            MetricEvidence(
                evidence_id="metric-producer-consumer-rate-comparison",
                metric_type=MetricEvidenceType.PRODUCER_CONSUMER_RATES,
                observation="Consumer throughput was lower than producer throughput.",
                raw_value_summary={
                    "producer_windowed_rate_per_second": 5.0,
                    "consumer_windowed_rate_per_second": 1.0,
                    "windowed_rate_difference_per_second": 4.0,
                    "consumer_is_slower": True,
                    "producer_baseline_rate_per_second": 5.0,
                    "producer_recent_rate_per_second": 5.0,
                    "producer_rate_change_ratio": 1.0,
                    "producer_surge": False,
                    "processing_error_rate_per_second": 0.0,
                    "processing_errors_present": False,
                    "valid_processing_present": True,
                },
                **common,
            ),
        ],
        log_evidence=[
            LogEvidence(
                evidence_id="log-slow-processing-summary",
                log_type=LogEvidenceType.SLOW_PROCESSING,
                observation="Slow processing events were present.",
                raw_value_summary={
                    "matching_log_count": 4,
                    "slow_processing_count": 4,
                    "database_operation_slow_count": 0,
                    "invalid_event_count": 0,
                },
                matching_log_count=4,
                timeline=[],
                **common,
            )
        ],
        negative_evidence=[
            NegativeEvidence(
                evidence_id="negative-no-database-errors",
                negative_type=NegativeEvidenceType.NO_DATABASE_ERRORS,
                observation="No database errors matched.",
                raw_value_summary={"matching_log_count": 0},
                **common,
            ),
            NegativeEvidence(
                evidence_id="negative-no-kafka-errors",
                negative_type=NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS,
                observation="No Kafka errors matched.",
                raw_value_summary={"matching_log_count": 0},
                **common,
            ),
        ],
        collection_tool_calls=6,
        collection_attempts=1,
    )


def test_evidence_bundle_rejects_evaluator_ground_truth() -> None:
    payload = _bundle().model_dump(mode="python")
    payload["expected_root_cause"] = "slow_consumer_processing"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EvidenceBundle.model_validate(payload)


def test_rules_replay_uses_no_telemetry_backend_and_is_accepted() -> None:
    result = replay_bundle(_bundle(), Settings(), approach="rules")

    assert result.proposal.model_provider == "deterministic-test"
    assert result.proposal.model_invoked is False
    assert result.proposal.provider_available is None
    assert result.proposal.structured_response_valid is True
    assert result.proposal.hypotheses[0].cause_code.value == "slow_consumer_processing"
    expected_references = {
        item.evidence_id
        for item in [
            *_bundle().metric_evidence,
            *_bundle().log_evidence,
            *_bundle().negative_evidence,
        ]
    }
    assert set(result.proposal.hypotheses[0].supporting_evidence_ids) == expected_references
    assert result.report.status == IncidentStatus.DIAGNOSED
    assert result.report.tool_call_count == 0
    assert result.proposal.model_call_count == 0
    assert result.report.model_call_count == 0


def test_rescore_reuses_the_parsed_proposal_without_a_model_call() -> None:
    initial = replay_bundle(_bundle(), Settings(), approach="rules")

    result = rescore_proposal(_bundle(), initial.proposal)

    assert result.approach == "rescore"
    assert result.proposal == initial.proposal
    assert result.report.status == IncidentStatus.DIAGNOSED
    assert result.report.model_call_count == 0


def test_artifacts_keep_replay_evidence_and_the_original_model_summary(tmp_path) -> None:
    bundle = _bundle()
    replay = replay_bundle(bundle, Settings(), approach="rules")

    paths = persist_investigation_artifacts(
        replay.report,
        [],
        tmp_path,
        evidence_bundle=bundle,
        model_proposal=replay.proposal,
    )

    assert paths.evidence_path is not None
    assert paths.proposal_path is not None
    persisted_bundle = EvidenceBundle.model_validate_json(paths.evidence_path.read_text())
    persisted_proposal = type(replay.proposal).model_validate_json(paths.proposal_path.read_text())
    assert persisted_bundle == bundle
    assert persisted_proposal.hypotheses[0].reasoning_summary == (
        "The bounded live evidence matches one distinct incident signature."
    )


def test_three_way_comparison_keeps_ground_truth_outside_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        replay_module,
        "create_model_provider",
        lambda _settings: EvidenceDrivenModelProvider(),
    )
    manifest = load_scenario_manifest(Path("scenarios/slow_consumer.yaml"))

    result = replay_module.compare_bundle(_bundle(), manifest, Settings())

    assert [case.approach for case in result.cases] == ["rules", "llm", "llm-rag"]
    assert all(case.proposal_exact_match is True for case in result.cases)
    assert all(case.verifier_accepted is True for case in result.cases)
    assert "expected_root_cause" not in _bundle().model_dump()


def test_rules_replay_cli_writes_a_result_without_live_services(tmp_path, capsys) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "replay.json"
    evidence_path.write_text(_bundle().model_dump_json(), encoding="utf-8")

    exit_code = benchmark_main(
        [
            "replay",
            str(evidence_path),
            "--approach",
            "rules",
            "--output-file",
            str(output_path),
        ]
    )

    assert exit_code == 0
    assert output_path.exists()
    assert "Proposed cause | slow_consumer_processing" in capsys.readouterr().out


def test_rules_replay_abstains_when_the_bundle_has_no_incident_signature() -> None:
    payload = _bundle().model_dump(mode="python")
    metrics = cast(list[dict[str, Any]], payload["metric_evidence"])
    metrics[0]["raw_value_summary"].update(
        {"start_value": 5.0, "end_value": 5.0, "trend": "stable"}
    )
    metrics[1]["raw_value_summary"].update(
        {"duration_seconds": 0.02, "processing_state": "normal"}
    )
    metrics[2]["raw_value_summary"].update(
        {
            "producer_windowed_rate_per_second": 5.0,
            "consumer_windowed_rate_per_second": 5.0,
            "windowed_rate_difference_per_second": 0.0,
            "consumer_is_slower": False,
        }
    )
    logs = cast(list[dict[str, Any]], payload["log_evidence"])
    logs[0]["matching_log_count"] = 0
    logs[0]["raw_value_summary"].update(
        {"matching_log_count": 0, "slow_processing_count": 0}
    )
    bundle = EvidenceBundle.model_validate(payload)

    result = replay_bundle(bundle, Settings(), approach="rules")

    assert result.proposal.hypotheses[0].cause_code.value == "insufficient_evidence"
    assert result.report.status == IncidentStatus.INSUFFICIENT_EVIDENCE


def test_replay_reports_missing_bounded_evidence() -> None:
    payload = _bundle().model_dump(mode="python")
    payload["metric_evidence"] = payload["metric_evidence"][:1]
    payload["completed_tasks"] = [
        item
        for item in payload["completed_tasks"]
        if item
        not in {
            InvestigationTaskType.CHECK_PROCESSING_LATENCY,
            InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES,
        }
    ]
    bundle = EvidenceBundle.model_validate(payload)

    result = replay_bundle(bundle, Settings(), approach="rules")

    assert result.report.status == IncidentStatus.INSUFFICIENT_EVIDENCE
    assert "required bounded evidence is unavailable" in result.report.verification_issues


def test_rescore_rejects_a_saved_slow_consumer_proposal_against_db_latency() -> None:
    initial = replay_bundle(_bundle(), Settings(), approach="rules")
    payload = _bundle().model_dump(mode="python")
    latency = cast(list[dict[str, Any]], payload["metric_evidence"])[1]
    latency["raw_value_summary"].update(
        {"database_duration_seconds": 1.0, "database_state": "elevated"}
    )
    contradictory = EvidenceBundle.model_validate(payload)
    proposal = initial.proposal.model_copy(
        update={"evidence_bundle_id": contradictory.bundle_id}
    )

    result = rescore_proposal(contradictory, proposal)

    assert result.report.status == IncidentStatus.INSUFFICIENT_EVIDENCE
    assert "database latency was not normal" in result.report.verification_issues
