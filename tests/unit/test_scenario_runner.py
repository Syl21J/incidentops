"""Unit coverage for common scenario signatures and ground-truth isolation."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from incidentops.benchmark.runner import (
    NEUTRAL_INCIDENT_DESCRIPTION,
    build_benchmark_incident_request,
)
from incidentops.config import Settings
from incidentops.scenario_runner.runtime import observations_match_manifest, run_scenario
from incidentops.scenarios import load_scenario_manifest
from incidentops.validation.models import ScenarioMetadata, SlowConsumerObservations

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _observations(scenario_id: str) -> SlowConsumerObservations:
    slow = scenario_id == "slow_consumer_v1"
    database = scenario_id == "database_latency_v1"
    traffic = scenario_id == "traffic_spike_v1"
    malformed = scenario_id == "malformed_events_v1"
    return SlowConsumerObservations(
        maximum_lag=40 if not malformed else 0,
        lag_start=0,
        lag_end=40 if not malformed else 0,
        lag_trend="increasing" if not malformed else "stable",
        lag_samples=8,
        p95_seconds=0.9 if slow or database else 0.05,
        latency_samples=8,
        database_p95_seconds=0.9 if database else 0.02,
        database_latency_samples=8,
        processing_state="elevated" if slow or database else "normal",
        database_state="elevated" if database else "normal",
        producer_rate=20 if traffic else 2,
        consumer_rate=1,
        consumer_is_slower=True,
        producer_baseline_rate=2,
        producer_recent_rate=20 if traffic else 2,
        producer_rate_change_ratio=10 if traffic else 1,
        producer_surge=traffic,
        processing_error_rate=1 if malformed else 0,
        processing_errors_present=malformed,
        valid_processing_present=True,
        slow_processing_log_count=3 if slow else 0,
        database_operation_slow_log_count=3 if database else 0,
        invalid_event_log_count=3 if malformed else 0,
        database_error_count=0,
        kafka_error_count=0,
    )


@pytest.mark.parametrize(
    ("file_name", "scenario_id"),
    [
        ("slow_consumer.yaml", "slow_consumer_v1"),
        ("database_latency.yaml", "database_latency_v1"),
        ("traffic_spike.yaml", "traffic_spike_v1"),
        ("malformed_events.yaml", "malformed_events_v1"),
    ],
)
def test_common_harness_accepts_only_each_manifest_signature(
    file_name: str,
    scenario_id: str,
) -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / file_name)
    observations = _observations(scenario_id)

    assert observations_match_manifest(manifest, observations) is True
    observations.database_error_count = 1
    assert observations_match_manifest(manifest, observations) is False


def test_exported_metadata_contains_no_ground_truth_fields() -> None:
    schema = ScenarioMetadata.model_json_schema()
    serialized_schema = str(schema)

    assert "root_cause" not in serialized_schema
    assert "expected_metrics" not in serialized_schema
    assert "expected_knowledge_documents" not in serialized_schema


def test_graph_request_contains_only_neutral_operational_inputs() -> None:
    start = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
    metadata = ScenarioMetadata(
        scenario_id="database_latency_v1",
        run_id="incident-run-123-456",
        topic="orders.database-latency.123-456",
        consumer_group="database-latency-123-456",
        start_time=start,
        end_time=start + timedelta(minutes=5),
        observations=_observations("database_latency_v1"),
    )

    request = build_benchmark_incident_request(metadata)
    payload = request.model_dump(mode="json")

    assert request.description == NEUTRAL_INCIDENT_DESCRIPTION
    assert set(payload) == {
        "description",
        "start_time",
        "end_time",
        "affected_services",
        "run_id",
    }
    serialized = request.model_dump_json()
    assert "database_latency_v1" not in serialized
    assert "database-latency" not in serialized
    assert "observations" not in serialized


@pytest.mark.parametrize(
    ("start_delay", "minimum_incident"),
    [(-1.0, 0.0), (121.0, 0.0), (0.0, -1.0), (0.0, 121.0)],
)
def test_scenario_phase_durations_are_bounded(
    start_delay: float,
    minimum_incident: float,
) -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / "slow_consumer.yaml")

    with pytest.raises(ValueError, match="between zero and 120 seconds"):
        run_scenario(
            manifest,
            Settings(),
            start_delay_seconds=start_delay,
            minimum_incident_seconds=minimum_incident,
        )
