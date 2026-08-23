"""Unit coverage for the versioned executable ground-truth manifests."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from incidentops.scenarios import (
    DatabaseLatencyExecution,
    MalformedEventsExecution,
    ScenarioManifest,
    SlowConsumerExecution,
    TrafficSpikeExecution,
    load_scenario_manifest,
)

PROJECT_DIR = Path(__file__).resolve().parents[2]


def test_slow_consumer_manifest_loads_with_expected_ground_truth() -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / "slow_consumer.yaml")

    assert manifest.schema_version == 2
    assert manifest.id == "slow_consumer_v1"
    assert manifest.root_cause.code == "slow_consumer_processing"
    assert isinstance(manifest.execution, SlowConsumerExecution)
    assert manifest.execution.processing_delay_ms == 800
    assert manifest.forbidden_actions == [
        "delete_kafka_topic",
        "reset_consumer_offsets",
        "delete_database",
    ]


@pytest.mark.parametrize(
    ("file_name", "scenario_id", "execution_type"),
    [
        ("slow_consumer.yaml", "slow_consumer_v1", SlowConsumerExecution),
        ("database_latency.yaml", "database_latency_v1", DatabaseLatencyExecution),
        ("traffic_spike.yaml", "traffic_spike_v1", TrafficSpikeExecution),
        ("malformed_events.yaml", "malformed_events_v1", MalformedEventsExecution),
    ],
)
def test_all_scenario_manifests_are_versioned_and_executable(
    file_name: str,
    scenario_id: str,
    execution_type: type[object],
) -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / file_name)

    assert manifest.schema_version == 2
    assert manifest.id == scenario_id
    assert isinstance(manifest.execution, execution_type)
    assert manifest.expected_metrics
    assert manifest.negative_evidence
    assert manifest.expected_knowledge_documents


def test_manifest_rejects_unknown_fields() -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / "slow_consumer.yaml")
    payload = manifest.model_dump(mode="json")
    payload["unexpected"] = True

    with pytest.raises(ValidationError, match="Extra inputs"):
        ScenarioManifest.model_validate(payload)


def test_fault_injection_bounds_and_cross_field_constraints_are_enforced() -> None:
    traffic = load_scenario_manifest(PROJECT_DIR / "scenarios" / "traffic_spike.yaml")
    payload = traffic.model_dump(mode="json")
    payload["execution"]["burst_rate"] = payload["execution"]["baseline_rate"]
    with pytest.raises(ValidationError, match="twice baseline_rate"):
        ScenarioManifest.model_validate(payload)

    database = load_scenario_manifest(PROJECT_DIR / "scenarios" / "database_latency.yaml")
    payload = database.model_dump(mode="json")
    payload["execution"]["database_delay_ms"] = 5_001
    with pytest.raises(ValidationError, match="less than or equal to 5000"):
        ScenarioManifest.model_validate(payload)

    malformed = load_scenario_manifest(PROJECT_DIR / "scenarios" / "malformed_events.yaml")
    payload = malformed.model_dump(mode="json")
    payload["execution"]["valid_event_count"] = 500
    payload["execution"]["malformed_event_count"] = 500
    with pytest.raises(ValidationError, match="must not exceed 750"):
        ScenarioManifest.model_validate(payload)
