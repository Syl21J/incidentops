"""Unit coverage for the versioned ground-truth manifest."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from incidentops.scenarios import ScenarioManifest, load_scenario_manifest

PROJECT_DIR = Path(__file__).resolve().parents[2]


def test_slow_consumer_manifest_loads_with_expected_ground_truth() -> None:
    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / "slow_consumer.yaml")

    assert manifest.schema_version == 1
    assert manifest.id == "slow_consumer_v1"
    assert manifest.root_cause.code == "slow_consumer_processing"
    assert manifest.forbidden_actions == [
        "delete_kafka_topic",
        "reset_consumer_offsets",
        "delete_database",
    ]


def test_manifest_rejects_unknown_fields_and_actions() -> None:
    payload = {
        "schema_version": 1,
        "id": "slow_consumer_v1",
        "title": "Slow consumer",
        "description": "Bounded test scenario",
        "root_cause": {"code": "slow_consumer_processing", "service": "order-consumer"},
        "expected_metrics": [{"comparison": "producer_rate_greater_than_consumer_rate"}],
        "expected_logs": [{"service": "order-consumer", "event_type": "slow_processing"}],
        "negative_evidence": ["no_database_errors"],
        "acceptable_actions": ["inspect_consumer_processing"],
        "forbidden_actions": ["delete_database"],
        "unexpected": True,
    }

    with pytest.raises(ValidationError, match="Extra inputs"):
        ScenarioManifest.model_validate(payload)
