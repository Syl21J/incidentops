"""Tests for typed helpers used by the repository validation scripts."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from incidentops.investigation.policy import InvestigationPolicy
from incidentops.validation.checks import (
    ValidationCheckError,
    delete_run_logs,
    delete_run_logs_and_verify,
    error_log_counts,
    log_services_ready,
    prometheus_targets_ready,
    scenario_window,
    slow_consumer_metrics_ready,
    validate_elasticsearch_mappings,
    validate_log_correlation,
    validate_service_aggregation,
    wait_for_run_logs_to_settle,
    write_scenario_metadata,
)
from incidentops.validation.models import ScenarioMetadata, SlowConsumerMetrics


def slow_metrics(**overrides: object) -> SlowConsumerMetrics:
    """Return a complete metric snapshot that passes scenario thresholds."""

    payload: dict[str, object] = {
        "maximum_lag": 40.0,
        "lag_start": 0.0,
        "lag_end": 35.0,
        "lag_trend": "increasing",
        "lag_samples": 5,
        "p95_seconds": 0.8,
        "latency_samples": 3,
        "producer_rate": 10.0,
        "consumer_rate": 1.0,
        "consumer_is_slower": True,
    }
    payload.update(overrides)
    return SlowConsumerMetrics.model_validate(payload)


def write_json(path: Path, payload: object) -> None:
    """Write one compact JSON fixture."""

    path.write_text(json.dumps(payload), encoding="utf-8")


def test_investigation_policy_groups_and_validates_limits() -> None:
    policy = InvestigationPolicy()

    assert policy.max_time_range_hours == 6
    assert policy.max_tool_calls == 10
    assert policy.max_attempts == 2

    with pytest.raises(ValueError, match="required knowledge retrieval"):
        InvestigationPolicy(knowledge_required=True)
    with pytest.raises(ValueError, match="max_tool_calls"):
        InvestigationPolicy(max_tool_calls=11)
    with pytest.raises(ValueError, match="knowledge_candidate_k"):
        InvestigationPolicy(knowledge_top_k=5, knowledge_candidate_k=4)


def test_elasticsearch_cleanup_rejects_non_validation_run_ids() -> None:
    with pytest.raises(ValidationCheckError, match="isolated validation-run"):
        delete_run_logs("local")


def test_elasticsearch_cleanup_waits_for_delayed_filebeat_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls: list[str] = []
    counts = iter([2, 0, 0, 0])
    monkeypatch.setattr(
        "incidentops.validation.checks.delete_run_logs",
        lambda run_id, **_kwargs: delete_calls.append(run_id),
    )
    monkeypatch.setattr(
        "incidentops.validation.checks.count_run_logs",
        lambda _run_id, **_kwargs: next(counts),
    )
    monkeypatch.setattr("incidentops.validation.checks.time.sleep", lambda _seconds: None)

    delete_run_logs_and_verify(
        "incident-run-123-456",
        maximum_attempts=4,
        stable_zero_observations=3,
    )

    assert delete_calls == ["incident-run-123-456"] * 4


def test_elasticsearch_cleanup_fails_when_documents_never_settle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "incidentops.validation.checks.delete_run_logs",
        lambda _run_id, **_kwargs: None,
    )
    monkeypatch.setattr(
        "incidentops.validation.checks.count_run_logs",
        lambda _run_id, **_kwargs: 1,
    )
    monkeypatch.setattr("incidentops.validation.checks.time.sleep", lambda _seconds: None)

    with pytest.raises(ValidationCheckError, match="1 documents remain"):
        delete_run_logs_and_verify(
            "incident-run-123-456",
            maximum_attempts=3,
            stable_zero_observations=2,
        )


def test_retained_log_wait_ignores_empty_and_changing_filebeat_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts = iter([0, 4, 4, 8, 12, 12, 12, 12, 12, 12, 12, 12])
    sleeps: list[float] = []
    monkeypatch.setattr(
        "incidentops.validation.checks.count_run_logs",
        lambda _run_id, **_kwargs: next(counts),
    )
    monkeypatch.setattr(
        "incidentops.validation.checks.time.sleep",
        lambda seconds: sleeps.append(seconds),
    )

    result = wait_for_run_logs_to_settle("incident-run-123-456")

    assert result == 12
    assert len(sleeps) == 11


def test_retained_log_wait_fails_when_delivery_never_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current = 0

    def changing_count(_run_id: str, **_kwargs: object) -> int:
        nonlocal current
        current += 1
        return current

    monkeypatch.setattr("incidentops.validation.checks.count_run_logs", changing_count)
    monkeypatch.setattr("incidentops.validation.checks.time.sleep", lambda _seconds: None)

    with pytest.raises(ValidationCheckError, match="did not settle"):
        wait_for_run_logs_to_settle(
            "incident-run-123-456",
            maximum_attempts=4,
            stable_observations=2,
        )


def test_slow_consumer_metrics_apply_all_acceptance_thresholds() -> None:
    assert slow_consumer_metrics_ready(
        slow_metrics(),
        minimum_lag=30,
        minimum_p95_seconds=0.7,
    )
    assert not slow_consumer_metrics_ready(
        slow_metrics(consumer_is_slower=False),
        minimum_lag=30,
        minimum_p95_seconds=0.7,
    )


def test_scenario_metadata_is_written_atomically_and_round_trips(tmp_path: Path) -> None:
    metrics_path = tmp_path / "metrics.json"
    output_path = tmp_path / "nested" / "metadata.json"
    metrics_path.write_text(slow_metrics().model_dump_json(), encoding="utf-8")

    write_scenario_metadata(
        output_path,
        metrics_path,
        run_id="slow-consumer-123",
        topic="orders.slow-consumer.123",
        consumer_group="slow-consumer-123",
        start_time="2026-08-01T13:00:00Z",
        end_time="2026-08-01T13:10:00Z",
        slow_processing_log_count=4,
        database_error_count=0,
        kafka_error_count=0,
    )

    metadata = ScenarioMetadata.model_validate_json(output_path.read_text(encoding="utf-8"))
    run_id, start, end = scenario_window(output_path)
    assert metadata.observations.maximum_lag == 40.0
    assert metadata.observations.slow_processing_log_count == 4
    assert run_id == "slow-consumer-123"
    assert start == "2026-08-01T13:00:00+00:00"
    assert end == "2026-08-01T13:10:00+00:00"


def test_scenario_metadata_rejects_an_inverted_window() -> None:
    with pytest.raises(ValidationError, match="start_time must be earlier"):
        ScenarioMetadata.model_validate(
            {
                "run_id": "slow-consumer-123",
                "topic": "orders.slow-consumer.123",
                "consumer_group": "slow-consumer-123",
                "start_time": "2026-08-01T13:10:00Z",
                "end_time": "2026-08-01T13:00:00Z",
                "observations": {
                    **slow_metrics().model_dump(),
                    "slow_processing_log_count": 4,
                    "database_error_count": 0,
                    "kafka_error_count": 0,
                },
            },
        )


def test_prometheus_target_check_uses_scrape_job_names() -> None:
    payload = json.dumps(
        {
            "status": "success",
            "data": {
                "activeTargets": [
                    {"health": "up", "labels": {"job": "incidentops-producer"}},
                    {"health": "up", "labels": {"job": "incidentops-consumer"}},
                ]
            },
        }
    )

    assert prometheus_targets_ready(payload)


def test_log_helpers_validate_services_correlation_and_error_categories(tmp_path: Path) -> None:
    result_path = tmp_path / "logs.json"
    base = {
        "timestamp": "2026-08-01T13:00:00Z",
        "level": "ERROR",
        "message": "bounded fixture",
        "logger": "incidentops.test",
    }
    write_json(
        result_path,
        {
            "total": 3,
            "logs": [
                {
                    **base,
                    "service": "order-producer",
                    "event_type": "producer_error",
                    "event_id": "event-1",
                },
                {
                    **base,
                    "service": "order-consumer",
                    "event_type": "database_write_failed",
                },
                {
                    **base,
                    "service": "order-consumer",
                    "event_type": "slow_processing",
                },
            ],
        },
    )

    assert log_services_ready(result_path)
    assert error_log_counts(result_path) == (1, 1)
    validate_log_correlation(result_path)


def test_service_aggregation_returns_both_application_counts(tmp_path: Path) -> None:
    result_path = tmp_path / "aggregation.json"
    write_json(
        result_path,
        {
            "group_by": "service",
            "buckets": [
                {"key": "order-producer", "count": 4},
                {"key": "order-consumer", "count": 5},
            ],
        },
    )

    assert validate_service_aggregation(result_path) == (4, 5)


def test_elasticsearch_mapping_check_accepts_matching_existing_indices(tmp_path: Path) -> None:
    expected_path = tmp_path / "expected.json"
    installed_path = tmp_path / "installed.json"
    mappings_path = tmp_path / "mappings.json"
    properties = {"run_id": {"type": "keyword"}, "duration_ms": {"type": "double"}}
    write_json(expected_path, {"template": {"mappings": {"properties": properties}}})
    write_json(
        installed_path,
        {
            "index_templates": [
                {"index_template": {"template": {"mappings": {"properties": properties}}}}
            ]
        },
    )
    write_json(
        mappings_path,
        {"incidentops-logs-2026.08.01": {"mappings": {"properties": properties}}},
    )

    validate_elasticsearch_mappings(expected_path, installed_path, mappings_path)
