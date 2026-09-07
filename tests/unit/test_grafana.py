"""Unit coverage for bounded, dashboard-scoped Grafana annotations."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from email.message import Message
from urllib.error import HTTPError
from urllib.request import Request

import pytest
from pydantic import SecretStr

from incidentops import grafana
from incidentops.config import Settings
from incidentops.validation.models import ScenarioMetadata, SlowConsumerObservations


def metadata() -> ScenarioMetadata:
    """Build neutral scenario output accepted by the annotation boundary."""

    start = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    return ScenarioMetadata(
        scenario_id="database_latency_v1",
        run_id="incident-run-123",
        topic="orders.database-latency.123",
        consumer_group="database-latency-123",
        start_time=start,
        end_time=start + timedelta(minutes=1),
        observations=SlowConsumerObservations(
            maximum_lag=40,
            lag_start=0,
            lag_end=40,
            lag_trend="increasing",
            lag_samples=8,
            p95_seconds=0.9,
            latency_samples=8,
            database_p95_seconds=0.9,
            database_latency_samples=8,
            processing_state="elevated",
            database_state="elevated",
            producer_rate=2,
            consumer_rate=1,
            consumer_is_slower=True,
            producer_baseline_rate=2,
            producer_recent_rate=2,
            producer_rate_change_ratio=1,
            producer_surge=False,
            processing_error_rate=0,
            processing_errors_present=False,
            valid_processing_present=True,
            slow_processing_log_count=0,
            database_operation_slow_log_count=3,
            invalid_event_log_count=0,
            database_error_count=0,
            kafka_error_count=0,
        ),
    )


class FakeResponse:
    """Minimal context-managed HTTP response used by urllib."""

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, limit: int) -> bytes:
        assert limit == grafana.MAX_RESPONSE_BYTES + 1
        return b'{"message":"Annotation added","id":123}'


def test_annotation_uses_exact_window_dashboard_scope_and_basic_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request: Request, *, timeout: float) -> FakeResponse:
        captured["request"] = request
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(grafana, "urlopen", fake_urlopen)
    settings = Settings(
        grafana_url="http://localhost:3300",
        grafana_admin_user="local-admin",
        grafana_admin_password=SecretStr("local-password"),
        grafana_timeout_seconds=4,
    )

    result = grafana.add_scenario_annotation(metadata(), settings)

    request = captured["request"]
    assert isinstance(request, Request)
    assert request.full_url == "http://localhost:3300/api/annotations"
    assert request.get_method() == "POST"
    assert request.get_header("Authorization") == (
        "Basic " + base64.b64encode(b"local-admin:local-password").decode("ascii")
    )
    assert captured["timeout"] == 4
    assert isinstance(request.data, bytes)
    payload = json.loads(request.data)
    assert payload == {
        "dashboardUID": "incidentops-pipeline",
        "time": 1_788_436_800_000,
        "timeEnd": 1_788_436_860_000,
        "tags": ["incidentops-scenario", "database_latency_v1"],
        "text": "Scenario database_latency_v1 (incident-run-123)",
    }
    assert result.annotation_id == 123
    assert result.dashboard_url == (
        "http://localhost:3300/d/incidentops-pipeline/incidentops-order-pipeline"
        "?from=1788436770000&to=1788436875000"
    )


@pytest.mark.parametrize(
    "url",
    [
        "localhost:3000",
        "ftp://localhost:3000",
        "http://admin:secret@localhost:3000",
        "http://localhost:3000/d/dashboard",
        "http://localhost:3000?orgId=1",
    ],
)
def test_annotation_rejects_non_origin_grafana_urls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    def unexpected_urlopen(*_: object, **__: object) -> None:
        pytest.fail("invalid Grafana URL reached the network boundary")

    monkeypatch.setattr(grafana, "urlopen", unexpected_urlopen)
    settings = Settings(grafana_url=url)

    with pytest.raises(grafana.GrafanaAnnotationError, match=r"HTTP\(S\) origin"):
        grafana.add_scenario_annotation(metadata(), settings)


def test_authentication_error_is_actionable_without_leaking_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unauthorized(request: Request, *, timeout: float) -> None:
        raise HTTPError(request.full_url, 401, "Unauthorized", Message(), None)

    monkeypatch.setattr(grafana, "urlopen", unauthorized)
    settings = Settings(grafana_admin_password=SecretStr("never-print-this"))

    with pytest.raises(grafana.GrafanaAnnotationError) as captured:
        grafana.add_scenario_annotation(metadata(), settings)

    message = str(captured.value)
    assert "GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD" in message
    assert "never-print-this" not in message
