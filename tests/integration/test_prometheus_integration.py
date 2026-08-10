"""Integration coverage for Prometheus health, scraping, and the typed client."""

import json
import os
import time
from datetime import UTC, datetime, timedelta
from typing import NoReturn
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from incidentops.metric_query import PrometheusClient, RangeQueryParams
from incidentops.metrics import MetricsServer, create_producer_metrics

PROMETHEUS_URL = "http://localhost:9090"
REQUIRE_INTEGRATION = os.getenv("INCIDENTOPS_REQUIRE_INTEGRATION", "").lower() == "true"
pytestmark = [pytest.mark.integration, pytest.mark.prometheus]


def _unavailable(message: str) -> NoReturn:
    """Skip optional local runs but fail when integration coverage is explicitly required."""

    if REQUIRE_INTEGRATION:
        pytest.fail(message)
    pytest.skip(message)


def _require_prometheus() -> None:
    try:
        with urlopen(f"{PROMETHEUS_URL}/-/healthy", timeout=2) as response:  # noqa: S310
            if response.status != 200:
                _unavailable("Prometheus is not healthy on localhost:9090")
    except (URLError, OSError):
        _unavailable("Prometheus is not available on localhost:9090")


def test_prometheus_health_scrape_and_range_query() -> None:
    _require_prometheus()
    metrics = create_producer_metrics()
    metrics.orders_produced.inc(4)
    try:
        server = MetricsServer.start(host="0.0.0.0", port=8001, registry=metrics.registry)
    except OSError:
        _unavailable("producer metrics port 8001 is already occupied")

    try:
        deadline = time.monotonic() + 20
        target_healthy = False
        while time.monotonic() < deadline:
            with urlopen(f"{PROMETHEUS_URL}/api/v1/targets", timeout=2) as response:  # noqa: S310
                payload = json.load(response)
            target_healthy = any(
                target.get("labels", {}).get("job") == "incidentops-producer"
                and target.get("health") == "up"
                for target in payload["data"]["activeTargets"]
            )
            if target_healthy:
                break
            time.sleep(1)
        assert target_healthy

        end = datetime.now(UTC)
        result = PrometheusClient(PROMETHEUS_URL).query_metric_range(
            RangeQueryParams(
                metric="incidentops_orders_produced_total",
                labels={"job": "incidentops-producer"},
                start=end - timedelta(minutes=1),
                end=end,
                step_seconds=2,
            )
        )
        assert any(series.samples for series in result.series)
    finally:
        server.close()
