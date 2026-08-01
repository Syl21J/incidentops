"""Integration coverage for Prometheus health, scraping, and the typed client."""

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

from incidentops.metric_query import PrometheusClient, RangeQueryParams
from incidentops.metrics import MetricsServer, create_producer_metrics
from incidentops.scenarios import load_scenario_manifest

PROMETHEUS_URL = "http://localhost:9090"
PROJECT_DIR = Path(__file__).resolve().parents[2]


def _require_prometheus() -> None:
    try:
        with urlopen(f"{PROMETHEUS_URL}/-/healthy", timeout=2) as response:  # noqa: S310
            if response.status != 200:
                pytest.skip("Prometheus is not healthy on localhost:9090")
    except (URLError, OSError):
        pytest.skip("Prometheus is not available on localhost:9090")


def test_prometheus_health_scrape_range_query_and_manifest() -> None:
    _require_prometheus()
    metrics = create_producer_metrics()
    metrics.orders_produced.inc(4)
    try:
        server = MetricsServer.start(host="0.0.0.0", port=8001, registry=metrics.registry)
    except OSError:
        pytest.skip("producer metrics port 8001 is already occupied")

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

    manifest = load_scenario_manifest(PROJECT_DIR / "scenarios" / "slow_consumer.yaml")
    assert manifest.id == "slow_consumer_v1"
