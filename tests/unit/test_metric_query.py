"""Unit coverage for bounded Prometheus query construction and parsing."""

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from incidentops.metric_query import (
    MAX_QUERY_RANGE,
    MetricQueryError,
    MetricSample,
    MetricSeries,
    PrometheusClient,
    RangeQueryParams,
    calculate_trend,
    compare_production_and_processing_rates,
    get_processing_latency_summary,
)

NOW = datetime(2026, 8, 1, 10, 0, tzinfo=UTC)


class ResponseClient(PrometheusClient):
    """Return an in-memory API payload and retain request parameters."""

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__("http://prometheus.test")
        self.payload = payload
        self.requests: list[tuple[str, dict[str, str]]] = []

    def _request(self, endpoint: str, parameters: Mapping[str, str]) -> dict[str, Any]:
        self.requests.append((endpoint, dict(parameters)))
        return self.payload


class RateClient(PrometheusClient):
    """Return deterministic samples for the two internal rate templates."""

    def __init__(self) -> None:
        super().__init__("http://prometheus.test")

    def _range_expression(
        self,
        expression: str,
        *,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[MetricSeries]:
        del end, step_seconds
        value = 20.0 if "produced" in expression else 1.25
        return [MetricSeries(labels={}, samples=[MetricSample(timestamp=start, value=value)])]


class TrafficRateClient(PrometheusClient):
    """Return an observed producer surge and a healthy processing stream."""

    def __init__(self) -> None:
        super().__init__("http://prometheus.test")

    def _range_expression(
        self,
        expression: str,
        *,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[MetricSeries]:
        del end, step_seconds
        if "orders_produced" in expression:
            values = [2.0, 2.0, 2.0, 20.0, 20.0, 20.0]
        elif "processing_errors" in expression:
            values = [0.0] * 6
        else:
            values = [5.0] * 6
        return [
            MetricSeries(
                labels={},
                samples=[
                    MetricSample(timestamp=start + timedelta(seconds=index * 2), value=value)
                    for index, value in enumerate(values)
                ],
            )
        ]


class LatencyProfileClient(PrometheusClient):
    """Return separate processing and database quantile samples."""

    def __init__(self) -> None:
        super().__init__("http://prometheus.test")

    def _range_expression(
        self,
        expression: str,
        *,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[MetricSeries]:
        del end, step_seconds
        value = 0.8 if "database_operation" in expression else 0.9
        return [
            MetricSeries(
                labels={},
                samples=[MetricSample(timestamp=start, value=value)],
            )
        ]


def test_query_builds_only_exact_allowlisted_selector() -> None:
    client = ResponseClient({"status": "success", "data": {"resultType": "matrix", "result": []}})
    params = RangeQueryParams(
        metric="incidentops_kafka_consumer_lag",
        labels={"job": "incidentops-consumer"},
        start=NOW - timedelta(minutes=5),
        end=NOW,
        step_seconds=2,
    )

    result = client.query_metric_range(params)

    assert result.series == []
    assert client.requests[0][0] == "/api/v1/query_range"
    assert client.requests[0][1]["query"] == (
        'incidentops_kafka_consumer_lag{job="incidentops-consumer"}'
    )


def test_metric_name_and_label_filters_are_allowlisted() -> None:
    common = {"start": NOW - timedelta(minutes=1), "end": NOW, "step_seconds": 2}
    with pytest.raises(ValidationError, match="allow-listed"):
        RangeQueryParams(metric="process_resident_memory_bytes", **common)
    with pytest.raises(ValidationError, match="job and instance"):
        RangeQueryParams(
            metric="incidentops_kafka_consumer_lag",
            labels={"run_id": "forbidden"},
            **common,
        )


def test_query_time_range_and_step_are_bounded() -> None:
    with pytest.raises(ValidationError, match="six hours"):
        RangeQueryParams(
            metric="incidentops_kafka_consumer_lag",
            start=NOW - MAX_QUERY_RANGE - timedelta(seconds=1),
            end=NOW,
            step_seconds=2,
        )
    with pytest.raises(ValidationError):
        RangeQueryParams(
            metric="incidentops_kafka_consumer_lag",
            start=NOW - timedelta(minutes=1),
            end=NOW,
            step_seconds=0,
        )


def test_prometheus_matrix_response_is_parsed_and_bounded() -> None:
    client = ResponseClient(
        {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {"job": "incidentops-consumer"},
                        "values": [[1785578400, "0"], [1785578402, "12"]],
                    }
                ],
            },
        }
    )
    result = client.query_metric_range(
        RangeQueryParams(
            metric="incidentops_kafka_consumer_lag",
            start=NOW - timedelta(minutes=1),
            end=NOW,
            step_seconds=2,
        )
    )

    assert [sample.value for sample in result.series[0].samples] == [0, 12]


def test_prometheus_non_finite_values_are_rejected() -> None:
    client = ResponseClient(
        {
            "status": "success",
            "data": {
                "result": [{"metric": {}, "values": [[1785578400, "NaN"]]}],
            },
        }
    )
    with pytest.raises(MetricQueryError, match="non-finite"):
        client.query_metric_range(
            RangeQueryParams(
                metric="incidentops_kafka_consumer_lag",
                start=NOW - timedelta(minutes=1),
                end=NOW,
                step_seconds=2,
            )
        )


@pytest.mark.parametrize(
    ("values", "expected"),
    [([0.0, 20.0], "increasing"), ([20.0, 0.0], "decreasing"), ([10.0, 10.5], "stable")],
)
def test_trend_calculation_is_deterministic(values: list[float], expected: str) -> None:
    assert calculate_trend(values) == expected


def test_rate_comparison_uses_predefined_queries() -> None:
    comparison = compare_production_and_processing_rates(
        RateClient(),
        start=NOW - timedelta(minutes=1),
        end=NOW,
    )

    assert comparison.producer_rate == 20
    assert comparison.consumer_rate == 1.25
    assert comparison.rate_difference == 18.75
    assert comparison.consumer_is_slower is True


def test_traffic_spike_is_derived_from_observed_producer_counter_rates() -> None:
    comparison = compare_production_and_processing_rates(
        TrafficRateClient(),
        start=NOW - timedelta(minutes=1),
        end=NOW,
    )

    assert comparison.producer_baseline_rate == 2
    assert comparison.producer_recent_rate == 20
    assert comparison.producer_rate_change_ratio == 10
    assert comparison.producer_surge is True
    assert comparison.processing_errors_present is False
    assert comparison.valid_processing_present is True


def test_processing_profile_distinguishes_database_operation_latency() -> None:
    summary = get_processing_latency_summary(
        LatencyProfileClient(),
        percentile=0.95,
        start=NOW - timedelta(minutes=1),
        end=NOW,
    )

    assert summary.processing_state == "elevated"
    assert summary.database_state == "elevated"
    assert summary.duration_seconds == 0.9
    assert summary.database_duration_seconds == 0.8
