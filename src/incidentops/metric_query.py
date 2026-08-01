"""Typed, bounded Prometheus queries with no arbitrary PromQL surface."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from incidentops.config import Settings, get_settings

ALLOWED_METRICS = frozenset(
    {
        "incidentops_orders_produced_total",
        "incidentops_order_production_errors_total",
        "incidentops_order_production_duration_seconds",
        "incidentops_producer_target_rate",
        "incidentops_orders_consumed_total",
        "incidentops_orders_processed_total",
        "incidentops_order_processing_errors_total",
        "incidentops_order_processing_duration_seconds",
        "incidentops_database_operation_duration_seconds",
        "incidentops_kafka_consumer_lag",
    }
)
ALLOWED_LABEL_FILTERS = frozenset({"job", "instance"})
LABEL_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
MAX_QUERY_RANGE = timedelta(hours=6)
MIN_STEP_SECONDS = 1
MAX_STEP_SECONDS = 300
MAX_RESPONSE_BYTES = 2_000_000
MAX_SERIES = 50
MAX_SAMPLES = 10_000
DEFAULT_TIMEOUT_SECONDS = 5.0


class MetricQueryError(RuntimeError):
    """Report a bounded query validation, transport, or response failure."""


class RangeQueryParams(BaseModel):
    """Validated public inputs for a simple allow-listed metric selector."""

    model_config = ConfigDict(extra="forbid")

    metric: str
    labels: dict[str, str] = Field(default_factory=dict)
    start: AwareDatetime
    end: AwareDatetime
    step_seconds: int = Field(ge=MIN_STEP_SECONDS, le=MAX_STEP_SECONDS)

    @field_validator("metric")
    @classmethod
    def allow_metric_name(cls, value: str) -> str:
        """Reject every metric outside the deliberately small public allowlist."""

        if value not in ALLOWED_METRICS:
            raise ValueError("metric is not allow-listed")
        return value

    @field_validator("labels")
    @classmethod
    def validate_labels(cls, value: dict[str, str]) -> dict[str, str]:
        """Allow only exact, low-cardinality scrape identity filters."""

        if not set(value) <= ALLOWED_LABEL_FILTERS:
            raise ValueError("only job and instance label filters are allowed")
        if any(not LABEL_VALUE_PATTERN.fullmatch(item) for item in value.values()):
            raise ValueError("label filter contains unsupported characters")
        return value

    @model_validator(mode="after")
    def validate_window(self) -> RangeQueryParams:
        """Require an ordered UTC-aware interval no longer than six hours."""

        if self.start >= self.end:
            raise ValueError("start must be earlier than end")
        if self.end - self.start > MAX_QUERY_RANGE:
            raise ValueError("metric query range must not exceed six hours")
        return self


class MetricSample(BaseModel):
    """One finite sample returned by Prometheus."""

    timestamp: AwareDatetime
    value: float


class MetricSeries(BaseModel):
    """One bounded Prometheus time series."""

    labels: dict[str, str]
    samples: list[MetricSample]


class MetricRangeResult(BaseModel):
    """Validated response for one allow-listed range query."""

    metric: str
    start: AwareDatetime
    end: AwareDatetime
    step_seconds: int
    series: list[MetricSeries]


class CurrentMetricSample(BaseModel):
    """One current value and its scrape labels."""

    timestamp: AwareDatetime
    value: float
    labels: dict[str, str]


class CurrentMetricResult(BaseModel):
    """Validated current values for an allow-listed metric."""

    metric: str
    samples: list[CurrentMetricSample]


class ConsumerLagSummary(BaseModel):
    """Deterministic aggregate lag statistics over a bounded interval."""

    metric: Literal["incidentops_kafka_consumer_lag"] = "incidentops_kafka_consumer_lag"
    start_value: float = Field(ge=0)
    end_value: float = Field(ge=0)
    minimum: float = Field(ge=0)
    maximum: float = Field(ge=0)
    trend: Literal["increasing", "stable", "decreasing"]
    sample_count: int = Field(gt=0)


class ProcessingLatencySummary(BaseModel):
    """A safe histogram percentile result."""

    metric: Literal["incidentops_order_processing_duration_seconds"] = (
        "incidentops_order_processing_duration_seconds"
    )
    percentile: float = Field(gt=0, lt=1)
    start: AwareDatetime
    end: AwareDatetime
    sample_count: int = Field(gt=0)
    duration_seconds: float = Field(ge=0)


class RateComparison(BaseModel):
    """Producer and successful consumer rates over the same interval."""

    start: AwareDatetime
    end: AwareDatetime
    producer_rate: float = Field(ge=0)
    consumer_rate: float = Field(ge=0)
    rate_difference: float
    consumer_is_slower: bool


def _selector(metric: str, labels: Mapping[str, str]) -> str:
    """Build an exact selector after public input has passed validation."""

    if not labels:
        return metric
    filters = ",".join(f'{key}="{value}"' for key, value in sorted(labels.items()))
    return f"{metric}{{{filters}}}"


def _parse_timestamp(value: str | int | float) -> datetime:
    timestamp = float(value)
    if not math.isfinite(timestamp):
        raise MetricQueryError("Prometheus returned a non-finite timestamp")
    return datetime.fromtimestamp(timestamp, tz=UTC)


def _parse_value(value: str | int | float) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise MetricQueryError("Prometheus returned a non-finite metric value")
    return parsed


class PrometheusClient:
    """Small read-only client for the Prometheus v1 HTTP API."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_response_bytes: int = MAX_RESPONSE_BYTES,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes

    def _request(self, endpoint: str, parameters: Mapping[str, str]) -> dict[str, Any]:
        url = f"{self.base_url}{endpoint}?{urlencode(parameters)}"
        request = Request(url, headers={"Accept": "application/json"})
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self.max_response_bytes:
                    raise MetricQueryError("Prometheus response exceeds the size limit")
                payload = response.read(self.max_response_bytes + 1)
        except (HTTPError, URLError, OSError, TimeoutError) as error:
            raise MetricQueryError(f"Prometheus is unavailable: {error}") from error
        if len(payload) > self.max_response_bytes:
            raise MetricQueryError("Prometheus response exceeds the size limit")
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise MetricQueryError("Prometheus returned invalid JSON") from error
        if not isinstance(decoded, dict) or decoded.get("status") != "success":
            raise MetricQueryError("Prometheus returned an unsuccessful response")
        return decoded

    def _range_expression(
        self,
        expression: str,
        *,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[MetricSeries]:
        payload = self._request(
            "/api/v1/query_range",
            {
                "query": expression,
                "start": start.astimezone(UTC).isoformat(),
                "end": end.astimezone(UTC).isoformat(),
                "step": str(step_seconds),
            },
        )
        try:
            result = payload["data"]["result"]
        except (KeyError, TypeError) as error:
            raise MetricQueryError("Prometheus range response has an invalid shape") from error
        if not isinstance(result, list) or len(result) > MAX_SERIES:
            raise MetricQueryError("Prometheus returned too many series")
        parsed: list[MetricSeries] = []
        sample_count = 0
        for raw_series in result:
            try:
                labels = raw_series["metric"]
                values = raw_series["values"]
            except (KeyError, TypeError) as error:
                raise MetricQueryError("Prometheus series has an invalid shape") from error
            if not isinstance(labels, dict) or not isinstance(values, list):
                raise MetricQueryError("Prometheus series has an invalid shape")
            samples = [
                MetricSample(timestamp=_parse_timestamp(item[0]), value=_parse_value(item[1]))
                for item in values
            ]
            sample_count += len(samples)
            if sample_count > MAX_SAMPLES:
                raise MetricQueryError("Prometheus returned too many samples")
            parsed.append(MetricSeries(labels=labels, samples=samples))
        return parsed

    def query_metric_range(self, params: RangeQueryParams) -> MetricRangeResult:
        """Query only one allow-listed metric with exact scrape-label filters."""

        series = self._range_expression(
            _selector(params.metric, params.labels),
            start=params.start,
            end=params.end,
            step_seconds=params.step_seconds,
        )
        return MetricRangeResult(
            metric=params.metric,
            start=params.start,
            end=params.end,
            step_seconds=params.step_seconds,
            series=series,
        )

    def get_current_metric(
        self,
        metric: str,
        labels: Mapping[str, str] | None = None,
    ) -> CurrentMetricResult:
        """Return current values after applying the same public allowlist."""

        now = datetime.now(UTC)
        params = RangeQueryParams(
            metric=metric,
            labels=dict(labels or {}),
            start=now - timedelta(seconds=1),
            end=now,
            step_seconds=1,
        )
        payload = self._request("/api/v1/query", {"query": _selector(params.metric, params.labels)})
        try:
            result = payload["data"]["result"]
        except (KeyError, TypeError) as error:
            raise MetricQueryError("Prometheus instant response has an invalid shape") from error
        if not isinstance(result, list) or len(result) > MAX_SERIES:
            raise MetricQueryError("Prometheus returned too many series")
        samples: list[CurrentMetricSample] = []
        for item in result:
            try:
                timestamp, value = item["value"]
                raw_labels = item["metric"]
            except (KeyError, TypeError, ValueError) as error:
                raise MetricQueryError("Prometheus instant result has an invalid shape") from error
            samples.append(
                CurrentMetricSample(
                    timestamp=_parse_timestamp(timestamp),
                    value=_parse_value(value),
                    labels=raw_labels,
                )
            )
        return CurrentMetricResult(metric=metric, samples=samples)


def query_metric_range(client: PrometheusClient, params: RangeQueryParams) -> MetricRangeResult:
    """Public functional wrapper for an allow-listed range query."""

    return client.query_metric_range(params)


def get_current_metric(
    client: PrometheusClient,
    metric: str,
    labels: Mapping[str, str] | None = None,
) -> CurrentMetricResult:
    """Public functional wrapper for one current allow-listed metric."""

    return client.get_current_metric(metric, labels)


def _summed_samples(series: list[MetricSeries]) -> list[MetricSample]:
    totals: dict[datetime, float] = {}
    for item in series:
        for sample in item.samples:
            totals[sample.timestamp] = totals.get(sample.timestamp, 0.0) + sample.value
    return [
        MetricSample(timestamp=timestamp, value=totals[timestamp]) for timestamp in sorted(totals)
    ]


def calculate_trend(values: list[float]) -> Literal["increasing", "stable", "decreasing"]:
    """Classify start-to-end movement with a deterministic one-unit tolerance."""

    if not values:
        raise MetricQueryError("no metric samples are available")
    tolerance = max(1.0, max(values) * 0.05)
    difference = values[-1] - values[0]
    if difference > tolerance:
        return "increasing"
    if difference < -tolerance:
        return "decreasing"
    return "stable"


def get_consumer_lag_summary(
    client: PrometheusClient,
    *,
    start: datetime,
    end: datetime,
    step_seconds: int = 2,
) -> ConsumerLagSummary:
    """Summarize total consumer lag without asking Prometheus for arbitrary PromQL."""

    result = client.query_metric_range(
        RangeQueryParams(
            metric="incidentops_kafka_consumer_lag",
            labels={"job": "incidentops-consumer"},
            start=start,
            end=end,
            step_seconds=step_seconds,
        )
    )
    samples = _summed_samples(result.series)
    if not samples:
        raise MetricQueryError("no consumer lag samples are available")
    values = [sample.value for sample in samples]
    return ConsumerLagSummary(
        start_value=values[0],
        end_value=values[-1],
        minimum=min(values),
        maximum=max(values),
        trend=calculate_trend(values),
        sample_count=len(values),
    )


def get_processing_latency_summary(
    client: PrometheusClient,
    *,
    percentile: float,
    start: datetime,
    end: datetime,
    step_seconds: int = 2,
) -> ProcessingLatencySummary:
    """Return one percentile from a fixed processing-histogram query template."""

    if percentile not in {0.5, 0.9, 0.95, 0.99}:
        raise MetricQueryError("percentile must be one of 0.5, 0.9, 0.95, or 0.99")
    RangeQueryParams(
        metric="incidentops_order_processing_duration_seconds",
        start=start,
        end=end,
        step_seconds=step_seconds,
    )
    expression = (
        f"histogram_quantile({percentile}, sum by (le) "
        "(rate(incidentops_order_processing_duration_seconds_bucket[30s])))"
    )
    samples = _summed_samples(
        client._range_expression(
            expression,
            start=start,
            end=end,
            step_seconds=step_seconds,
        )
    )
    if not samples:
        raise MetricQueryError("no processing latency samples are available")
    return ProcessingLatencySummary(
        percentile=percentile,
        start=start,
        end=end,
        sample_count=len(samples),
        duration_seconds=samples[-1].value,
    )


def _mean_sample_value(series: list[MetricSeries], name: str) -> float:
    samples = _summed_samples(series)
    if not samples:
        raise MetricQueryError(f"no {name} rate samples are available")
    return sum(sample.value for sample in samples) / len(samples)


def compare_production_and_processing_rates(
    client: PrometheusClient,
    *,
    start: datetime,
    end: datetime,
    step_seconds: int = 2,
) -> RateComparison:
    """Compare fixed 30-second producer and consumer counter rates."""

    RangeQueryParams(
        metric="incidentops_orders_produced_total",
        start=start,
        end=end,
        step_seconds=step_seconds,
    )
    producer_rate = _mean_sample_value(
        client._range_expression(
            "rate(incidentops_orders_produced_total[30s])",
            start=start,
            end=end,
            step_seconds=step_seconds,
        ),
        "producer",
    )
    consumer_rate = _mean_sample_value(
        client._range_expression(
            "rate(incidentops_orders_processed_total[30s])",
            start=start,
            end=end,
            step_seconds=step_seconds,
        ),
        "consumer",
    )
    difference = producer_rate - consumer_rate
    return RateComparison(
        start=start,
        end=end,
        producer_rate=max(0.0, producer_rate),
        consumer_rate=max(0.0, consumer_rate),
        rate_difference=difference,
        consumer_is_slower=difference > 0,
    )


def _bounded_minutes(value: str) -> int:
    minutes = int(value)
    maximum = int(MAX_QUERY_RANGE.total_seconds() // 60)
    if not 1 <= minutes <= maximum:
        raise argparse.ArgumentTypeError(f"minutes must be between 1 and {maximum}")
    return minutes


def build_parser() -> argparse.ArgumentParser:
    """Build the safe metrics summary CLI."""

    parser = argparse.ArgumentParser(description="Query bounded IncidentOps Prometheus metrics.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("lag", "rates"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("--minutes", type=_bounded_minutes, default=10)
    latency = subparsers.add_parser("latency")
    latency.add_argument("--minutes", type=_bounded_minutes, default=10)
    latency.add_argument("--percentile", type=float, choices=[0.5, 0.9, 0.95, 0.99], default=0.95)
    return parser


def run_cli(arguments: argparse.Namespace, settings: Settings) -> int:
    """Execute one predefined metric operation and print validated JSON."""

    end = datetime.now(UTC)
    start = end - timedelta(minutes=arguments.minutes)
    client = PrometheusClient(settings.prometheus_url)
    try:
        if arguments.command == "lag":
            result: BaseModel = get_consumer_lag_summary(client, start=start, end=end)
        elif arguments.command == "rates":
            result = compare_production_and_processing_rates(client, start=start, end=end)
        else:
            result = get_processing_latency_summary(
                client,
                percentile=arguments.percentile,
                start=start,
                end=end,
            )
    except (MetricQueryError, ValueError) as error:
        print(f"Metric query failed: {error}", file=sys.stderr)
        return 1
    print(result.model_dump_json(indent=2))
    return 0


def main() -> None:
    """Run the bounded metric query CLI."""

    settings = get_settings()
    sys.exit(run_cli(build_parser().parse_args(), settings))


if __name__ == "__main__":
    main()
