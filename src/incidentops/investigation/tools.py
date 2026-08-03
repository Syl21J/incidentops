"""LangChain-compatible wrappers around existing bounded read-only query functions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import AwareDatetime, Field, field_validator, model_validator

from elasticsearch import ApiError, Elasticsearch, TransportError
from incidentops.config import Settings
from incidentops.investigation.models import (
    EvidenceAvailability,
    Identifier,
    InvestigationTaskType,
    InvestigationWindow,
    LogEvidence,
    LogEvidenceType,
    MetricEvidence,
    MetricEvidenceType,
    NegativeEvidence,
    NegativeEvidenceType,
    StrictModel,
    evidence_id_for_task,
)
from incidentops.log_search import LogSearchParams, search_logs
from incidentops.metric_query import (
    MetricQueryError,
    PrometheusClient,
    compare_production_and_processing_rates,
    get_consumer_lag_summary,
    get_processing_latency_summary,
)

MAX_LOG_TIMELINE_ENTRIES = 100
PROMETHEUS_TIMEOUT_SECONDS = 5.0
ELASTICSEARCH_TIMEOUT_SECONDS = 10.0

DATABASE_ERROR_EVENTS = ("database_connection_failed", "database_write_failed")
KAFKA_ERROR_EVENTS = ("consumer_error", "producer_error", "delivery_failed", "topic_error")

ALLOWED_INVESTIGATION_TOOLS = frozenset(InvestigationTaskType)

ToolEvidence = MetricEvidence | LogEvidence | NegativeEvidence


class InvestigationToolInput(StrictModel):
    """The only arguments accepted by every investigation tool."""

    start_time: AwareDatetime
    end_time: AwareDatetime
    run_id: Identifier | None = None
    investigation_attempt: int = Field(default=1, ge=1, le=2)

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        """Normalize tool timestamps to UTC."""

        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_window(self) -> InvestigationToolInput:
        """Apply the investigation hard cap before reaching either backend."""

        InvestigationWindow(start_time=self.start_time, end_time=self.end_time)
        return self


class InvestigationToolset:
    """Closed dispatcher for six deterministic, read-only investigation operations."""

    def __init__(
        self,
        prometheus_client: PrometheusClient,
        elasticsearch_client: Elasticsearch,
        *,
        owns_elasticsearch_client: bool = False,
    ) -> None:
        self._prometheus = prometheus_client
        self._elasticsearch = elasticsearch_client
        self._owns_elasticsearch_client = owns_elasticsearch_client

    @classmethod
    def from_settings(cls, settings: Settings) -> InvestigationToolset:
        """Construct clients with explicit timeouts and bounded retries."""

        return cls(
            PrometheusClient(
                settings.prometheus_url,
                timeout_seconds=PROMETHEUS_TIMEOUT_SECONDS,
            ),
            Elasticsearch(
                settings.elasticsearch_url,
                request_timeout=ELASTICSEARCH_TIMEOUT_SECONDS,
                retry_on_timeout=True,
                max_retries=2,
            ),
            owns_elasticsearch_client=True,
        )

    def close(self) -> None:
        """Close only an Elasticsearch client created by this toolset."""

        if self._owns_elasticsearch_client:
            self._elasticsearch.close()

    def execute(
        self,
        task_type: InvestigationTaskType,
        tool_input: InvestigationToolInput,
    ) -> ToolEvidence:
        """Execute exactly one allow-listed task without arbitrary query input."""

        if task_type == InvestigationTaskType.CHECK_CONSUMER_LAG:
            return self._check_consumer_lag(tool_input)
        if task_type == InvestigationTaskType.CHECK_PROCESSING_LATENCY:
            return self._check_processing_latency(tool_input)
        if task_type == InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES:
            return self._compare_producer_consumer_rates(tool_input)
        if task_type == InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS:
            return self._find_slow_processing_logs(tool_input)
        if task_type == InvestigationTaskType.FIND_DATABASE_ERRORS:
            return self._find_error_logs(
                tool_input,
                task_type=task_type,
                event_types=DATABASE_ERROR_EVENTS,
                log_type=LogEvidenceType.DATABASE_ERRORS,
                negative_type=NegativeEvidenceType.NO_DATABASE_ERRORS,
                label="database errors",
            )
        if task_type == InvestigationTaskType.FIND_KAFKA_ERRORS:
            return self._find_error_logs(
                tool_input,
                task_type=task_type,
                event_types=KAFKA_ERROR_EVENTS,
                log_type=LogEvidenceType.KAFKA_ERRORS,
                negative_type=NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS,
                label="Kafka errors",
            )
        raise ValueError(f"unsupported investigation task: {task_type}")

    def _unavailable_metric(
        self,
        tool_input: InvestigationToolInput,
        task_type: InvestigationTaskType,
        metric_type: MetricEvidenceType,
        error: Exception,
    ) -> MetricEvidence:
        return MetricEvidence(
            evidence_id=evidence_id_for_task(task_type),
            metric_type=metric_type,
            observation="The bounded Prometheus summary is unavailable.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={"error_type": type(error).__name__},
            availability=EvidenceAvailability.UNAVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
        )

    def _check_consumer_lag(self, tool_input: InvestigationToolInput) -> MetricEvidence:
        try:
            summary = get_consumer_lag_summary(
                self._prometheus,
                start=tool_input.start_time,
                end=tool_input.end_time,
            )
        except (MetricQueryError, ValueError) as error:
            return self._unavailable_metric(
                tool_input,
                InvestigationTaskType.CHECK_CONSUMER_LAG,
                MetricEvidenceType.CONSUMER_LAG,
                error,
            )
        return MetricEvidence(
            evidence_id=evidence_id_for_task(InvestigationTaskType.CHECK_CONSUMER_LAG),
            metric_type=MetricEvidenceType.CONSUMER_LAG,
            observation=f"Consumer lag was {summary.trend} during the incident window.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={
                "start_value": summary.start_value,
                "end_value": summary.end_value,
                "minimum": summary.minimum,
                "maximum": summary.maximum,
                "trend": summary.trend,
                "sample_count": summary.sample_count,
            },
            availability=EvidenceAvailability.AVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
        )

    def _check_processing_latency(self, tool_input: InvestigationToolInput) -> MetricEvidence:
        try:
            summary = get_processing_latency_summary(
                self._prometheus,
                percentile=0.95,
                start=tool_input.start_time,
                end=tool_input.end_time,
            )
        except (MetricQueryError, ValueError) as error:
            return self._unavailable_metric(
                tool_input,
                InvestigationTaskType.CHECK_PROCESSING_LATENCY,
                MetricEvidenceType.PROCESSING_LATENCY,
                error,
            )
        return MetricEvidence(
            evidence_id=evidence_id_for_task(InvestigationTaskType.CHECK_PROCESSING_LATENCY),
            metric_type=MetricEvidenceType.PROCESSING_LATENCY,
            observation="The bounded P95 processing-latency summary was collected.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={
                "percentile": summary.percentile,
                "duration_seconds": summary.duration_seconds,
                "sample_count": summary.sample_count,
            },
            availability=EvidenceAvailability.AVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
        )

    def _compare_producer_consumer_rates(
        self,
        tool_input: InvestigationToolInput,
    ) -> MetricEvidence:
        try:
            summary = compare_production_and_processing_rates(
                self._prometheus,
                start=tool_input.start_time,
                end=tool_input.end_time,
            )
        except (MetricQueryError, ValueError) as error:
            return self._unavailable_metric(
                tool_input,
                InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES,
                MetricEvidenceType.PRODUCER_CONSUMER_RATES,
                error,
            )
        return MetricEvidence(
            evidence_id=evidence_id_for_task(InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES),
            metric_type=MetricEvidenceType.PRODUCER_CONSUMER_RATES,
            observation=(
                "Active producer throughput exceeded active consumer throughput."
                if summary.consumer_is_slower
                else "Active producer throughput did not exceed active consumer throughput."
            ),
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={
                "producer_windowed_rate_per_second": summary.producer_rate,
                "consumer_windowed_rate_per_second": summary.consumer_rate,
                "windowed_rate_difference_per_second": summary.rate_difference,
                "consumer_is_slower": summary.consumer_is_slower,
            },
            availability=EvidenceAvailability.AVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
        )

    def _unavailable_log(
        self,
        tool_input: InvestigationToolInput,
        task_type: InvestigationTaskType,
        log_type: LogEvidenceType,
        error: Exception,
    ) -> LogEvidence:
        return LogEvidence(
            evidence_id=evidence_id_for_task(task_type),
            log_type=log_type,
            observation="The bounded Elasticsearch result is unavailable.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={"error_type": type(error).__name__},
            availability=EvidenceAvailability.UNAVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
            matching_log_count=0,
        )

    def _find_slow_processing_logs(self, tool_input: InvestigationToolInput) -> LogEvidence:
        task_type = InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS
        try:
            result = search_logs(
                self._elasticsearch,
                LogSearchParams(
                    start=tool_input.start_time,
                    end=tool_input.end_time,
                    services=["order-consumer"],
                    event_types=["slow_processing"],
                    run_id=tool_input.run_id,
                    limit=MAX_LOG_TIMELINE_ENTRIES,
                ),
            )
        except (ApiError, TransportError, KeyError, TypeError, ValueError) as error:
            return self._unavailable_log(
                tool_input,
                task_type,
                LogEvidenceType.SLOW_PROCESSING,
                error,
            )
        timeline = [item.timestamp for item in result.logs]
        return LogEvidence(
            evidence_id=evidence_id_for_task(task_type),
            log_type=LogEvidenceType.SLOW_PROCESSING,
            observation=f"Elasticsearch found {result.total} slow-processing events.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={
                "matching_log_count": result.total,
                "timeline_entries_returned": len(timeline),
            },
            availability=EvidenceAvailability.AVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
            matching_log_count=result.total,
            timeline=timeline,
        )

    def _find_error_logs(
        self,
        tool_input: InvestigationToolInput,
        *,
        task_type: InvestigationTaskType,
        event_types: tuple[str, ...],
        log_type: LogEvidenceType,
        negative_type: NegativeEvidenceType,
        label: str,
    ) -> LogEvidence | NegativeEvidence:
        try:
            result = search_logs(
                self._elasticsearch,
                LogSearchParams(
                    start=tool_input.start_time,
                    end=tool_input.end_time,
                    event_types=list(event_types),
                    run_id=tool_input.run_id,
                    limit=MAX_LOG_TIMELINE_ENTRIES,
                ),
            )
        except (ApiError, TransportError, KeyError, TypeError, ValueError) as error:
            return self._unavailable_log(tool_input, task_type, log_type, error)
        if result.total == 0:
            return NegativeEvidence(
                evidence_id=evidence_id_for_task(task_type, negative=True),
                negative_type=negative_type,
                observation=f"No {label} matched the bounded structured filters.",
                start_time=tool_input.start_time,
                end_time=tool_input.end_time,
                raw_value_summary={"matching_log_count": 0},
                availability=EvidenceAvailability.AVAILABLE,
                collection_attempt=tool_input.investigation_attempt,
            )
        timeline = [item.timestamp for item in result.logs]
        return LogEvidence(
            evidence_id=evidence_id_for_task(task_type),
            log_type=log_type,
            observation=f"Elasticsearch found {result.total} {label}.",
            start_time=tool_input.start_time,
            end_time=tool_input.end_time,
            raw_value_summary={
                "matching_log_count": result.total,
                "timeline_entries_returned": len(timeline),
            },
            availability=EvidenceAvailability.AVAILABLE,
            collection_attempt=tool_input.investigation_attempt,
            matching_log_count=result.total,
            timeline=timeline,
        )


TOOL_DESCRIPTIONS: dict[InvestigationTaskType, str] = {
    InvestigationTaskType.CHECK_CONSUMER_LAG: (
        "Return a bounded deterministic consumer-lag summary for the exact incident window."
    ),
    InvestigationTaskType.CHECK_PROCESSING_LATENCY: (
        "Return a bounded deterministic P95 processing-latency summary."
    ),
    InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES: (
        "Compare bounded active producer and consumer throughput summaries."
    ),
    InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS: (
        "Count structured slow-processing events without exposing raw queries."
    ),
    InvestigationTaskType.FIND_DATABASE_ERRORS: (
        "Check structured database error events and explicitly report zero matches."
    ),
    InvestigationTaskType.FIND_KAFKA_ERRORS: (
        "Check structured Kafka error events and explicitly report zero matches."
    ),
}


def build_langchain_tools(toolset: InvestigationToolset) -> list[StructuredTool]:
    """Expose only the closed dispatcher through structured LangChain tools."""

    tools: list[StructuredTool] = []
    for task_type in InvestigationTaskType:

        def invoke_tool(
            *,
            _task_type: InvestigationTaskType = task_type,
            **kwargs: Any,
        ) -> dict[str, Any]:
            tool_input = InvestigationToolInput.model_validate(kwargs)
            result = toolset.execute(_task_type, tool_input)
            return result.model_dump(mode="json")

        tools.append(
            StructuredTool.from_function(
                func=invoke_tool,
                name=task_type.value,
                description=TOOL_DESCRIPTIONS[task_type],
                args_schema=InvestigationToolInput,
            )
        )
    return tools
