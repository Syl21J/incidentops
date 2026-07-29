"""Bounded Elasticsearch log search, aggregation, and timeline tools."""

import argparse
import sys
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from elasticsearch import Elasticsearch
from incidentops.config import Settings, get_settings

INDEX_PATTERN = "incidentops-logs-*"
DEFAULT_WINDOW_MINUTES = 15
MAX_WINDOW = timedelta(days=7)
MAX_SEARCH_RESULTS = 500
MAX_AGGREGATION_BUCKETS = 100

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
AggregationField = Literal["service", "event_type", "level"]
Keyword = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)]
Identifier = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)]
SearchText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
Interval = Annotated[str, StringConstraints(pattern=r"^[1-9]\d*[smhd]$")]


def _utc_iso(value: datetime) -> str:
    """Serialize an aware datetime as an Elasticsearch-compatible UTC timestamp."""

    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class TimeBoundParams(BaseModel):
    """Mandatory bounded time window, with a safe recent default."""

    model_config = ConfigDict(extra="forbid")

    start: AwareDatetime | None = None
    end: AwareDatetime | None = None

    @field_validator("start", "end")
    @classmethod
    def normalize_timestamp(cls, value: datetime | None) -> datetime | None:
        """Normalize provided timestamps to UTC."""

        return None if value is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def resolve_and_validate_window(self) -> "TimeBoundParams":
        """Supply a recent default and reject invalid or excessive windows."""

        end = self.end or datetime.now(UTC)
        start = self.start or end - timedelta(minutes=DEFAULT_WINDOW_MINUTES)
        if start >= end:
            raise ValueError("start must be earlier than end")
        if end - start > MAX_WINDOW:
            raise ValueError("time window must not exceed seven days")
        self.start = start
        self.end = end
        return self


class LogFilterParams(TimeBoundParams):
    """Structured filters shared by search and aggregation operations."""

    services: list[Keyword] = Field(default_factory=list, max_length=20)
    levels: list[LogLevel] = Field(default_factory=list, max_length=5)
    event_types: list[Keyword] = Field(default_factory=list, max_length=20)
    run_id: Identifier | None = None


class LogSearchParams(LogFilterParams):
    """Validated parameters accepted by the full-text log search."""

    event_id: Identifier | None = None
    order_id: Identifier | None = None
    message: SearchText | None = None
    limit: int = Field(default=50, ge=1, le=MAX_SEARCH_RESULTS)


class LogAggregationParams(LogFilterParams):
    """Validated parameters for one allow-listed terms aggregation."""

    group_by: AggregationField = "event_type"


class LogTimelineParams(LogFilterParams):
    """Validated parameters for a bounded date histogram."""

    interval: Interval = "1m"


class LogEntry(BaseModel):
    """One validated application log returned by Elasticsearch."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    timestamp: AwareDatetime = Field(alias="@timestamp")
    level: LogLevel
    service: Keyword
    event_type: Keyword
    message: str
    logger: Keyword
    event_id: Identifier | None = None
    order_id: Identifier | None = None
    duration_ms: float | None = None
    error_type: Keyword | None = None
    run_id: Identifier | None = None


class LogSearchResult(BaseModel):
    """Validated search result and total hit count."""

    total: int = Field(ge=0)
    logs: list[LogEntry]


class LogCountBucket(BaseModel):
    """One terms aggregation bucket."""

    key: str
    count: int = Field(ge=0)


class LogCountResult(BaseModel):
    """Validated terms aggregation response."""

    group_by: AggregationField
    buckets: list[LogCountBucket]


class LogTimelineBucket(BaseModel):
    """One timestamped date-histogram bucket."""

    timestamp: AwareDatetime
    count: int = Field(ge=0)


class LogTimelineResult(BaseModel):
    """Validated date histogram response."""

    interval: str
    buckets: list[LogTimelineBucket]


class _RawTotal(BaseModel):
    value: int = Field(ge=0)


class _RawSearchHit(BaseModel):
    source: dict[str, Any] = Field(alias="_source")


class _RawSearchHits(BaseModel):
    total: int | _RawTotal
    hits: list[_RawSearchHit]


class _RawSearchResponse(BaseModel):
    hits: _RawSearchHits


class _RawTermBucket(BaseModel):
    key: str
    doc_count: int = Field(ge=0)


class _RawTimelineBucket(BaseModel):
    key_as_string: AwareDatetime
    doc_count: int = Field(ge=0)


TERM_BUCKETS = TypeAdapter(list[_RawTermBucket])
TIMELINE_BUCKETS = TypeAdapter(list[_RawTimelineBucket])


def build_log_query(params: LogFilterParams) -> dict[str, Any]:
    """Build an allow-listed Elasticsearch bool query from validated filters."""

    if params.start is None or params.end is None:
        raise ValueError("validated parameters must contain a resolved time window")

    filters: list[dict[str, Any]] = [
        {
            "range": {
                "@timestamp": {
                    "gte": _utc_iso(params.start),
                    "lte": _utc_iso(params.end),
                }
            }
        }
    ]
    if params.services:
        filters.append({"terms": {"service": params.services}})
    if params.levels:
        filters.append({"terms": {"level": params.levels}})
    if params.event_types:
        filters.append({"terms": {"event_type": params.event_types}})
    if params.run_id is not None:
        filters.append({"term": {"run_id": params.run_id}})

    if isinstance(params, LogSearchParams):
        if params.event_id is not None:
            filters.append({"term": {"event_id": params.event_id}})
        if params.order_id is not None:
            filters.append({"term": {"order_id": params.order_id}})

    query: dict[str, Any] = {"bool": {"filter": filters}}
    if isinstance(params, LogSearchParams) and params.message is not None:
        query["bool"]["must"] = [{"match": {"message": params.message}}]
    return query


def search_logs(client: Elasticsearch, params: LogSearchParams) -> LogSearchResult:
    """Search application logs with bounded structured and full-text filters."""

    response = client.search(
        index=INDEX_PATTERN,
        query=build_log_query(params),
        sort=[{"@timestamp": {"order": "asc"}}],
        size=params.limit,
        track_total_hits=True,
    )
    parsed = _RawSearchResponse.model_validate(response.body)
    total = parsed.hits.total
    total_value = total if isinstance(total, int) else total.value
    logs = [LogEntry.model_validate(hit.source) for hit in parsed.hits.hits]
    return LogSearchResult(total=total_value, logs=logs)


def count_logs_by_event_type(
    client: Elasticsearch,
    params: LogAggregationParams,
) -> LogCountResult:
    """Count logs with one allow-listed keyword terms aggregation."""

    response = client.search(
        index=INDEX_PATTERN,
        query=build_log_query(params),
        size=0,
        aggs={
            "grouped_logs": {
                "terms": {
                    "field": params.group_by,
                    "size": MAX_AGGREGATION_BUCKETS,
                }
            }
        },
    )
    raw_buckets = response.body["aggregations"]["grouped_logs"]["buckets"]
    parsed_buckets = TERM_BUCKETS.validate_python(raw_buckets)
    return LogCountResult(
        group_by=params.group_by,
        buckets=[
            LogCountBucket(key=bucket.key, count=bucket.doc_count) for bucket in parsed_buckets
        ],
    )


def get_log_timeline(
    client: Elasticsearch,
    params: LogTimelineParams,
) -> LogTimelineResult:
    """Return a bounded fixed-interval date histogram of application logs."""

    if params.start is None or params.end is None:
        raise ValueError("validated parameters must contain a resolved time window")

    response = client.search(
        index=INDEX_PATTERN,
        query=build_log_query(params),
        size=0,
        aggs={
            "log_timeline": {
                "date_histogram": {
                    "field": "@timestamp",
                    "fixed_interval": params.interval,
                    "min_doc_count": 0,
                    "extended_bounds": {
                        "min": _utc_iso(params.start),
                        "max": _utc_iso(params.end),
                    },
                }
            }
        },
    )
    raw_buckets = response.body["aggregations"]["log_timeline"]["buckets"]
    parsed_buckets = TIMELINE_BUCKETS.validate_python(raw_buckets)
    return LogTimelineResult(
        interval=params.interval,
        buckets=[
            LogTimelineBucket(timestamp=bucket.key_as_string, count=bucket.doc_count)
            for bucket in parsed_buckets
        ],
    )


def _positive_minutes(value: str) -> int:
    """Parse a positive CLI duration bounded by the maximum search window."""

    minutes = int(value)
    maximum_minutes = int(MAX_WINDOW.total_seconds() // 60)
    if not 1 <= minutes <= maximum_minutes:
        raise argparse.ArgumentTypeError(f"minutes must be between 1 and {maximum_minutes}")
    return minutes


def _aware_datetime(value: str) -> datetime:
    """Parse one ISO 8601 timestamp for argparse."""

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("timestamp must use ISO 8601") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed


def _add_common_filters(parser: argparse.ArgumentParser) -> None:
    """Add bounded time and structured filter flags to a subcommand."""

    parser.add_argument("--start", type=_aware_datetime)
    parser.add_argument("--end", type=_aware_datetime)
    parser.add_argument("--minutes", type=_positive_minutes, default=DEFAULT_WINDOW_MINUTES)
    parser.add_argument("--service", action="append", dest="services", default=[])
    parser.add_argument(
        "--level",
        action="append",
        dest="levels",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=[],
    )
    parser.add_argument("--event-type", action="append", dest="event_types", default=[])
    parser.add_argument("--run-id")


def build_parser() -> argparse.ArgumentParser:
    """Build the read-only log search demonstration CLI."""

    parser = argparse.ArgumentParser(description="Search IncidentOps application logs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    search_parser = subparsers.add_parser("search", help="Search structured logs.")
    _add_common_filters(search_parser)
    search_parser.add_argument("--event-id")
    search_parser.add_argument("--order-id")
    search_parser.add_argument("--message")
    search_parser.add_argument("--limit", type=int, default=50)

    aggregate_parser = subparsers.add_parser(
        "aggregate", help="Count logs by an allow-listed field."
    )
    _add_common_filters(aggregate_parser)
    aggregate_parser.add_argument(
        "--group-by",
        choices=["service", "event_type", "level"],
        default="event_type",
    )

    timeline_parser = subparsers.add_parser("timeline", help="Count logs in fixed time intervals.")
    _add_common_filters(timeline_parser)
    timeline_parser.add_argument("--interval", default="1m")
    return parser


def _time_window(arguments: argparse.Namespace) -> tuple[datetime, datetime]:
    """Resolve CLI time flags to a concrete UTC interval."""

    end = arguments.end or datetime.now(UTC)
    start = arguments.start or end - timedelta(minutes=arguments.minutes)
    return start, end


def _common_values(arguments: argparse.Namespace) -> dict[str, Any]:
    """Translate common argparse values to Pydantic model input."""

    start, end = _time_window(arguments)
    return {
        "start": start,
        "end": end,
        "services": arguments.services,
        "levels": arguments.levels,
        "event_types": arguments.event_types,
        "run_id": arguments.run_id,
    }


def run_cli(arguments: argparse.Namespace, settings: Settings) -> int:
    """Execute one read-only command and return its process exit code."""

    client = Elasticsearch(
        settings.elasticsearch_url,
        request_timeout=10,
        retry_on_timeout=True,
        max_retries=2,
    )
    try:
        common = _common_values(arguments)
        if arguments.command == "search":
            result: BaseModel = search_logs(
                client,
                LogSearchParams(
                    **common,
                    event_id=arguments.event_id,
                    order_id=arguments.order_id,
                    message=arguments.message,
                    limit=arguments.limit,
                ),
            )
        elif arguments.command == "aggregate":
            result = count_logs_by_event_type(
                client,
                LogAggregationParams(**common, group_by=arguments.group_by),
            )
        else:
            result = get_log_timeline(
                client,
                LogTimelineParams(**common, interval=arguments.interval),
            )
    except (KeyError, TypeError, ValidationError, ValueError) as error:
        print(f"Log query failed: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        # The CLI boundary converts transport and API errors to a non-zero status.
        print(f"Elasticsearch request failed: {error}", file=sys.stderr)
        return 1
    finally:
        client.close()

    print(result.model_dump_json(indent=2, by_alias=True))
    return 0


def main() -> None:
    """Run the bounded read-only log search CLI."""

    arguments = build_parser().parse_args()
    sys.exit(run_cli(arguments, get_settings()))


if __name__ == "__main__":
    main()
