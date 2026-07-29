"""Unit tests for bounded Elasticsearch log queries and response validation."""

from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from elasticsearch import Elasticsearch
from incidentops.log_search import (
    MAX_SEARCH_RESULTS,
    LogAggregationParams,
    LogSearchParams,
    LogTimelineParams,
    build_log_query,
    count_logs_by_event_type,
    get_log_timeline,
    search_logs,
)

START = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
END = START + timedelta(minutes=30)


def mock_client(response_body: dict[str, object]) -> tuple[Elasticsearch, MagicMock]:
    """Return a typed Elasticsearch mock with one configured response."""

    client_mock = MagicMock(spec=Elasticsearch)
    response_mock = MagicMock()
    response_mock.body = response_body
    client_mock.search.return_value = response_mock
    return cast(Elasticsearch, client_mock), client_mock


def test_build_log_query_uses_only_validated_structured_filters() -> None:
    params = LogSearchParams(
        start=START,
        end=END,
        services=["order-consumer"],
        levels=["INFO"],
        event_types=["order_processed"],
        run_id="query-test",
        event_id="event-1",
        order_id="order-1",
        message="processed order",
    )

    query = build_log_query(params)

    assert query["bool"]["filter"] == [
        {
            "range": {
                "@timestamp": {
                    "gte": "2026-07-29T10:00:00Z",
                    "lte": "2026-07-29T10:30:00Z",
                }
            }
        },
        {"terms": {"service": ["order-consumer"]}},
        {"terms": {"level": ["INFO"]}},
        {"terms": {"event_type": ["order_processed"]}},
        {"term": {"run_id": "query-test"}},
        {"term": {"event_id": "event-1"}},
        {"term": {"order_id": "order-1"}},
    ]
    assert query["bool"]["must"] == [{"match": {"message": "processed order"}}]


def test_search_limit_and_time_window_are_bounded() -> None:
    with pytest.raises(ValidationError):
        LogSearchParams(start=START, end=END, limit=MAX_SEARCH_RESULTS + 1)

    with pytest.raises(ValidationError):
        LogSearchParams(start=START, end=START + timedelta(days=8))

    defaults = LogSearchParams()
    assert defaults.start is not None
    assert defaults.end is not None
    assert defaults.end - defaults.start == timedelta(minutes=15)


def test_search_logs_sorts_and_validates_hits() -> None:
    source = {
        "@timestamp": "2026-07-29T10:01:00Z",
        "level": "INFO",
        "service": "order-consumer",
        "event_type": "order_processed",
        "message": "Order event processed",
        "logger": "order-consumer",
        "event_id": "event-1",
        "order_id": "order-1",
        "duration_ms": 1.5,
        "run_id": "response-test",
    }
    client, client_mock = mock_client(
        {"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [{"_source": source}]}}
    )

    result = search_logs(
        client,
        LogSearchParams(start=START, end=END, services=["order-consumer"], limit=10),
    )

    assert result.total == 1
    assert result.logs[0].service == "order-consumer"
    call = client_mock.search.call_args.kwargs
    assert call["size"] == 10
    assert call["sort"][0] == {"@timestamp": {"order": "asc"}}
    assert call["track_total_hits"] is True


def test_search_logs_rejects_invalid_elasticsearch_source() -> None:
    client, _ = mock_client(
        {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": "not-a-date",
                            "level": "NOTICE",
                            "service": "order-consumer",
                        }
                    }
                ],
            }
        }
    )

    with pytest.raises(ValidationError):
        search_logs(client, LogSearchParams(start=START, end=END))


def test_count_logs_builds_allow_listed_terms_aggregation() -> None:
    client, client_mock = mock_client(
        {
            "hits": {"total": 0, "hits": []},
            "aggregations": {
                "grouped_logs": {
                    "buckets": [
                        {"key": "order_processed", "doc_count": 4},
                        {"key": "consumer_started", "doc_count": 1},
                    ]
                }
            },
        }
    )

    result = count_logs_by_event_type(
        client,
        LogAggregationParams(
            start=START,
            end=END,
            group_by="event_type",
            run_id="aggregate-test",
        ),
    )

    assert result.buckets[0].count == 4
    call = client_mock.search.call_args.kwargs
    assert call["size"] == 0
    assert call["aggs"]["grouped_logs"]["terms"]["field"] == "event_type"
    assert {"term": {"run_id": "aggregate-test"}} in call["query"]["bool"]["filter"]


def test_timeline_builds_fixed_interval_aggregation() -> None:
    client, client_mock = mock_client(
        {
            "hits": {"total": 0, "hits": []},
            "aggregations": {
                "log_timeline": {
                    "buckets": [
                        {
                            "key_as_string": "2026-07-29T10:00:00.000Z",
                            "key": 0,
                            "doc_count": 3,
                        }
                    ]
                }
            },
        }
    )

    result = get_log_timeline(
        client,
        LogTimelineParams(
            start=START,
            end=END,
            interval="1m",
            services=["order-consumer"],
        ),
    )

    assert result.buckets[0].count == 3
    aggregation = client_mock.search.call_args.kwargs["aggs"]["log_timeline"]["date_histogram"]
    assert aggregation["fixed_interval"] == "1m"
    assert aggregation["min_doc_count"] == 0
