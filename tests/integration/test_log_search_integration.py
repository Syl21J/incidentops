"""Integration coverage for the versioned log template and search operations."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from elasticsearch import Elasticsearch
from incidentops.log_search import (
    LogAggregationParams,
    LogSearchParams,
    LogTimelineParams,
    count_logs_by_event_type,
    get_log_timeline,
    search_logs,
)

PROJECT_DIR = Path(__file__).resolve().parents[2]
TEMPLATE_FILE = PROJECT_DIR / "elasticsearch" / "index-template.json"


def test_template_index_search_filters_and_aggregations() -> None:
    """Exercise the complete read path using only uniquely named test resources."""

    client = Elasticsearch("http://localhost:9200", request_timeout=5)
    if not client.ping():
        pytest.skip("Elasticsearch is not available on localhost:9200")

    run_id = f"integration-{uuid4().hex}"
    template_name = f"incidentops-logs-test-{uuid4().hex}"
    index_name = f"incidentops-logs-test-{uuid4().hex}-000001"
    timestamp = datetime.now(UTC) - timedelta(minutes=1)
    template = json.loads(TEMPLATE_FILE.read_text(encoding="utf-8"))
    template["index_patterns"] = [f"{index_name.rsplit('-', 1)[0]}-*"]
    template["priority"] = 600

    try:
        client.indices.put_index_template(
            name=template_name,
            index_patterns=template["index_patterns"],
            priority=template["priority"],
            version=template["version"],
            meta=template["_meta"],
            template=template["template"],
        )
        assert client.indices.exists_index_template(name=template_name)

        document = {
            "@timestamp": timestamp.isoformat().replace("+00:00", "Z"),
            "level": "INFO",
            "service": "order-consumer",
            "event_type": "integration_event",
            "message": "Unique searchable integration needle",
            "logger": "order-consumer",
            "event_id": f"event-{run_id}",
            "order_id": f"order-{run_id}",
            "duration_ms": 3.5,
            "run_id": run_id,
        }
        client.index(index=index_name, document=document, refresh="wait_for")

        mapping = client.indices.get_mapping(index=index_name).body
        properties = mapping[index_name]["mappings"]["properties"]
        assert properties["message"]["type"] == "text"
        assert properties["service"]["type"] == "keyword"

        full_text = search_logs(
            client,
            LogSearchParams(
                start=timestamp - timedelta(minutes=1),
                end=timestamp + timedelta(minutes=1),
                message="searchable integration",
                run_id=run_id,
            ),
        )
        assert full_text.total == 1

        exact_service = search_logs(
            client,
            LogSearchParams(
                start=timestamp - timedelta(minutes=1),
                end=timestamp + timedelta(minutes=1),
                services=["order-consumer"],
                run_id=run_id,
            ),
        )
        assert exact_service.total == 1

        outside_period = search_logs(
            client,
            LogSearchParams(
                start=timestamp + timedelta(minutes=1),
                end=timestamp + timedelta(minutes=2),
                run_id=run_id,
            ),
        )
        assert outside_period.total == 0

        counts = count_logs_by_event_type(
            client,
            LogAggregationParams(
                start=timestamp - timedelta(minutes=1),
                end=timestamp + timedelta(minutes=1),
                run_id=run_id,
            ),
        )
        assert [(bucket.key, bucket.count) for bucket in counts.buckets] == [
            ("integration_event", 1)
        ]

        timeline = get_log_timeline(
            client,
            LogTimelineParams(
                start=timestamp - timedelta(minutes=1),
                end=timestamp + timedelta(minutes=1),
                interval="1m",
                run_id=run_id,
            ),
        )
        assert sum(bucket.count for bucket in timeline.buckets) == 1
    finally:
        client.indices.delete(index=index_name, ignore_unavailable=True)
        if client.indices.exists_index_template(name=template_name):
            client.indices.delete_index_template(name=template_name)
        client.close()
