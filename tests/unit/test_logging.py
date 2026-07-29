"""Unit tests for structured application logs."""

import json

import pytest

from incidentops.logging import configure_logging


def test_json_log_contains_service_and_context(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logger = configure_logging("order-producer", "INFO")

    logger.info(
        "Order delivered",
        extra={
            "event_type": "order_delivered",
            "event_id": "event-1",
            "duration_ms": 1.5,
        },
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["level"] == "INFO"
    assert payload["service"] == "order-producer"
    assert payload["event_type"] == "order_delivered"
    assert payload["event_id"] == "event-1"
    assert payload["duration_ms"] == 1.5
