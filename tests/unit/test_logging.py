"""Unit tests for structured application logs."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from incidentops.logging import configure_logging, get_third_party_logger


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

    assert datetime.fromisoformat(payload["@timestamp"].replace("Z", "+00:00")).tzinfo
    assert payload["level"] == "INFO"
    assert payload["service"] == "order-producer"
    assert payload["event_type"] == "order_delivered"
    assert payload["message"] == "Order delivered"
    assert payload["logger"] == "order-producer"
    assert payload["event_id"] == "event-1"
    assert payload["duration_ms"] == 1.5


def test_file_logging_writes_one_valid_json_object_per_line(tmp_path: Path) -> None:
    logger = configure_logging(
        "order-consumer",
        "INFO",
        file_enabled=True,
        log_directory=tmp_path,
        run_id="unit-run",
    )

    logger.info(
        "Order event processed",
        extra={
            "event_type": "order_processed",
            "event_id": "event-2",
            "order_id": "order-2",
            "duration_ms": 2.25,
        },
    )

    lines = (tmp_path / "order-consumer.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["run_id"] == "unit-run"
    assert payload["event_id"] == "event-2"
    assert payload["order_id"] == "order-2"
    assert isinstance(payload["duration_ms"], float)


def test_file_logging_can_be_disabled(tmp_path: Path) -> None:
    logger = configure_logging(
        "order-producer",
        "INFO",
        file_enabled=False,
        log_directory=tmp_path,
    )

    logger.info("Stdout only", extra={"event_type": "stdout_only"})

    assert list(tmp_path.iterdir()) == []


def test_third_party_log_level_is_configured_separately(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(
        "order-producer",
        "DEBUG",
        third_party_level="ERROR",
    )
    third_party_logger = get_third_party_logger("order-producer")

    third_party_logger.warning("Hidden third-party warning")
    third_party_logger.error("Visible third-party error")

    payload = json.loads(capsys.readouterr().out)
    assert payload["level"] == "ERROR"
    assert payload["logger"] == "order-producer.third_party"
