"""Structured JSON logging for future Elasticsearch ingestion."""

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Final

OPTIONAL_FIELDS: Final = (
    "event_id",
    "order_id",
    "duration_ms",
    "error_type",
    "topic",
    "consumer_group",
    "count",
    "failed",
    "inserted",
)


class JsonFormatter(logging.Formatter):
    """Render one compact JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": getattr(record, "service", record.name),
            "event_type": getattr(record, "event_type", "application_log"),
            "message": record.getMessage(),
        }

        for field in OPTIONAL_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        if record.exc_info and "error_type" not in payload:
            exception_type = record.exc_info[0]
            if exception_type is not None:
                payload["error_type"] = exception_type.__name__

        return json.dumps(payload, default=str, separators=(",", ":"))


def configure_logging(service: str, level: str) -> logging.Logger:
    """Configure and return one service logger writing JSON to stdout."""

    logger = logging.getLogger(service)
    logger.handlers.clear()
    logger.setLevel(level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    return logger
