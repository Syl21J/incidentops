"""Structured JSON logging for stdout and optional JSON Lines files."""

import json
import logging
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
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
    "inserted_count",
)


class JsonFormatter(logging.Formatter):
    """Render one compact JSON object per log record."""

    def __init__(self, service: str, run_id: str | None) -> None:
        super().__init__()
        self.service = service
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "@timestamp": datetime.now(UTC)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self.service,
            "event_type": getattr(record, "event_type", "application_log"),
            "message": record.getMessage(),
            "logger": record.name,
        }
        if self.run_id is not None:
            payload["run_id"] = self.run_id

        for field in OPTIONAL_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        if record.exc_info and "error_type" not in payload:
            exception_type = record.exc_info[0]
            if exception_type is not None:
                payload["error_type"] = exception_type.__name__

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, separators=(",", ":"))


def _close_handlers(loggers: Iterable[logging.Logger]) -> None:
    """Detach and close handlers left by an earlier configuration."""

    handlers: set[logging.Handler] = set()
    for logger in loggers:
        handlers.update(logger.handlers)
        logger.handlers.clear()
    for handler in handlers:
        handler.close()


def get_third_party_logger(service: str) -> logging.Logger:
    """Return the logger reserved for a service's third-party libraries."""

    return logging.getLogger(f"{service}.third_party")


def configure_logging(
    service: str,
    level: str,
    *,
    third_party_level: str = "WARNING",
    file_enabled: bool = False,
    log_directory: Path = Path("logs"),
    run_id: str | None = None,
) -> logging.Logger:
    """Configure one service logger for stdout and an optional JSONL file."""

    logger = logging.getLogger(service)
    third_party_logger = get_third_party_logger(service)
    _close_handlers((logger, third_party_logger))

    formatter = JsonFormatter(service, run_id)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    handlers: list[logging.Handler] = [stdout_handler]

    file_error: OSError | None = None
    if file_enabled:
        try:
            log_directory.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(
                log_directory / f"{service}.jsonl",
                mode="a",
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except OSError as error:
            file_error = error

    logger.setLevel(level)
    logger.propagate = False
    third_party_logger.setLevel(third_party_level)
    third_party_logger.propagate = False
    for handler in handlers:
        logger.addHandler(handler)
        third_party_logger.addHandler(handler)

    if file_error is not None:
        logger.warning(
            "JSONL file logging could not be enabled",
            extra={
                "event_type": "log_file_unavailable",
                "error_type": type(file_error).__name__,
            },
        )

    return logger
