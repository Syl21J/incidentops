"""Command-line producer for deterministic order events."""

import argparse
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass
from threading import Event
from types import FrameType

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from pydantic import ValidationError

from incidentops.config import Settings, get_settings
from incidentops.logging import configure_logging, get_third_party_logger
from incidentops.models import generate_order_event

SERVICE_NAME = "order-producer"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass
class DeliverySummary:
    """Track asynchronous Kafka delivery outcomes."""

    sent: int = 0
    failed: int = 0


def non_negative_integer(value: str) -> int:
    """Parse a non-negative integer for argparse."""

    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
    return parsed


def positive_float(value: str) -> float:
    """Parse a positive floating-point value for argparse."""

    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def valid_run_id(value: str) -> str:
    """Restrict run identifiers to log-safe and SQL-test-safe characters."""

    if not RUN_ID_PATTERN.fullmatch(value):
        raise argparse.ArgumentTypeError(
            "run ID must contain only letters, numbers, dots, underscores, or hyphens"
        )
    return value


def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    logger: logging.Logger,
    third_party_logger: logging.Logger,
) -> None:
    """Create the local single-partition topic when it does not exist."""

    admin = AdminClient({"bootstrap.servers": bootstrap_servers}, logger=third_party_logger)
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        operation_timeout=10,
    )

    try:
        futures[topic].result(timeout=15)
        logger.info(
            "Kafka topic created",
            extra={"event_type": "topic_created", "topic": topic},
        )
    except KafkaException as error:
        kafka_error = error.args[0] if error.args else None
        if (
            isinstance(kafka_error, KafkaError)
            and kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS
        ):
            logger.info(
                "Kafka topic already exists",
                extra={"event_type": "topic_exists", "topic": topic},
            )
            return
        raise


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    """Build the producer command-line parser."""

    parser = argparse.ArgumentParser(description="Produce validated order events to Kafka.")
    parser.add_argument("--count", type=non_negative_integer, default=1)
    parser.add_argument("--rate", type=positive_float, default=1.0)
    parser.add_argument("--seed", type=int, default=settings.order_random_seed)
    parser.add_argument("--run-id", type=valid_run_id, default=settings.run_id)
    parser.add_argument("--schema-version", default="1.0")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--log-level", default=settings.log_level)
    return parser


def run(arguments: argparse.Namespace, settings: Settings) -> int:
    """Produce the requested batch and return a process exit code."""

    logger = configure_logging(
        SERVICE_NAME,
        arguments.log_level,
        third_party_level=settings.third_party_log_level,
        file_enabled=settings.log_file_enabled,
        log_directory=settings.log_directory,
        run_id=arguments.run_id,
    )
    third_party_logger = get_third_party_logger(SERVICE_NAME)
    shutdown_requested = Event()
    summary = DeliverySummary()
    started_at = time.monotonic()

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        shutdown_requested.set()
        logger.warning(
            "Shutdown requested",
            extra={"event_type": "shutdown_requested", "error_type": f"signal_{signum}"},
        )

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    try:
        ensure_topic(
            arguments.bootstrap_servers,
            arguments.topic,
            logger,
            third_party_logger,
        )
    except KafkaException as error:
        logger.error(
            "Could not create or inspect the Kafka topic",
            extra={
                "event_type": "topic_error",
                "topic": arguments.topic,
                "error_type": type(error).__name__,
            },
        )
        return 1

    producer = Producer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "client.id": SERVICE_NAME,
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 15_000,
        },
        logger=third_party_logger,
    )

    def delivery_report(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            summary.failed += 1
            logger.error(
                "Order event delivery failed",
                extra={
                    "event_type": "delivery_failed",
                    "topic": message.topic(),
                    "error_type": error.name(),
                },
            )
            return
        summary.sent += 1

    interval_seconds = 1 / arguments.rate
    next_delivery_at = time.monotonic()

    try:
        for index in range(arguments.count):
            if shutdown_requested.is_set():
                break

            wait_seconds = next_delivery_at - time.monotonic()
            if wait_seconds > 0 and shutdown_requested.wait(wait_seconds):
                break

            try:
                event = generate_order_event(
                    index=index,
                    seed=arguments.seed,
                    run_id=arguments.run_id,
                    schema_version=arguments.schema_version,
                )
            except (ValidationError, ValueError) as error:
                summary.failed += 1
                logger.error(
                    "Generated order event failed validation",
                    extra={
                        "event_type": "validation_failed",
                        "error_type": type(error).__name__,
                    },
                )
                continue

            while not shutdown_requested.is_set():
                try:
                    producer.produce(
                        topic=arguments.topic,
                        key=str(event.event_id).encode("utf-8"),
                        value=event.to_json_bytes(),
                        on_delivery=delivery_report,
                    )
                    break
                except BufferError:
                    producer.poll(0.5)

            producer.poll(0)
            next_delivery_at += interval_seconds
    except KafkaException as error:
        summary.failed += 1
        logger.error(
            "Kafka producer failed",
            extra={
                "event_type": "producer_error",
                "error_type": type(error).__name__,
                "topic": arguments.topic,
            },
        )
    finally:
        undelivered = producer.flush(15)
        summary.failed += undelivered

    duration_ms = round((time.monotonic() - started_at) * 1000, 2)
    logger.info(
        "Order production completed",
        extra={
            "event_type": "production_summary",
            "topic": arguments.topic,
            "count": summary.sent,
            "failed": summary.failed,
            "duration_ms": duration_ms,
        },
    )
    return 0 if summary.failed == 0 else 1


def main() -> None:
    """Run the order producer CLI."""

    settings = get_settings()
    arguments = build_parser(settings).parse_args()
    sys.exit(run(arguments, settings))


if __name__ == "__main__":
    main()
