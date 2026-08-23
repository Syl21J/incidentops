"""Command-line producer for deterministic order events."""

import argparse
import json
import logging
import re
import signal
import sys
import time
from dataclasses import dataclass
from functools import partial
from threading import Event
from types import FrameType

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from pydantic import ValidationError

from incidentops.config import Settings, get_settings
from incidentops.logging import configure_logging, get_third_party_logger
from incidentops.metrics import MetricsServer, create_producer_metrics
from incidentops.models import generate_order_event

SERVICE_NAME = "order-producer"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass
class DeliverySummary:
    """Track asynchronous Kafka delivery outcomes."""

    sent: int = 0
    failed: int = 0
    malformed_sent: int = 0


@dataclass(frozen=True, slots=True)
class ProductionPhase:
    """One observed production-rate phase executed by the same process."""

    name: str
    count: int
    rate: float


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


def non_negative_float(value: str) -> float:
    """Parse a non-negative bounded grace duration."""

    parsed = float(value)
    if not 0 <= parsed <= 15:
        raise argparse.ArgumentTypeError("value must be between zero and 15")
    return parsed


def positive_integer(value: str) -> int:
    """Parse an integer in the TCP port range."""

    parsed = int(value)
    if not 1 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("value must be between 1 and 65535")
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
    parser.add_argument(
        "--malformed-count",
        type=non_negative_integer,
        default=0,
        help="Scenario-only count of deterministic invalid payloads mixed into the batch.",
    )
    parser.add_argument("--baseline-count", type=non_negative_integer)
    parser.add_argument("--baseline-rate", type=positive_float)
    parser.add_argument("--burst-count", type=non_negative_integer)
    parser.add_argument("--burst-rate", type=positive_float)
    parser.add_argument("--seed", type=int, default=settings.order_random_seed)
    parser.add_argument("--run-id", type=valid_run_id, default=settings.run_id)
    parser.add_argument("--schema-version", default="1.0")
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--log-level", default=settings.log_level)
    parser.add_argument("--metrics-host", default=settings.metrics_host)
    parser.add_argument(
        "--metrics-port", type=positive_integer, default=settings.producer_metrics_port
    )
    parser.add_argument(
        "--no-metrics",
        action="store_false",
        dest="metrics_enabled",
        default=settings.metrics_enabled,
        help="Disable the Prometheus endpoint (primarily for isolated tests).",
    )
    parser.add_argument(
        "--metrics-grace-seconds",
        type=non_negative_float,
        default=0.0,
        help="Keep the metrics endpoint alive briefly for a final bounded scrape.",
    )
    return parser


def production_phases(arguments: argparse.Namespace) -> list[ProductionPhase]:
    """Validate and return either one normal phase or a baseline/burst schedule."""

    scheduled_values = (
        getattr(arguments, "baseline_count", None),
        getattr(arguments, "baseline_rate", None),
        getattr(arguments, "burst_count", None),
        getattr(arguments, "burst_rate", None),
    )
    if any(value is not None for value in scheduled_values):
        if any(value is None for value in scheduled_values):
            raise ValueError("baseline and burst count/rate values must be supplied together")
        baseline_count, baseline_rate, burst_count, burst_rate = scheduled_values
        if not isinstance(baseline_count, int) or not 1 <= baseline_count <= 300:
            raise ValueError("baseline count must be between one and 300")
        if not isinstance(burst_count, int) or not 1 <= burst_count <= 1_000:
            raise ValueError("burst count must be between one and 1000")
        if not isinstance(baseline_rate, (int, float)) or not isinstance(burst_rate, (int, float)):
            raise ValueError("baseline and burst rates must be numbers")
        if not 0 < baseline_rate <= 100:
            raise ValueError("baseline rate must be greater than zero and at most 100")
        if not 0 < burst_rate <= 500:
            raise ValueError("burst rate must be greater than zero and at most 500")
        if burst_rate < baseline_rate * 2:
            raise ValueError("burst rate must be at least twice baseline rate")
        if getattr(arguments, "malformed_count", 0):
            raise ValueError("malformed payload injection cannot be combined with a rate schedule")
        return [
            ProductionPhase("baseline", baseline_count, float(baseline_rate)),
            ProductionPhase("burst", burst_count, float(burst_rate)),
        ]

    malformed_count = getattr(arguments, "malformed_count", 0)
    if malformed_count > 500:
        raise ValueError("malformed count must not exceed 500")
    if malformed_count and arguments.count + malformed_count > 750:
        raise ValueError("combined valid and malformed count must not exceed 750")
    return [ProductionPhase("steady", arguments.count + malformed_count, arguments.rate)]


def is_malformed_delivery(index: int, total: int, malformed_count: int) -> bool:
    """Spread a fixed invalid count deterministically across one mixed batch."""

    if not 0 <= malformed_count <= total:
        raise ValueError("malformed count must be between zero and total deliveries")
    return ((index + 1) * malformed_count // total) > (index * malformed_count // total)


def malformed_order_payload(index: int, run_id: str) -> bytes:
    """Return a safe deterministic payload missing required OrderEvent fields."""

    return json.dumps(
        {
            "schema_version": "1.0",
            "event_id": f"malformed-{index:06d}",
            "run_id": run_id,
            "invalid_reason": "missing_required_order_fields",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


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
    metrics = create_producer_metrics()
    phases = production_phases(arguments)
    metrics.target_rate.set(phases[0].rate)
    metrics_server: MetricsServer | None = None

    if arguments.metrics_enabled:
        try:
            metrics_server = MetricsServer.start(
                host=arguments.metrics_host,
                port=arguments.metrics_port,
                registry=metrics.registry,
            )
        except OSError as error:
            logger.error(
                "Could not start the Prometheus metrics endpoint",
                extra={
                    "event_type": "metrics_start_failed",
                    "error_type": type(error).__name__,
                },
            )
            return 1

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
        metrics.production_errors.inc()
        logger.error(
            "Could not create or inspect the Kafka topic",
            extra={
                "event_type": "topic_error",
                "topic": arguments.topic,
                "error_type": type(error).__name__,
            },
        )
        if metrics_server is not None:
            metrics_server.close()
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

    def delivery_report(
        error: KafkaError | None,
        message: Message,
        delivery_started_at: float,
        malformed: bool,
    ) -> None:
        if error is not None:
            summary.failed += 1
            metrics.production_errors.inc()
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
        summary.malformed_sent += int(malformed)
        metrics.orders_produced.inc()
        metrics.production_duration.observe(time.monotonic() - delivery_started_at)
        if malformed:
            logger.warning(
                "Deterministic malformed scenario payload delivered",
                extra={
                    "event_type": "malformed_event_published",
                    "topic": message.topic(),
                },
            )

    total_deliveries = sum(phase.count for phase in phases)
    malformed_count = getattr(arguments, "malformed_count", 0)
    delivery_index = 0
    valid_index = 0

    try:
        for phase in phases:
            metrics.target_rate.set(phase.rate)
            logger.info(
                "Order production phase started",
                extra={
                    "event_type": "production_phase_started",
                    "phase": phase.name,
                    "count": phase.count,
                    "rate": phase.rate,
                },
            )
            interval_seconds = 1 / phase.rate
            next_delivery_at = time.monotonic()
            for _phase_index in range(phase.count):
                if shutdown_requested.is_set():
                    break

                wait_seconds = next_delivery_at - time.monotonic()
                if wait_seconds > 0 and shutdown_requested.wait(wait_seconds):
                    break

                malformed = is_malformed_delivery(
                    delivery_index,
                    total_deliveries,
                    malformed_count,
                )
                if malformed:
                    key = f"malformed-{delivery_index:06d}".encode()
                    payload = malformed_order_payload(delivery_index, arguments.run_id)
                else:
                    try:
                        event = generate_order_event(
                            index=valid_index,
                            seed=arguments.seed,
                            run_id=arguments.run_id,
                            schema_version=arguments.schema_version,
                        )
                    except (ValidationError, ValueError) as error:
                        summary.failed += 1
                        metrics.production_errors.inc()
                        logger.error(
                            "Generated order event failed validation",
                            extra={
                                "event_type": "validation_failed",
                                "error_type": type(error).__name__,
                            },
                        )
                        delivery_index += 1
                        continue
                    key = str(event.event_id).encode("utf-8")
                    payload = event.to_json_bytes()
                    valid_index += 1

                while not shutdown_requested.is_set():
                    try:
                        delivery_started_at = time.monotonic()
                        producer.produce(
                            topic=arguments.topic,
                            key=key,
                            value=payload,
                            on_delivery=partial(
                                delivery_report,
                                delivery_started_at=delivery_started_at,
                                malformed=malformed,
                            ),
                        )
                        break
                    except BufferError:
                        producer.poll(0.5)
                    except KafkaException as error:
                        summary.failed += 1
                        metrics.production_errors.inc()
                        logger.error(
                            "Order event production failed",
                            extra={
                                "event_type": "producer_error",
                                "error_type": type(error).__name__,
                                "topic": arguments.topic,
                            },
                        )
                        break

                producer.poll(0)
                delivery_index += 1
                next_delivery_at += interval_seconds
            if shutdown_requested.is_set():
                break
    except KafkaException as error:
        summary.failed += 1
        metrics.production_errors.inc()
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
        if undelivered:
            metrics.production_errors.inc(undelivered)

    duration_ms = round((time.monotonic() - started_at) * 1000, 2)
    logger.info(
        "Order production completed",
        extra={
            "event_type": "production_summary",
            "topic": arguments.topic,
            "count": summary.sent,
            "failed": summary.failed,
            "malformed_count": summary.malformed_sent,
            "duration_ms": duration_ms,
        },
    )
    grace_seconds = getattr(arguments, "metrics_grace_seconds", 0.0)
    if grace_seconds and not shutdown_requested.is_set():
        shutdown_requested.wait(grace_seconds)
    if metrics_server is not None:
        metrics_server.close()
    return 0 if summary.failed == 0 else 1


def main() -> None:
    """Run the order producer CLI."""

    settings = get_settings()
    arguments = build_parser(settings).parse_args()
    sys.exit(run(arguments, settings))


if __name__ == "__main__":
    main()
