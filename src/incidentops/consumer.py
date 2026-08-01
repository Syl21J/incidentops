"""Command-line Kafka consumer persisting validated orders in PostgreSQL."""

import argparse
import logging
import signal
import sys
import time
from types import FrameType

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from pydantic import ValidationError

from incidentops.config import Settings, get_settings
from incidentops.database import connect_database, insert_order
from incidentops.logging import configure_logging, get_third_party_logger
from incidentops.metrics import ConsumerMetrics, MetricsServer, create_consumer_metrics
from incidentops.models import OrderEvent

SERVICE_NAME = "order-consumer"


def positive_integer(value: str) -> int:
    """Parse a positive integer for argparse."""

    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def positive_float(value: str) -> float:
    """Parse a positive floating-point value for argparse."""

    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def tcp_port(value: str) -> int:
    """Parse a valid TCP port."""

    parsed = int(value)
    if not 1 <= parsed <= 65535:
        raise argparse.ArgumentTypeError("value must be between 1 and 65535")
    return parsed


def processing_delay_ms(value: str) -> int:
    """Parse the bounded development-only processing delay."""

    parsed = int(value)
    if not 0 <= parsed <= 5_000:
        raise argparse.ArgumentTypeError("processing delay must be between 0 and 5000 ms")
    return parsed


def slow_threshold_ms(value: str) -> int:
    """Parse the bounded slow-processing logging threshold."""

    parsed = int(value)
    if not 0 <= parsed <= 60_000:
        raise argparse.ArgumentTypeError("slow threshold must be between 0 and 60000 ms")
    return parsed


def calculate_partition_lag(low_offset: int, high_offset: int, committed_offset: int) -> int:
    """Return retained messages after the committed next offset, never below zero.

    Kafka reports an invalid negative offset when a group has not committed. In that
    case the retained low watermark is the safe effective starting offset.
    """

    effective_committed = low_offset if committed_offset < low_offset else committed_offset
    return max(0, high_offset - effective_committed)


def collect_total_consumer_lag(
    consumer: Consumer,
    partitions: list[TopicPartition],
    *,
    timeout: float = 1.0,
) -> int:
    """Sum authoritative Kafka high-watermark minus committed offsets."""

    if not partitions:
        return 0
    requested = [TopicPartition(item.topic, item.partition) for item in partitions]
    committed = consumer.committed(requested, timeout=timeout)
    total = 0
    for partition in committed:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(partition.topic, partition.partition),
            timeout=timeout,
            cached=False,
        )
        total += calculate_partition_lag(low, high, partition.offset)
    return total


def log_slow_processing(
    logger: logging.Logger,
    event: OrderEvent,
    duration_ms: float,
    threshold_ms: int,
) -> None:
    """Emit scenario evidence only when the complete processing path is slow."""

    if duration_ms < threshold_ms:
        return
    logger.warning(
        "Order processing exceeded the configured slow threshold",
        extra={
            "event_type": "slow_processing",
            "event_id": str(event.event_id),
            "order_id": str(event.order_id),
            "duration_ms": round(duration_ms, 2),
        },
    )


def build_parser(settings: Settings) -> argparse.ArgumentParser:
    """Build the consumer command-line parser."""

    parser = argparse.ArgumentParser(
        description="Consume validated order events and store them in PostgreSQL."
    )
    parser.add_argument("--bootstrap-servers", default=settings.kafka_bootstrap_servers)
    parser.add_argument("--topic", default=settings.kafka_topic)
    parser.add_argument("--group", default=settings.kafka_consumer_group)
    parser.add_argument("--max-messages", type=positive_integer)
    parser.add_argument("--idle-timeout", type=positive_float)
    parser.add_argument("--run-id", default=settings.run_id)
    parser.add_argument("--log-level", default=settings.log_level)
    parser.add_argument(
        "--processing-delay-ms",
        type=processing_delay_ms,
        default=settings.consumer_processing_delay_ms,
        help="Development/test-only artificial delay inside each message processing path.",
    )
    parser.add_argument(
        "--slow-processing-threshold-ms",
        type=slow_threshold_ms,
        default=settings.slow_processing_threshold_ms,
    )
    parser.add_argument(
        "--lag-update-interval-seconds",
        type=positive_float,
        default=settings.consumer_lag_update_interval_seconds,
    )
    parser.add_argument("--metrics-host", default=settings.metrics_host)
    parser.add_argument("--metrics-port", type=tcp_port, default=settings.consumer_metrics_port)
    parser.add_argument(
        "--no-metrics",
        action="store_false",
        dest="metrics_enabled",
        default=settings.metrics_enabled,
        help="Disable the Prometheus endpoint (primarily for isolated tests).",
    )
    return parser


def commit_message(consumer: Consumer, message: Message) -> None:
    """Synchronously commit the offset immediately after handling one message."""

    consumer.commit(message=message, asynchronous=False)


def run(arguments: argparse.Namespace, settings: Settings) -> int:
    """Consume messages until a signal or optional message limit is reached."""

    logger = configure_logging(
        SERVICE_NAME,
        arguments.log_level,
        third_party_level=settings.third_party_log_level,
        file_enabled=settings.log_file_enabled,
        log_directory=settings.log_directory,
        run_id=arguments.run_id,
    )
    third_party_logger = get_third_party_logger(SERVICE_NAME)
    shutdown_requested = False
    handled_count = 0
    inserted_count = 0
    invalid_count = 0
    started_at = time.monotonic()
    last_message_at = started_at
    assigned_partitions: list[TopicPartition] = []
    next_lag_update_at = started_at
    metrics: ConsumerMetrics = create_consumer_metrics()
    metrics.kafka_consumer_lag.set(0)
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
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.warning(
            "Shutdown requested",
            extra={"event_type": "shutdown_requested", "error_type": f"signal_{signum}"},
        )

    def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
        nonlocal assigned_partitions, next_lag_update_at
        consumer.assign(partitions)
        assigned_partitions = list(partitions)
        next_lag_update_at = 0.0
        logger.info(
            "Kafka partitions assigned",
            extra={
                "event_type": "partitions_assigned",
                "topic": arguments.topic,
                "consumer_group": arguments.group,
                "count": len(partitions),
            },
        )

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "group.id": arguments.group,
            "client.id": SERVICE_NAME,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        },
        logger=third_party_logger,
    )

    try:
        connection = connect_database(settings)
    except psycopg.Error as error:
        metrics.processing_errors.inc()
        logger.error(
            "Could not connect to PostgreSQL",
            extra={
                "event_type": "database_connection_failed",
                "error_type": type(error).__name__,
            },
        )
        consumer.close()
        if metrics_server is not None:
            metrics_server.close()
        return 1

    consumer.subscribe([arguments.topic], on_assign=on_assign)
    logger.info(
        "Order consumer started",
        extra={
            "event_type": "consumer_started",
            "topic": arguments.topic,
            "consumer_group": arguments.group,
        },
    )

    exit_code = 0
    try:
        while not shutdown_requested:
            if arguments.max_messages is not None and handled_count >= arguments.max_messages:
                break

            now = time.monotonic()
            if assigned_partitions and now >= next_lag_update_at:
                try:
                    metrics.kafka_consumer_lag.set(
                        collect_total_consumer_lag(consumer, assigned_partitions)
                    )
                except (KafkaException, RuntimeError) as error:
                    logger.warning(
                        "Kafka consumer lag collection failed",
                        extra={
                            "event_type": "lag_collection_failed",
                            "error_type": type(error).__name__,
                            "topic": arguments.topic,
                            "consumer_group": arguments.group,
                        },
                    )
                next_lag_update_at = now + arguments.lag_update_interval_seconds

            message = consumer.poll(1.0)
            if message is None:
                if (
                    arguments.idle_timeout is not None
                    and time.monotonic() - last_message_at >= arguments.idle_timeout
                ):
                    logger.error(
                        "Consumer idle timeout reached",
                        extra={
                            "event_type": "consumer_timeout",
                            "topic": arguments.topic,
                            "consumer_group": arguments.group,
                        },
                    )
                    exit_code = 1
                    break
                continue

            message_error = message.error()
            if message_error is not None:
                if message_error.code() == KafkaError._PARTITION_EOF:
                    continue
                metrics.processing_errors.inc()
                logger.error(
                    "Kafka consumer returned an error",
                    extra={
                        "event_type": "consumer_error",
                        "error_type": message_error.name(),
                        "topic": arguments.topic,
                    },
                )
                continue

            last_message_at = time.monotonic()
            event_started_at = time.monotonic()
            metrics.orders_consumed.inc()
            if arguments.processing_delay_ms:
                time.sleep(arguments.processing_delay_ms / 1000)
            try:
                payload = message.value()
                if payload is None:
                    raise ValueError("Kafka message has no value")
                event = OrderEvent.from_json_bytes(payload)
            except (ValidationError, UnicodeDecodeError, ValueError) as error:
                invalid_count += 1
                handled_count += 1
                metrics.processing_errors.inc()
                metrics.processing_duration.observe(time.monotonic() - event_started_at)
                logger.error(
                    "Invalid order event was skipped",
                    extra={
                        "event_type": "invalid_event_skipped",
                        "error_type": type(error).__name__,
                        "topic": arguments.topic,
                    },
                )
                # An invalid event is intentionally handled by skipping it in this phase.
                commit_message(consumer, message)
                continue

            database_started_at = time.monotonic()
            try:
                inserted = insert_order(connection, event)
            except psycopg.Error as error:
                metrics.database_duration.observe(time.monotonic() - database_started_at)
                metrics.processing_duration.observe(time.monotonic() - event_started_at)
                metrics.processing_errors.inc()
                logger.error(
                    "Could not persist the order event",
                    extra={
                        "event_type": "database_write_failed",
                        "event_id": str(event.event_id),
                        "order_id": str(event.order_id),
                        "error_type": type(error).__name__,
                    },
                )
                exit_code = 1
                break
            metrics.database_duration.observe(time.monotonic() - database_started_at)

            # The database transaction is committed before this offset commit.
            commit_message(consumer, message)
            handled_count += 1
            inserted_count += int(inserted)
            metrics.orders_processed.inc()
            duration_seconds = time.monotonic() - event_started_at
            metrics.processing_duration.observe(duration_seconds)
            duration_ms = duration_seconds * 1000

            logger.info(
                "Order event processed",
                extra={
                    "event_type": "order_processed" if inserted else "order_duplicate",
                    "event_id": str(event.event_id),
                    "order_id": str(event.order_id),
                    "inserted": inserted,
                    "duration_ms": round(duration_ms, 2),
                },
            )
            log_slow_processing(
                logger,
                event,
                duration_ms,
                arguments.slow_processing_threshold_ms,
            )
    except KafkaException as error:
        metrics.processing_errors.inc()
        logger.error(
            "Kafka consumer failed",
            extra={
                "event_type": "consumer_error",
                "error_type": type(error).__name__,
                "topic": arguments.topic,
            },
        )
        exit_code = 1
    finally:
        consumer.close()
        connection.close()
        if metrics_server is not None:
            metrics_server.close()

    logger.info(
        "Order consumer stopped",
        extra={
            "event_type": "consumer_summary",
            "topic": arguments.topic,
            "consumer_group": arguments.group,
            "count": handled_count,
            "inserted_count": inserted_count,
            "failed": invalid_count,
            "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
        },
    )
    return exit_code


def main() -> None:
    """Run the order consumer CLI."""

    settings = get_settings()
    arguments = build_parser(settings).parse_args()
    sys.exit(run(arguments, settings))


if __name__ == "__main__":
    main()
