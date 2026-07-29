"""Command-line Kafka consumer persisting validated orders in PostgreSQL."""

import argparse
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

    def request_shutdown(signum: int, _frame: FrameType | None) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True
        logger.warning(
            "Shutdown requested",
            extra={"event_type": "shutdown_requested", "error_type": f"signal_{signum}"},
        )

    def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
        consumer.assign(partitions)
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
        logger.error(
            "Could not connect to PostgreSQL",
            extra={
                "event_type": "database_connection_failed",
                "error_type": type(error).__name__,
            },
        )
        consumer.close()
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
            try:
                payload = message.value()
                if payload is None:
                    raise ValueError("Kafka message has no value")
                event = OrderEvent.from_json_bytes(payload)
            except (ValidationError, UnicodeDecodeError, ValueError) as error:
                invalid_count += 1
                handled_count += 1
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

            event_started_at = time.monotonic()
            try:
                inserted = insert_order(connection, event)
            except psycopg.Error as error:
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

            # The database transaction is committed before this offset commit.
            commit_message(consumer, message)
            handled_count += 1
            inserted_count += int(inserted)

            logger.info(
                "Order event processed",
                extra={
                    "event_type": "order_processed" if inserted else "order_duplicate",
                    "event_id": str(event.event_id),
                    "order_id": str(event.order_id),
                    "inserted": inserted,
                    "duration_ms": round((time.monotonic() - event_started_at) * 1000, 2),
                },
            )
    except KafkaException as error:
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
