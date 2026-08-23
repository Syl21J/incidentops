"""PostgreSQL access for processed order events."""

from typing import Any

import psycopg
from psycopg import Connection

from incidentops.config import Settings
from incidentops.models import OrderEvent

INSERT_ORDER_SQL = """
INSERT INTO processed_orders (
    event_id,
    order_id,
    customer_id,
    amount,
    currency,
    created_at,
    schema_version
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id
"""
DATABASE_DELAY_SQL = "SELECT pg_sleep(%s)"


def connect_database(settings: Settings) -> Connection[tuple[Any, ...]]:
    """Open a PostgreSQL connection from application settings."""

    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        dbname=settings.postgres_db,
        connect_timeout=10,
    )


def insert_order(
    connection: Connection[tuple[Any, ...]],
    event: OrderEvent,
    *,
    artificial_delay_ms: int = 0,
) -> bool:
    """Insert an order once with an optional bounded scenario-only database delay."""

    if not 0 <= artificial_delay_ms <= 5_000:
        raise ValueError("artificial database delay must be between 0 and 5000 ms")

    parameters = (
        event.event_id,
        event.order_id,
        event.customer_id,
        event.amount,
        event.currency,
        event.created_at,
        event.schema_version,
    )

    with connection.transaction():
        with connection.cursor() as cursor:
            if artificial_delay_ms:
                cursor.execute(DATABASE_DELAY_SQL, (artificial_delay_ms / 1000,))
            cursor.execute(INSERT_ORDER_SQL, parameters)
            return cursor.fetchone() is not None
