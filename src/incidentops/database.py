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
) -> bool:
    """Insert an order once and return whether a new row was created."""

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
            cursor.execute(INSERT_ORDER_SQL, parameters)
            return cursor.fetchone() is not None
