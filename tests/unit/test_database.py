"""Unit tests for idempotent PostgreSQL insertion."""

from typing import Any, cast
from unittest.mock import MagicMock

from psycopg import Connection

from incidentops.database import INSERT_ORDER_SQL, insert_order
from incidentops.models import generate_order_event


def test_insert_order_reports_insert_then_duplicate() -> None:
    event = generate_order_event(index=0, seed=1, run_id="database-test")
    connection_mock = MagicMock()
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    cursor_mock.fetchone.side_effect = [(event.event_id,), None]
    connection = cast(Connection[tuple[Any, ...]], connection_mock)

    assert insert_order(connection, event) is True
    assert insert_order(connection, event) is False
    assert cursor_mock.execute.call_count == 2
    assert "ON CONFLICT (event_id) DO NOTHING" in INSERT_ORDER_SQL
