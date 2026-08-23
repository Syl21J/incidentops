"""Unit tests for idempotent PostgreSQL insertion."""

from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from psycopg import Connection

from incidentops.database import DATABASE_DELAY_SQL, INSERT_ORDER_SQL, insert_order
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


def test_insert_order_applies_only_the_fixed_bounded_database_delay() -> None:
    event = generate_order_event(index=0, seed=1, run_id="database-delay-test")
    connection_mock = MagicMock()
    cursor_mock = connection_mock.cursor.return_value.__enter__.return_value
    cursor_mock.fetchone.return_value = (event.event_id,)
    connection = cast(Connection[tuple[Any, ...]], connection_mock)

    assert insert_order(connection, event, artificial_delay_ms=800) is True
    assert cursor_mock.execute.call_args_list[0].args == (DATABASE_DELAY_SQL, (0.8,))
    assert cursor_mock.execute.call_args_list[1].args[0] == INSERT_ORDER_SQL


@pytest.mark.parametrize("delay_ms", [-1, 5_001])
def test_insert_order_rejects_unbounded_database_delay(delay_ms: int) -> None:
    event = generate_order_event(index=0, seed=1, run_id="database-delay-bounds")
    connection = cast(Connection[tuple[Any, ...]], MagicMock())

    with pytest.raises(ValueError, match="between 0 and 5000"):
        insert_order(connection, event, artificial_delay_ms=delay_ms)
