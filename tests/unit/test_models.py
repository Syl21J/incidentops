"""Unit tests for validated order events."""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from incidentops.models import OrderEvent, generate_order_event


def make_event(**overrides: object) -> OrderEvent:
    """Build a valid event with optional field overrides."""

    values: dict[str, object] = {
        "event_id": uuid4(),
        "order_id": uuid4(),
        "customer_id": "customer-1",
        "amount": Decimal("12.50"),
        "currency": "EUR",
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
        "schema_version": "1.0",
    }
    values.update(overrides)
    return OrderEvent.model_validate(values)


def test_valid_event_normalizes_timestamp_to_utc() -> None:
    source_time = datetime(2026, 1, 1, 1, tzinfo=timezone(timedelta(hours=2)))

    event = make_event(created_at=source_time)

    assert event.created_at.tzinfo is UTC


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_event(created_at=datetime(2026, 1, 1))


@pytest.mark.parametrize("amount", [Decimal("0"), Decimal("-0.01")])
def test_non_positive_amount_is_rejected(amount: Decimal) -> None:
    with pytest.raises(ValidationError):
        make_event(amount=amount)


def test_json_round_trip_uses_utf8_bytes() -> None:
    event = make_event()

    payload = event.to_json_bytes()
    restored = OrderEvent.from_json_bytes(payload)

    assert isinstance(payload, bytes)
    assert restored == event


def test_deterministic_generation() -> None:
    first = generate_order_event(index=4, seed=123, run_id="unit-test")
    second = generate_order_event(index=4, seed=123, run_id="unit-test")

    assert first == second
