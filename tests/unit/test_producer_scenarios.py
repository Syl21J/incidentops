"""Unit coverage for bounded traffic and malformed-event producer modes."""

from argparse import Namespace

import pytest

from incidentops.producer import (
    is_malformed_delivery,
    malformed_order_payload,
    production_phases,
)


def test_traffic_schedule_requires_and_preserves_baseline_then_burst() -> None:
    phases = production_phases(
        Namespace(
            count=0,
            rate=1.0,
            malformed_count=0,
            baseline_count=30,
            baseline_rate=3.0,
            burst_count=300,
            burst_rate=100.0,
        )
    )

    assert [(item.name, item.count, item.rate) for item in phases] == [
        ("baseline", 30, 3.0),
        ("burst", 300, 100.0),
    ]


def test_traffic_schedule_rejects_partial_or_small_rate_change() -> None:
    partial = Namespace(
        count=0,
        rate=1.0,
        malformed_count=0,
        baseline_count=30,
        baseline_rate=3.0,
        burst_count=None,
        burst_rate=None,
    )
    with pytest.raises(ValueError, match="supplied together"):
        production_phases(partial)

    partial.burst_count = 100
    partial.burst_rate = 5.0
    with pytest.raises(ValueError, match="at least twice"):
        production_phases(partial)

    partial.burst_rate = 501.0
    with pytest.raises(ValueError, match="at most 500"):
        production_phases(partial)


def test_malformed_messages_are_exactly_and_deterministically_interleaved() -> None:
    first = [is_malformed_delivery(index, 60, 20) for index in range(60)]
    second = [is_malformed_delivery(index, 60, 20) for index in range(60)]

    assert first == second
    assert sum(first) == 20
    assert any(first)
    assert not all(first)


def test_malformed_payload_is_json_but_still_fails_normal_order_validation() -> None:
    from incidentops.models import OrderEvent

    payload = malformed_order_payload(3, "malformed-events-test")

    with pytest.raises(ValueError):
        OrderEvent.from_json_bytes(payload)


def test_malformed_count_is_bounded_and_not_combined_with_traffic_schedule() -> None:
    with pytest.raises(ValueError, match="between zero and total"):
        is_malformed_delivery(0, 2, 3)

    arguments = Namespace(
        count=0,
        rate=1.0,
        malformed_count=1,
        baseline_count=10,
        baseline_rate=2.0,
        burst_count=20,
        burst_rate=20.0,
    )
    with pytest.raises(ValueError, match="cannot be combined"):
        production_phases(arguments)

    arguments = Namespace(
        count=500,
        rate=1.0,
        malformed_count=300,
        baseline_count=None,
        baseline_rate=None,
        burst_count=None,
        burst_rate=None,
    )
    with pytest.raises(ValueError, match="must not exceed 750"):
        production_phases(arguments)
