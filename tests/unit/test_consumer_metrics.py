"""Unit coverage for lag math and development-only delay validation."""

import argparse
from typing import cast

import pytest
from confluent_kafka import Consumer, TopicPartition

from incidentops.consumer import (
    calculate_partition_lag,
    collect_total_consumer_lag,
    processing_delay_ms,
)


class FakeConsumer:
    """Minimal authoritative-offset fake used without a live broker."""

    def committed(
        self,
        partitions: list[TopicPartition],
        timeout: float,
    ) -> list[TopicPartition]:
        assert timeout == 1.0
        return [
            TopicPartition(partitions[0].topic, partitions[0].partition, 40),
            TopicPartition(partitions[1].topic, partitions[1].partition, -1001),
        ]

    def get_watermark_offsets(
        self,
        partition: TopicPartition,
        timeout: float,
        cached: bool,
    ) -> tuple[int, int]:
        assert timeout == 1.0
        assert cached is False
        return (10, 100) if partition.partition == 0 else (20, 50)


@pytest.mark.parametrize(
    ("low", "high", "committed", "expected"),
    [(10, 100, 40, 60), (10, 100, -1001, 90), (10, 100, 120, 0)],
)
def test_partition_lag_is_bounded_and_handles_missing_commit(
    low: int,
    high: int,
    committed: int,
    expected: int,
) -> None:
    assert calculate_partition_lag(low, high, committed) == expected


def test_total_consumer_lag_sums_authoritative_partition_offsets() -> None:
    partitions = [TopicPartition("orders", 0), TopicPartition("orders", 1)]

    lag = collect_total_consumer_lag(cast(Consumer, FakeConsumer()), partitions)

    assert lag == 90


def test_processing_delay_validation_is_bounded() -> None:
    assert processing_delay_ms("0") == 0
    assert processing_delay_ms("5000") == 5000
    with pytest.raises(argparse.ArgumentTypeError):
        processing_delay_ms("-1")
    with pytest.raises(argparse.ArgumentTypeError):
        processing_delay_ms("5001")
