"""Prometheus metric registries and HTTP endpoint lifecycle helpers."""

from __future__ import annotations

from dataclasses import dataclass
from threading import Lock, Thread
from typing import Any
from weakref import WeakKeyDictionary

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

PROCESSING_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
DATABASE_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)


@dataclass(frozen=True)
class ProducerMetrics:
    """All bounded-cardinality metrics emitted by the order producer."""

    registry: CollectorRegistry
    orders_produced: Counter
    production_errors: Counter
    production_duration: Histogram
    target_rate: Gauge


@dataclass(frozen=True)
class ConsumerMetrics:
    """All bounded-cardinality metrics emitted by the order consumer."""

    registry: CollectorRegistry
    orders_consumed: Counter
    orders_processed: Counter
    processing_errors: Counter
    processing_duration: Histogram
    database_duration: Histogram
    kafka_consumer_lag: Gauge


_producer_cache: WeakKeyDictionary[CollectorRegistry, ProducerMetrics] = WeakKeyDictionary()
_consumer_cache: WeakKeyDictionary[CollectorRegistry, ConsumerMetrics] = WeakKeyDictionary()
_cache_lock = Lock()


def create_producer_metrics(registry: CollectorRegistry | None = None) -> ProducerMetrics:
    """Create or reuse producer collectors in an isolated registry."""

    registry = registry or CollectorRegistry()
    with _cache_lock:
        cached = _producer_cache.get(registry)
        if cached is not None:
            return cached
        metrics = ProducerMetrics(
            registry=registry,
            orders_produced=Counter(
                "incidentops_orders_produced_total",
                "Order events whose delivery Kafka confirmed.",
                registry=registry,
            ),
            production_errors=Counter(
                "incidentops_order_production_errors_total",
                "Order events that could not be produced or delivered.",
                registry=registry,
            ),
            production_duration=Histogram(
                "incidentops_order_production_duration_seconds",
                "Seconds from a produce request until Kafka delivery confirmation.",
                buckets=PROCESSING_BUCKETS,
                registry=registry,
            ),
            target_rate=Gauge(
                "incidentops_producer_target_rate",
                "Configured producer target rate in order events per second.",
                registry=registry,
            ),
        )
        _producer_cache[registry] = metrics
        return metrics


def create_consumer_metrics(registry: CollectorRegistry | None = None) -> ConsumerMetrics:
    """Create or reuse consumer collectors in an isolated registry."""

    registry = registry or CollectorRegistry()
    with _cache_lock:
        cached = _consumer_cache.get(registry)
        if cached is not None:
            return cached
        metrics = ConsumerMetrics(
            registry=registry,
            orders_consumed=Counter(
                "incidentops_orders_consumed_total",
                "Kafka messages received by the order consumer.",
                registry=registry,
            ),
            orders_processed=Counter(
                "incidentops_orders_processed_total",
                "Valid order events completed after a successful database transaction.",
                registry=registry,
            ),
            processing_errors=Counter(
                "incidentops_order_processing_errors_total",
                "Malformed messages and processing or database failures.",
                registry=registry,
            ),
            processing_duration=Histogram(
                "incidentops_order_processing_duration_seconds",
                "Seconds spent in the complete per-message processing path.",
                buckets=PROCESSING_BUCKETS,
                registry=registry,
            ),
            database_duration=Histogram(
                "incidentops_database_operation_duration_seconds",
                "Seconds spent in the PostgreSQL order transaction.",
                buckets=DATABASE_BUCKETS,
                registry=registry,
            ),
            kafka_consumer_lag=Gauge(
                "incidentops_kafka_consumer_lag",
                "Total Kafka high-watermark offset minus committed group offset.",
                registry=registry,
            ),
        )
        _consumer_cache[registry] = metrics
        return metrics


class MetricsServer:
    """Own a Prometheus WSGI server so applications can stop it cleanly."""

    def __init__(self, server: Any, thread: Thread) -> None:
        self._server = server
        self._thread = thread

    @classmethod
    def start(
        cls,
        *,
        host: str,
        port: int,
        registry: CollectorRegistry,
    ) -> MetricsServer:
        """Start one daemon HTTP server for an explicit registry."""

        server, thread = start_http_server(port=port, addr=host, registry=registry)
        return cls(server, thread)

    def close(self) -> None:
        """Stop accepting scrapes and release the listening port."""

        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
