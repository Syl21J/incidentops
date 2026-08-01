"""Unit coverage for safe Prometheus collector registration."""

from prometheus_client import CollectorRegistry

from incidentops.metrics import create_consumer_metrics, create_producer_metrics


def _metric_names(registry: CollectorRegistry) -> set[str]:
    return {metric.name for collector in registry.collect() for metric in collector.samples}


def test_producer_metrics_register_once_and_update_after_delivery() -> None:
    registry = CollectorRegistry()
    first = create_producer_metrics(registry)
    second = create_producer_metrics(registry)

    assert first is second
    first.orders_produced.inc()
    first.production_duration.observe(0.02)
    first.target_rate.set(25)

    values = {
        sample.name: sample.value
        for collector in registry.collect()
        for sample in collector.samples
    }
    assert values["incidentops_orders_produced_total"] == 1
    assert values["incidentops_order_production_duration_seconds_count"] == 1
    assert values["incidentops_producer_target_rate"] == 25


def test_consumer_metrics_use_required_names_and_update_successfully() -> None:
    registry = CollectorRegistry()
    metrics = create_consumer_metrics(registry)
    metrics.orders_consumed.inc()
    metrics.orders_processed.inc()
    metrics.processing_duration.observe(0.8)
    metrics.database_duration.observe(0.01)
    metrics.kafka_consumer_lag.set(120)

    names = _metric_names(registry)
    assert "incidentops_orders_consumed_total" in names
    assert "incidentops_orders_processed_total" in names
    assert "incidentops_order_processing_errors_total" in names
    assert "incidentops_order_processing_duration_seconds_count" in names
    assert "incidentops_database_operation_duration_seconds_count" in names
    assert "incidentops_kafka_consumer_lag" in names


def test_custom_metrics_have_no_high_cardinality_labels() -> None:
    registries = [create_producer_metrics().registry, create_consumer_metrics().registry]

    for registry in registries:
        for collector in registry.collect():
            for sample in collector.samples:
                assert "event_id" not in sample.labels
                assert "order_id" not in sample.labels
                assert "run_id" not in sample.labels
                assert "exception" not in sample.labels
                assert sample.labels.keys() <= {"le"}
