---
schema_version: 1
document_id: metric_consumer_lag
document_type: metric
title: Kafka Consumer Lag Metric
services: [kafka, order-consumer, prometheus]
technologies: [kafka, prometheus, python]
incident_types: [consumer_lag, slow_processing, traffic_spike, database_latency, kafka_broker_failure]
status: active
updated_at: 2026-08-05
---
# Kafka Consumer Lag Metric

## Purpose
Interpret the bounded total lag summary for the configured order consumer group.

## Symptoms
- Lag is positive and increases across consecutive samples.
- Lag stays elevated after publication slows.
- No samples are returned for the expected consumer group.

## Likely Causes
- Slow processing, database latency, a traffic spike, or broker disruption can all increase lag.
- Missing samples can reflect an unassigned consumer or a Prometheus scrape problem.

## Checks
- Compare the first, last, minimum, and maximum lag values in one bounded window.
- Correlate lag with rate comparison, processing duration, and structured error events.
- Verify the topic and group labels are the expected low-cardinality values.

## Safe Actions
- Treat lag as a symptom and collect a discriminating signal before recommending changes.
- Restore metric collection when samples are unavailable.

## Escalation
Escalate sustained growth with no identified bottleneck or any evidence of unavailable Kafka partitions.
