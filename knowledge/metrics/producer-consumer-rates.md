---
schema_version: 1
document_id: metric_producer_consumer_rates
document_type: metric
title: Producer Consumer Rate Comparison
services: [order-producer, order-consumer, prometheus]
technologies: [python, prometheus, kafka]
incident_types: [consumer_lag, traffic_spike, slow_processing]
status: active
updated_at: 2026-08-05
---
# Producer Consumer Rate Comparison

## Purpose
Compare fixed producer and consumer throughput summaries over one incident window.

## Symptoms
- Producer rate exceeds consumer rate while lag grows.
- Both rates fall even though backlog exists.
- One rate is missing because its application target was not scraped.

## Likely Causes
- A traffic spike raises producer rate above normal consumer capacity.
- Slow processing lowers consumer rate without increasing producer rate.
- Kafka or database failures can reduce one or both rates.

## Checks
- Align the rate window with lag, processing duration, and structured events.
- Distinguish a producer surge from a consumer slowdown using baseline behavior.
- Check Prometheus target health when either series is absent.

## Safe Actions
- Reduce confirmed processing cost or apply human-reviewed scaling when partitions allow it.
- Wait for valid samples rather than inferring a zero rate from missing data.

## Escalation
Escalate when throughput remains inconsistent after targets recover or capacity limits are unclear.
