---
schema_version: 1
document_id: runbook_consumer_lag_triage
document_type: runbook
title: Consumer Lag Triage
services: [order-producer, kafka, order-consumer, postgres]
technologies: [python, kafka, postgres, prometheus, elasticsearch]
incident_types: [consumer_lag, slow_processing, traffic_spike, database_latency, kafka_broker_failure]
status: active
updated_at: 2026-08-05
---
# Consumer Lag Triage

## Purpose
Distinguish the main causes of growing Kafka consumer lag without changing offsets.

## Symptoms
- Total consumer lag rises across consecutive samples.
- Producer throughput is greater than consumer throughput.
- Order processing completion is delayed.

## Likely Causes
- Per-message processing is slow even when Kafka and PostgreSQL are healthy.
- A traffic spike exceeds stable consumer capacity.
- Database latency blocks consumer commits.
- Kafka broker errors interrupt fetches; lag alone does not prove this cause.

## Checks
- Compare producer and consumer rates over the exact incident window.
- Check processing P95, database error events, and Kafka error events.
- Verify whether zero database and Kafka errors provide useful negative evidence.

## Safe Actions
- Remove a confirmed artificial or application processing delay.
- Scale consumers only when partitions permit useful parallelism.
- Preserve consumer offsets while the cause is uncertain.

## Escalation
Escalate when lag grows with normal processing latency and no traffic increase, or when broker availability is degraded.
