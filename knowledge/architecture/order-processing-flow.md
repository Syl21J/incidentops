---
schema_version: 1
document_id: architecture_order_processing_flow
document_type: architecture
title: Order Processing Flow
services: [order-producer, kafka, order-consumer, postgres]
technologies: [python, kafka, postgres]
incident_types: [consumer_lag, duplicate_processing]
status: active
updated_at: 2026-08-05
---
# Order Processing Flow

## Purpose
Describe the durable path from order creation to idempotent database storage.

## Symptoms
- Producer logs continue while processed-order throughput falls.
- Kafka consumer lag rises when downstream processing is slower than publication.
- Replayed events may appear after a consumer restart without creating duplicate rows.

## Likely Causes
- Slow consumer processing, database latency, a traffic spike, or a Kafka broker problem can all increase lag.
- A mismatched consumer group can make a healthy consumer appear disconnected from the expected offsets.

## Checks
- Compare producer and consumer rates over the same bounded time window.
- Check consumer lag, processing duration, Kafka errors, and database errors together.
- Confirm the configured topic and consumer group match the running applications.

## Safe Actions
- Preserve the topic, offsets, and PostgreSQL data while collecting evidence.
- Reduce confirmed processing latency or scale consumers only after checking partition capacity.

## Escalation
Escalate when lag continues to grow after the confirmed bottleneck is removed or when Kafka reports unavailable partitions.
