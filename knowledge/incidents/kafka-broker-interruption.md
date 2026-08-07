---
schema_version: 1
document_id: incident_kafka_broker_interruption
document_type: incident
title: Kafka Broker Interruption Incident
services: [order-producer, kafka, order-consumer]
technologies: [python, kafka, docker, elasticsearch]
incident_types: [kafka_broker_failure, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Kafka Broker Interruption Incident

## Purpose
Record the evidence pattern for an interruption affecting Kafka publication or consumption.

## Symptoms
- Kafka errors appear in producer or consumer logs.
- Delivery and polling throughput fall near the same time.
- Lag can rise after publication resumes if the consumer was unavailable longer.

## Likely Causes
- Broker unavailability or incorrect advertised endpoint connectivity.
- Slow consumer processing is a distractor when lag persists after broker recovery.

## Checks
- Confirm broker health and correlate Kafka events across both applications.
- Compare processing duration to determine whether a second bottleneck exists.
- Verify the configured topic and consumer group remain unchanged.

## Safe Actions
- Restore the existing broker connectivity and observe bounded recovery signals.
- Preserve Kafka storage, topics, partitions, and offsets.

## Escalation
Escalate unavailable partitions, repeated broker restarts, storage faults, or any proposed destructive administration.
