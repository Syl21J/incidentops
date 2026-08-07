---
schema_version: 1
document_id: log_event_kafka_errors
document_type: log_event
title: Kafka Error Log Events
services: [order-producer, kafka, order-consumer, elasticsearch]
technologies: [python, kafka, elasticsearch]
incident_types: [kafka_broker_failure, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Kafka Error Log Events

## Purpose
Interpret structured producer and consumer errors associated with Kafka transport or broker access.

## Symptoms
- Delivery, polling, or broker errors occur near a throughput drop.
- Consumer lag rises while processing duration remains normal.
- Both producer and consumer report connectivity problems.

## Likely Causes
- Broker unavailability, endpoint mismatch, or transient transport failure.
- Lag without Kafka errors more often points to traffic, processing, or database causes.

## Checks
- Search only allow-listed event types in the bounded incident window.
- Compare errors across producer and consumer services.
- Preserve zero matches as negative evidence rather than claiming broker failure from lag alone.

## Safe Actions
- Restore the existing broker connection and verify application recovery.
- Do not reset offsets, delete topics, or run Kafka administration from the investigation.

## Escalation
Escalate persistent broker health failures, unavailable partitions, or repeated cross-service transport errors.
