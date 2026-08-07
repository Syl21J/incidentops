---
schema_version: 1
document_id: runbook_kafka_broker_health
document_type: runbook
title: Kafka Broker Health Response
services: [order-producer, kafka, order-consumer]
technologies: [python, kafka, docker, elasticsearch]
incident_types: [kafka_broker_failure, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Kafka Broker Health Response

## Purpose
Confirm broker or transport failures without administering Kafka from the investigation path.

## Symptoms
- Producer or consumer emits Kafka error events.
- Fetches or deliveries pause and consumer lag may rise.
- Both applications can report failures near the same time.

## Likely Causes
- The broker is unavailable or the advertised endpoint is unreachable.
- The external and internal Kafka endpoints were confused.
- Slow processing and traffic spikes also increase lag without broker errors.

## Checks
- Inspect the project Kafka container health and relevant service logs.
- Confirm WSL applications use the external endpoint and containers use the internal endpoint.
- Use bounded Kafka error searches; preserve zero results as negative evidence.

## Safe Actions
- Restore the existing broker or correct validated endpoint configuration.
- Keep topics, partitions, consumer groups, offsets, and volumes intact.

## Escalation
Escalate when the broker remains unhealthy, partitions are unavailable, or recovery would require destructive administration.
