---
schema_version: 1
document_id: incident_slow_consumer_processing
document_type: incident
title: Slow Consumer Processing Incident
services: [order-producer, kafka, order-consumer, postgres]
technologies: [python, kafka, postgres, prometheus, elasticsearch]
incident_types: [slow_processing, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Slow Consumer Processing Incident

## Purpose
Record the evidence pattern for a consumer slowed by per-order processing work.

## Symptoms
- Consumer lag increased while producer throughput stayed near its target.
- Consumer throughput fell below producer throughput.
- Processing P95 and slow-processing events were elevated.

## Likely Causes
- A bounded development processing delay or slow application operation affected every order.
- Database and Kafka failures were plausible distractors but required separate checks.

## Checks
- Confirm elevated duration and slow-processing events in the same run window.
- Confirm database and Kafka error searches return no matching failures.
- Verify the lag trend reverses after the processing bottleneck is removed.

## Safe Actions
- Remove the confirmed delay or optimize the measured consumer operation.
- Retain Kafka offsets and PostgreSQL rows while backlog drains.

## Escalation
Escalate if lag does not drain after throughput recovers or if new database or Kafka evidence appears.
