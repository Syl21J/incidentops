---
schema_version: 1
document_id: runbook_malformed_order_events
document_type: runbook
title: Malformed Order Event Response
services: [order-producer, kafka, order-consumer]
technologies: [python, kafka, elasticsearch]
incident_types: [malformed_event, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Malformed Order Event Response

## Purpose
Identify invalid order payloads without replaying, deleting, or rewriting Kafka records.

## Symptoms
- The consumer rejects an event before database processing.
- Validation failures cluster around one producer version or payload shape.
- Consumer throughput can fall while lag rises if invalid events are repeatedly retried.

## Likely Causes
- A required identifier is missing or uses an unsupported type.
- A producer contract change is incompatible with the validated consumer model.
- Slow processing can also increase lag but does not explain validation errors.

## Checks
- Inspect bounded validation event counts and safe field names without copying raw payloads.
- Confirm the producing service version and expected schema.
- Compare consumer lag with database and Kafka error checks.

## Safe Actions
- Correct the producer contract or consumer validation through reviewed application changes.
- Preserve the original topic, offsets, and malformed records for controlled analysis.

## Escalation
Escalate when multiple producers emit incompatible schemas or recovery requires a reviewed replay plan.
