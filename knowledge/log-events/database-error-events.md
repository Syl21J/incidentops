---
schema_version: 1
document_id: log_event_database_errors
document_type: log_event
title: Database Error Log Events
services: [order-consumer, postgres, elasticsearch]
technologies: [python, postgres, elasticsearch]
incident_types: [database_unavailable, database_latency, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Database Error Log Events

## Purpose
Interpret allow-listed consumer events that indicate database connection or processing failure.

## Symptoms
- Database errors coincide with reduced consumer throughput.
- Lag rises while Kafka error checks remain empty.
- Processing retries or failures appear for the same run window.

## Likely Causes
- PostgreSQL is unreachable, saturated, or rejecting the configured connection.
- Application validation failure can resemble processing failure without database outage.

## Checks
- Query exact database event types with bounded results.
- Inspect PostgreSQL health and compare processing duration.
- Preserve a zero-result check as negative evidence when diagnosing other lag causes.

## Safe Actions
- Restore validated connectivity and allow idempotent processing to resume.
- Avoid printing credentials or issuing database writes from investigation tools.

## Escalation
Escalate recurring connection loss, storage faults, saturation, or any suspected data-integrity issue.
