---
schema_version: 1
document_id: runbook_database_connectivity
document_type: runbook
title: Database Connectivity Response
services: [order-consumer, postgres]
technologies: [python, postgres, elasticsearch]
incident_types: [database_unavailable, database_latency, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Database Connectivity Response

## Purpose
Diagnose PostgreSQL failures that block idempotent order processing.

## Symptoms
- Database connection or processing error events appear.
- Consumer throughput falls and Kafka lag may rise.
- Kafka can remain healthy while completed database writes stop.

## Likely Causes
- PostgreSQL is unavailable or credentials and endpoint settings are incorrect.
- Connection saturation or slow queries increase processing latency.
- A Kafka failure can also reduce throughput, so lag is not database-specific.

## Checks
- Inspect PostgreSQL health and bounded consumer database error events.
- Confirm host, port, database, and user settings without printing secrets.
- Compare database evidence with Kafka error checks and processing duration.

## Safe Actions
- Restore the validated connection configuration or database availability.
- Preserve the processed-orders table and rely on idempotent inserts during replay.

## Escalation
Escalate for persistent saturation, storage faults, corruption signals, or any action that could remove database data.
