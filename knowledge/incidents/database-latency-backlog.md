---
schema_version: 1
document_id: incident_database_latency_backlog
document_type: incident
title: Database Latency Backlog Incident
services: [order-consumer, postgres, kafka]
technologies: [python, postgres, kafka, prometheus, elasticsearch]
incident_types: [database_latency, slow_processing, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Database Latency Backlog Incident

## Purpose
Record the evidence pattern for consumer backlog driven by slow PostgreSQL operations.

## Symptoms
- Processing duration and consumer lag rise together.
- Database errors or latency evidence appears while Kafka error checks remain empty.
- Producer rate need not increase.

## Likely Causes
- Slow database response extends each consumer transaction.
- Application processing delay produces similar metric symptoms but lacks database evidence.

## Checks
- Correlate database events with processing duration and lag.
- Confirm producer rate did not surge before the backlog.
- Check Kafka errors explicitly and retain their absence as negative evidence.

## Safe Actions
- Investigate database saturation or slow operations with read-only diagnostics.
- Preserve the table and rely on idempotent inserts during recovery.

## Escalation
Escalate persistent saturation, storage pressure, corruption signals, or required database changes.
