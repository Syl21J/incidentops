---
schema_version: 1
document_id: runbook_slow_processing
document_type: runbook
title: Slow Processing Response
services: [order-consumer, postgres]
technologies: [python, postgres, prometheus, elasticsearch]
incident_types: [slow_processing, consumer_lag, database_latency]
status: active
updated_at: 2026-08-05
---
# Slow Processing Response

## Purpose
Validate and reduce excessive order-consumer processing duration.

## Symptoms
- Processing duration P95 is elevated.
- Structured slow-processing events appear for the consumer.
- Consumer lag rises while producer activity remains steady.

## Likely Causes
- Application work or an explicit development delay extends every message.
- PostgreSQL latency can produce the same duration and lag symptoms.
- CPU contention can increase duration without database errors.

## Checks
- Correlate slow-processing events with processing duration in the same UTC window.
- Check database errors before attributing all delay to application code.
- Compare producer and consumer rates to confirm sustained backlog growth.

## Safe Actions
- Remove only a confirmed development delay or optimize the measured slow operation.
- Keep idempotent database writes and offset handling unchanged during triage.

## Escalation
Escalate when duration remains elevated after the confirmed slow operation is removed or database latency is suspected.
