---
schema_version: 1
document_id: log_event_slow_processing
document_type: log_event
title: Slow Processing Log Event
services: [order-consumer, elasticsearch]
technologies: [python, elasticsearch]
incident_types: [slow_processing, consumer_lag, database_latency]
status: active
updated_at: 2026-08-05
---
# Slow Processing Log Event

## Purpose
Interpret structured `slow_processing` events emitted after an order exceeds the threshold.

## Symptoms
- Events cluster while processing P95 and consumer lag rise.
- Events contain bounded duration and correlation identifiers.
- No events appear even though aggregate duration looks elevated.

## Likely Causes
- Application work or database latency exceeded the configured threshold.
- A threshold mismatch or log pipeline failure can explain missing events.

## Checks
- Filter by service, event type, run identifier, and exact UTC window.
- Compare event durations with the Prometheus duration summary.
- Check database errors and log-pipeline health before assigning cause.

## Safe Actions
- Use event identifiers only for correlation, never as metric labels.
- Correct the confirmed slow operation while preserving database idempotency.

## Escalation
Escalate repeated high durations with unclear internal cause or inconsistent metric and log evidence.
