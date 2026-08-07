---
schema_version: 1
document_id: metric_processing_duration
document_type: metric
title: Order Processing Duration Metric
services: [order-consumer, prometheus, postgres]
technologies: [python, prometheus, postgres]
incident_types: [slow_processing, database_latency, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Order Processing Duration Metric

## Purpose
Interpret the fixed P95 summary of consumer processing duration.

## Symptoms
- P95 rises above the configured slow-processing threshold.
- Duration is elevated during a period of increasing consumer lag.
- Duration samples are absent despite processed orders.

## Likely Causes
- Consumer work, a development delay, or database latency extends the measured operation.
- Prometheus scrape gaps can make an otherwise healthy histogram unavailable.

## Checks
- Use the same bounded window for duration, lag, and log evidence.
- Look for slow-processing and database error events.
- Confirm at least two scrapes and sufficient processed observations exist.

## Safe Actions
- Optimize only the operation supported by timing and log evidence.
- Restore the Prometheus target without changing metric labels or storage.

## Escalation
Escalate when high duration lacks a local explanation or when database performance requires specialist analysis.
