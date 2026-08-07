---
schema_version: 1
document_id: runbook_consumer_rebalance
document_type: runbook
title: Consumer Rebalance Response
services: [kafka, order-consumer]
technologies: [python, kafka, prometheus, elasticsearch]
incident_types: [consumer_rebalance, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Consumer Rebalance Response

## Purpose
Distinguish a temporary Kafka group rebalance from a persistent processing bottleneck.

## Symptoms
- Consumption pauses briefly while partitions are reassigned.
- Lag rises during the pause and begins draining after assignment stabilizes.
- Consumer lifecycle events appear without database or broker failure evidence.

## Likely Causes
- A consumer instance joined, left, restarted, or missed its group heartbeat.
- Long processing can trigger repeated rebalances when polling deadlines are exceeded.
- A traffic spike raises lag without necessarily causing group membership changes.

## Checks
- Correlate bounded consumer lifecycle events with the lag interval.
- Confirm assignments stabilize and consumer throughput resumes.
- Check processing duration, Kafka errors, and database errors for a persistent secondary cause.

## Safe Actions
- Restore stable consumer membership and correct confirmed polling or heartbeat settings.
- Preserve offsets and avoid group administration during evidence collection.

## Escalation
Escalate repeated rebalances, unstable membership, or lag that continues growing after assignment recovery.
