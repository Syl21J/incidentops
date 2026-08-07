---
schema_version: 1
document_id: incident_traffic_spike_backlog
document_type: incident
title: Traffic Spike Backlog Incident
services: [order-producer, kafka, order-consumer]
technologies: [python, kafka, prometheus]
incident_types: [traffic_spike, consumer_lag]
status: active
updated_at: 2026-08-05
---
# Traffic Spike Backlog Incident

## Purpose
Record the evidence pattern for backlog caused by publication above stable consumer capacity.

## Symptoms
- Producer rate rises sharply above its recent behavior.
- Consumer rate remains stable and processing duration stays normal.
- Consumer lag grows without database or Kafka errors.

## Likely Causes
- Legitimate or test traffic exceeded the single consumer's sustainable throughput.
- Slow processing is a distractor when only the rate imbalance and lag are inspected.

## Checks
- Compare producer and consumer rates with processing P95.
- Verify the producer surge begins before lag growth.
- Check for absent database and Kafka errors.

## Safe Actions
- Reduce avoidable publication pressure or use human-reviewed consumer scaling when partitions permit.
- Allow the existing consumer group to drain backlog without resetting offsets.

## Escalation
Escalate when sustained demand exceeds designed capacity or partitioning prevents safe scaling.
