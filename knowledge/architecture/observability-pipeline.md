---
schema_version: 1
document_id: architecture_observability_pipeline
document_type: architecture
title: Observability Pipeline
services: [order-producer, order-consumer, filebeat, elasticsearch, prometheus]
technologies: [python, filebeat, elasticsearch, prometheus, docker]
incident_types: [log_pipeline_failure, metrics_unavailable]
status: active
updated_at: 2026-08-05
---
# Observability Pipeline

## Purpose
Explain how application logs and metrics reach their independent local stores.

## Symptoms
- Application processing remains healthy while Elasticsearch has no recent events.
- Prometheus targets become down even though JSON logs continue.
- File logs exist but Filebeat does not advance its registry.

## Likely Causes
- Filebeat output failure affects logs without affecting order processing.
- An unreachable WSL metrics endpoint affects Prometheus without stopping the application.
- Incorrect time windows can look like missing telemetry in either store.

## Checks
- Inspect application output before checking Filebeat and Elasticsearch health.
- Verify Prometheus targets and the exact application metrics endpoints.
- Use bounded queries with the same run identifier and UTC incident window.

## Safe Actions
- Restore connectivity without deleting Filebeat registry or Elasticsearch indices.
- Keep Prometheus storage and wait for two scrapes after restoring a target.

## Escalation
Escalate when the source telemetry is present but the corresponding collector remains unable to forward or scrape it.
