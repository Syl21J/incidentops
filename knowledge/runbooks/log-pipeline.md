---
schema_version: 1
document_id: runbook_log_pipeline
document_type: runbook
title: Log Pipeline Response
services: [order-producer, order-consumer, filebeat, elasticsearch]
technologies: [python, filebeat, elasticsearch, docker]
incident_types: [log_pipeline_failure]
status: active
updated_at: 2026-08-05
---
# Log Pipeline Response

## Purpose
Restore structured log delivery while preserving application and collector state.

## Symptoms
- JSONL files contain recent events but Elasticsearch queries return none.
- Filebeat reports an output or registry error.
- Application stdout remains available while indexed logs stop.

## Likely Causes
- Elasticsearch is unavailable or Filebeat cannot reach it.
- File permissions prevent the read-only log mount from being harvested.
- A wrong run identifier or UTC window hides otherwise indexed documents.

## Checks
- Inspect application files, Filebeat health, and Elasticsearch health in that order.
- Verify Filebeat output and use a bounded query for the exact run identifier.
- Confirm the versioned log mapping remains compatible.

## Safe Actions
- Restore connectivity or read access without deleting registry files, volumes, or indices.
- Keep the existing filestream identity to prevent replaying old files.

## Escalation
Escalate when indexed mappings are incompatible or collector state cannot progress without a scoped migration.
