#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_DIR="$(mktemp -d -t incidentops-agent-workflow.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly REPORT_FILE="${TEMP_DIR}/incident-report.json"

scenario_run_id=""
evaluation_file="${TEMP_DIR}/evaluation.json"

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

cleanup() {
  local exit_status=$?
  local cleanup_failed=false
  local parsed_run_id=""

  trap - EXIT
  if [[ -z "${scenario_run_id}" && -f "${METADATA_FILE}" ]]; then
    if parsed_run_id="$(uv run python - "${METADATA_FILE}" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["run_id"])
PY
)"; then
      scenario_run_id="${parsed_run_id}"
    else
      error "Could not recover the retained run_id from scenario metadata."
      cleanup_failed=true
    fi
  fi
  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting retained Elasticsearch documents for run_id ${scenario_run_id}"
    if ! RUN_ID_TO_DELETE="${scenario_run_id}" uv run python - <<'PY'
import os

from elasticsearch import Elasticsearch

client = Elasticsearch("http://localhost:9200", request_timeout=10)
try:
    client.delete_by_query(
        index="incidentops-logs-*",
        query={"term": {"run_id": os.environ["RUN_ID_TO_DELETE"]}},
        allow_no_indices=True,
        conflicts="proceed",
        ignore_unavailable=True,
        refresh=True,
    )
finally:
    client.close()
PY
    then
      error "Could not delete retained documents for this scenario run."
      cleanup_failed=true
    fi
  fi

  rm -rf -- "${TEMP_DIR}"
  if [[ "${cleanup_failed}" == "true" && "${exit_status}" -eq 0 ]]; then
    exit_status=1
  fi
  exit "${exit_status}"
}

trap cleanup EXIT
cd "${PROJECT_DIR}"

if ! command -v uv >/dev/null 2>&1; then
  error "uv is required but was not found."
  exit 1
fi

log "Validating Compose and the existing infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Verifying Prometheus, Elasticsearch, and Filebeat"
curl --fail --silent --show-error http://localhost:9090/-/healthy >/dev/null
curl --fail --silent --show-error http://localhost:9200/_cluster/health >/dev/null
docker compose exec -T filebeat \
  filebeat test output -c /usr/share/filebeat/filebeat.yml >/dev/null
success "Required observability services are healthy"

log "Running the isolated slow_consumer_v1 scenario"
./scripts/check-slow-consumer-scenario.sh \
  --retain-investigation-data \
  --output-metadata "${METADATA_FILE}"

read -r scenario_run_id scenario_start scenario_end < <(
  uv run python - "${METADATA_FILE}" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(metadata["run_id"], metadata["start_time"], metadata["end_time"])
PY
)
if [[ -z "${scenario_run_id}" || -z "${scenario_start}" || -z "${scenario_end}" ]]; then
  error "Scenario metadata is incomplete."
  exit 1
fi
success "Captured exact scenario window ${scenario_start} to ${scenario_end}"

artifact_directory="$(uv run python -c '
from incidentops.config import Settings
print(Settings().investigation_artifact_directory)
')"
evaluation_file="${artifact_directory}/${scenario_run_id}.evaluation.json"

log "Executing LangGraph with the explicit non-production scripted model"
uv run python -m incidentops.investigation.cli investigate \
  --description "Orders were delayed during the bounded scenario window." \
  --start-time "${scenario_start}" \
  --end-time "${scenario_end}" \
  --run-id "${scenario_run_id}" \
  --affected-service order-consumer \
  --output-format json \
  --output-file "${REPORT_FILE}" \
  --model-provider scripted-test \
  --persist-artifacts
success "The workflow produced a validated structured report"

log "Evaluating the report against slow_consumer.yaml"
uv run python -m incidentops.evaluation.cli \
  --report "${REPORT_FILE}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${evaluation_file}"

uv run python - \
  "${REPORT_FILE}" \
  "${evaluation_file}" \
  "${METADATA_FILE}" \
  "${artifact_directory}" <<'PY'
import json
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
evaluation = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
metadata = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))

assert report["status"] == "diagnosed"
assert report["primary_root_cause"]["cause_code"] == "slow_consumer_processing"
assert evaluation["root_cause_exact_match"] is True
assert evaluation["root_cause_rank"] == 1
assert evaluation["expected_metric_evidence_recall"] == 1.0
assert evaluation["expected_log_evidence_recall"] == 1.0
assert evaluation["negative_evidence_recall"] == 1.0
assert evaluation["unsupported_evidence_reference_count"] == 0
assert evaluation["forbidden_action_count"] == 0
assert evaluation["tool_call_count"] <= 10
assert evaluation["investigation_attempt_count"] <= 2
assert report["tool_call_count"] == 6

metric_sources = [
    item for item in report["supporting_evidence"] if item["source"] == "prometheus"
]
log_sources = [
    item for item in report["supporting_evidence"] if item["source"] == "elasticsearch"
]
assert len(metric_sources) == 3
assert len(log_sources) >= 1
assert len(report["negative_evidence"]) == 2
assert all(item["source"] == "elasticsearch" for item in report["negative_evidence"])

artifact_directory = Path(sys.argv[4])
trace_path = artifact_directory / f'{report["investigation_id"]}.trace.jsonl'
report_artifact = artifact_directory / f'{report["investigation_id"]}.report.json'
assert trace_path.is_file()
assert report_artifact.is_file()
trace_events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
event_types = {item["event_type"] for item in trace_events}
assert "tool_called" in event_types
assert "tool_completed" in event_types
assert "verification_completed" in event_types
assert "investigation_completed" in event_types

observations = metadata["observations"]
print("\nIncidentOps agent workflow validation succeeded.")
print(f'Root cause: {report["primary_root_cause"]["cause_code"]}')
print(f'Diagnosis status: {report["status"]}')
print("Metric evidence found: 3/3")
print(f'Log evidence found: {len(log_sources)}/1')
print(f'Negative evidence found: {len(report["negative_evidence"])}/2')
print(f'Tool calls: {evaluation["tool_call_count"]}')
print(f'Investigation attempts: {evaluation["investigation_attempt_count"]}')
print(f'Unsupported references: {evaluation["unsupported_evidence_reference_count"]}')
print(f'Forbidden actions: {evaluation["forbidden_action_count"]}')
print(f'Workflow duration: {evaluation["workflow_duration_seconds"]:.3f} seconds')
print(f'Real maximum lag: {observations["maximum_lag"]}')
print(f'Real P95 processing duration: {observations["p95_seconds"]} seconds')
print(f'Real slow-processing logs: {observations["slow_processing_log_count"]}')
print(f'Persisted report: {report_artifact}')
print(f'Persisted trace: {trace_path}')
print(f'Persisted evaluation: {Path(sys.argv[2])}')
PY
