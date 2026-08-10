#!/usr/bin/env bash

# Purpose: validate the bounded LangGraph investigation end to end with the scripted model and
# real scenario evidence from Prometheus and Elasticsearch.
# Run when: investigation nodes, tools, verification, reporting, evaluation, or scenario
# integration changes. The initialized Compose services must already be running.

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
    if parsed_run_id="$(
      uv run python -m incidentops.validation.cli metadata-run-id "${METADATA_FILE}"
    )"; then
      scenario_run_id="${parsed_run_id}"
    else
      error "Could not recover the retained run_id from scenario metadata."
      cleanup_failed=true
    fi
  fi
  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting retained Elasticsearch documents for run_id ${scenario_run_id}"
    if ! uv run python -m incidentops.validation.cli delete-run-logs "${scenario_run_id}"
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
  uv run python -m incidentops.validation.cli scenario-window "${METADATA_FILE}"
)
if [[ -z "${scenario_run_id}" || -z "${scenario_start}" || -z "${scenario_end}" ]]; then
  error "Scenario metadata is incomplete."
  exit 1
fi
success "Captured exact scenario window ${scenario_start} to ${scenario_end}"

artifact_directory="$(uv run python -m incidentops.validation.cli artifact-directory)"
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

uv run python -m incidentops.validation.cli validate-agent-workflow \
  --report "${REPORT_FILE}" \
  --evaluation "${evaluation_file}" \
  --metadata "${METADATA_FILE}" \
  --artifact-directory "${artifact_directory}"
