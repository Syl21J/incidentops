#!/usr/bin/env bash

# Purpose: end-to-end RAG validation with the configured live OpenAI-compatible model.
# This script uses real infrastructure and may make up to four billable model calls. It requires
# valid LLM_MODEL and LLM_API_KEY settings, and the model must support strict structured output.
# Run when: explicitly validating prompts, model configuration, or real-model behavior after the
# deterministic agent and RAG checks pass. The initialized Compose services must already be running.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_DIR="$(mktemp -d -t incidentops-live-rag-workflow.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly REPORT_FILE="${TEMP_DIR}/live-rag-report.json"

scenario_run_id=""
evaluation_file="${TEMP_DIR}/live-rag-evaluation.json"

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
  local recovered_run_id=""

  trap - EXIT
  if [[ -z "${scenario_run_id}" && -f "${METADATA_FILE}" ]]; then
    if recovered_run_id="$(
      uv run python -m incidentops.validation.cli metadata-run-id "${METADATA_FILE}"
    )"; then
      scenario_run_id="${recovered_run_id}"
    else
      error "Could not recover the retained scenario run identifier."
      cleanup_failed=true
    fi
  fi

  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting only retained log documents for run_id ${scenario_run_id}"
    if ! uv run python -m incidentops.validation.cli delete-run-logs "${scenario_run_id}"
    then
      error "Could not remove the retained run-scoped log documents."
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

log "Validating the live model configuration without exposing credentials"
LLM_PROVIDER=openai-compatible \
  uv run python -m incidentops.validation.cli validate-live-model

log "Validating Compose and required infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Ingesting the controlled corpus with production CPU embeddings"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge ingest --knowledge-directory knowledge

log "Running slow_consumer_v1 once and retaining its bounded log window"
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
evaluation_file="${artifact_directory}/${scenario_run_id}.live-rag.evaluation.json"

log "Executing LangGraph with the live model and required hybrid retrieval"
# Pin the retrieval contract so a live acceptance run fails instead of silently losing RAG.
LLM_PROVIDER=openai-compatible \
KNOWLEDGE_ENABLED=true \
KNOWLEDGE_REQUIRED=true \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
EMBEDDING_PROVIDER=sentence-transformers \
  uv run python -m incidentops.investigation.cli investigate \
    --description "Orders were delayed during the bounded scenario window." \
    --start-time "${scenario_start}" \
    --end-time "${scenario_end}" \
    --run-id "${scenario_run_id}" \
    --affected-service order-consumer \
    --output-format json \
    --output-file "${REPORT_FILE}" \
    --model-provider openai-compatible \
    --persist-artifacts
success "The live model produced a locally validated structured report"

log "Evaluating the live report outside the investigation graph"
uv run python -m incidentops.evaluation.cli \
  --report "${REPORT_FILE}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${evaluation_file}"

uv run python -m incidentops.validation.cli validate-live-rag-workflow \
  --report "${REPORT_FILE}" \
  --evaluation "${evaluation_file}" \
  --artifact-directory "${artifact_directory}"
