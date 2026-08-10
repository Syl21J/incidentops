#!/usr/bin/env bash

# Purpose: deterministic end-to-end RAG validation with the scripted-test model.
# This script uses real infrastructure, metrics, logs, embeddings, and retrieval, but it never
# contacts an external LLM endpoint and does not require an API key.
# Run when: the corpus, chunking, embeddings, ingestion, retrieval, RAG integration, or retrieval
# evaluation changes. The initialized Compose services must already be running.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_DIR="$(mktemp -d -t incidentops-rag-workflow.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly BASELINE_REPORT="${TEMP_DIR}/baseline-report.json"
readonly RAG_REPORT="${TEMP_DIR}/rag-report.json"
readonly BASELINE_EVALUATION="${TEMP_DIR}/baseline-evaluation.json"
readonly RAG_EVALUATION="${TEMP_DIR}/rag-evaluation.json"
readonly RETRIEVAL_EVALUATION="${TEMP_DIR}/retrieval-evaluation.json"

scenario_run_id=""

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
  local recovered_run_id=""
  trap - EXIT
  if [[ -z "${scenario_run_id}" && -f "${METADATA_FILE}" ]]; then
    if recovered_run_id="$(
      uv run python -m incidentops.validation.cli metadata-run-id "${METADATA_FILE}"
    )"; then
      scenario_run_id="${recovered_run_id}"
    else
      error "Could not recover the retained scenario run identifier."
      exit_status=1
    fi
  fi
  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting only retained log documents for run_id ${scenario_run_id}"
    if ! uv run python -m incidentops.validation.cli delete-run-logs "${scenario_run_id}"
    then
      error "Could not remove the retained run-scoped log documents."
      exit_status=1
    fi
  fi
  rm -rf -- "${TEMP_DIR}"
  exit "${exit_status}"
}

trap cleanup EXIT
cd "${PROJECT_DIR}"

log "Validating Compose and required infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Ingesting the complete controlled corpus with CPU embeddings"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge ingest --knowledge-directory knowledge

log "Running the ten-case hybrid retrieval benchmark"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge evaluate \
    --cases evaluation/retrieval_cases.yaml \
    --mode hybrid >"${RETRIEVAL_EVALUATION}"

uv run python -m incidentops.validation.cli \
  validate-retrieval-benchmark "${RETRIEVAL_EVALUATION}"
success "Hybrid retrieval benchmark passed"

log "Running slow_consumer_v1 once and retaining only its bounded log window"
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

common_arguments=(
  investigate
  --description "Orders were delayed during the bounded scenario window."
  --start-time "${scenario_start}"
  --end-time "${scenario_end}"
  --run-id "${scenario_run_id}"
  --affected-service order-consumer
  --output-format json
  --model-provider scripted-test
)

log "Running the baseline graph against the retained live evidence"
# Keep the baseline independent from any knowledge settings in the developer's .env file.
KNOWLEDGE_ENABLED=false \
  uv run python -m incidentops.investigation.cli \
    "${common_arguments[@]}" \
    --output-file "${BASELINE_REPORT}"

log "Running the RAG graph against exactly the same retained live evidence"
# Pin the retrieval contract so this acceptance check cannot silently degrade or change mode.
KNOWLEDGE_ENABLED=true \
KNOWLEDGE_REQUIRED=true \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
EMBEDDING_PROVIDER=sentence-transformers \
  uv run python -m incidentops.investigation.cli \
    "${common_arguments[@]}" \
    --output-file "${RAG_REPORT}"

uv run python -m incidentops.evaluation.cli \
  --report "${BASELINE_REPORT}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${BASELINE_EVALUATION}"
uv run python -m incidentops.evaluation.cli \
  --report "${RAG_REPORT}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${RAG_EVALUATION}"

uv run python -m incidentops.validation.cli validate-rag-workflow \
  --baseline-report "${BASELINE_REPORT}" \
  --rag-report "${RAG_REPORT}" \
  --baseline-evaluation "${BASELINE_EVALUATION}" \
  --rag-evaluation "${RAG_EVALUATION}"
