#!/usr/bin/env bash

# Purpose: run the complete deterministic Stage Seven benchmark against real local telemetry.
# The runner injects each bounded incident once, compares no-RAG and required-RAG investigations
# over the same window, and deletes only resources carrying each run's unique identifiers.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly RESULT_DIRECTORY="${PROJECT_DIR}/artifacts/benchmarks"
readonly RESULT_FILE="${BENCHMARK_OUTPUT:-${RESULT_DIRECTORY}/multi-incident-$(date -u +%Y%m%dT%H%M%SZ).json}"

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

cd "${PROJECT_DIR}"

if ! command -v uv >/dev/null 2>&1; then
  error "uv is required but was not found."
  exit 1
fi
if ! command -v docker >/dev/null 2>&1; then
  error "Docker is required but was not found."
  exit 1
fi

log "Validating Compose and the running infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Initializing PostgreSQL and Elasticsearch idempotently"
./scripts/initialize-database.sh
./scripts/initialize-elasticsearch.sh

log "Ingesting the controlled knowledge corpus with local CPU embeddings"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge ingest --knowledge-directory knowledge

mkdir -p -- "${RESULT_DIRECTORY}"
log "Running all four incidents once and comparing no RAG with required RAG"
EMBEDDING_PROVIDER=sentence-transformers \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
  uv run python -m incidentops.benchmark.cli run \
    --all \
    --model-provider deterministic-test \
    --knowledge-mode compare \
    --output-file "${RESULT_FILE}"

uv run python -m incidentops.benchmark.cli validate "${RESULT_FILE}"
success "All diagnoses, aggregate metrics, confusion matrices, and scoped cleanup passed"
printf 'Benchmark result: %s\n' "${RESULT_FILE}"
