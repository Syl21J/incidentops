#!/usr/bin/env bash

# Shared setup for explicit benchmark and investigation runs. Source from an entry point.

readonly PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

log() {
  printf '[INFO] %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

prepare_environment() {
  local live_model="$1"
  local rag_dependencies="${2:-false}"
  local dependency
  local -a sync_arguments=(--frozen)

  cd "${PROJECT_DIR}"
  for dependency in uv docker curl; do
    if ! command -v "${dependency}" >/dev/null 2>&1; then
      error "${dependency} is required but was not found."
      return 1
    fi
  done

  if [[ "${rag_dependencies}" == "true" ]]; then
    sync_arguments+=(--extra rag)
  fi
  log "Installing the locked Python environment"
  uv sync "${sync_arguments[@]}"

  if [[ "${live_model}" == "true" ]]; then
    log "Checking the configured live model before starting services"
    # Settings reads .env as data; never source it as shell code or print credentials.
    uv run python -m incidentops.validation.cli validate-live-model
  fi

  log "Validating Compose and starting the project infrastructure"
  mkdir -p logs
  docker compose config --quiet
  docker compose up -d
  "${PROJECT_DIR}/scripts/check-infrastructure.sh"
  log "Pipeline dashboard: http://$(docker compose port grafana 3000)/d/incidentops-pipeline/incidentops-order-pipeline"

  log "Initializing PostgreSQL and Elasticsearch idempotently"
  "${PROJECT_DIR}/scripts/initialize-database.sh"
  "${PROJECT_DIR}/scripts/initialize-elasticsearch.sh"
  docker compose exec -T filebeat \
    filebeat test output -c /usr/share/filebeat/filebeat.yml >/dev/null
}

ingest_knowledge() {
  log "Ingesting the controlled knowledge corpus with local CPU embeddings"
  EMBEDDING_PROVIDER=sentence-transformers \
    uv run incidentops-knowledge ingest --knowledge-directory knowledge
}
