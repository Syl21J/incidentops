#!/usr/bin/env bash

# Purpose: install and verify the versioned Elasticsearch log index template without deleting an
# existing index.
# Run when: setting up a new environment or after the log template or mapped fields change. The
# project Elasticsearch service must already be running and healthy.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMPLATE_FILE="${PROJECT_DIR}/elasticsearch/index-template.json"
readonly TEMPLATE_NAME="incidentops-logs"
readonly ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
readonly WAIT_TIMEOUT_SECONDS="${ELASTICSEARCH_INIT_TIMEOUT:-120}"
TEMP_DIR="$(mktemp -d -t incidentops-elasticsearch-init.XXXXXX)"
readonly TEMP_DIR
readonly TEMPLATE_RESPONSE="${TEMP_DIR}/template-response.json"
readonly MAPPINGS_RESPONSE="${TEMP_DIR}/mappings-response.json"

cleanup() {
  if [[ -z "${TEMP_DIR}" || "${TEMP_DIR}" == "/" || ! -d "${TEMP_DIR}" ]]; then
    return
  fi
  rm -rf -- "${TEMP_DIR}"
}

trap cleanup EXIT

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

if [[ ! -r "${TEMPLATE_FILE}" ]]; then
  error "Elasticsearch index template is missing: ${TEMPLATE_FILE}"
  exit 1
fi

log "Waiting for Elasticsearch at ${ELASTICSEARCH_URL}"
deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
until curl --fail --silent --show-error "${ELASTICSEARCH_URL}/_cluster/health" >/dev/null; do
  if (( SECONDS >= deadline )); then
    error "Timed out after ${WAIT_TIMEOUT_SECONDS}s waiting for Elasticsearch."
    exit 1
  fi
  sleep 2
done
success "Elasticsearch is available"

log "Creating or updating index template ${TEMPLATE_NAME}"
curl \
  --fail \
  --silent \
  --show-error \
  --request PUT \
  --header 'Content-Type: application/json' \
  --data-binary "@${TEMPLATE_FILE}" \
  "${ELASTICSEARCH_URL}/_index_template/${TEMPLATE_NAME}" >/dev/null
success "Index template ${TEMPLATE_NAME} is installed"

curl \
  --fail \
  --silent \
  --show-error \
  "${ELASTICSEARCH_URL}/_index_template/${TEMPLATE_NAME}" >"${TEMPLATE_RESPONSE}"

curl \
  --fail \
  --silent \
  --show-error \
  "${ELASTICSEARCH_URL}/incidentops-logs-*/_mapping?allow_no_indices=true" \
  >"${MAPPINGS_RESPONSE}"

uv run python -m incidentops.validation.cli validate-elasticsearch-mappings \
  --template "${TEMPLATE_FILE}" \
  --installed-template "${TEMPLATE_RESPONSE}" \
  --index-mappings "${MAPPINGS_RESPONSE}"

success "The template and existing IncidentOps log mappings are compatible"
printf '\nIncidentOps Elasticsearch initialization succeeded.\n'
