#!/usr/bin/env bash

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

python3 - \
  "${TEMPLATE_FILE}" \
  "${TEMPLATE_RESPONSE}" \
  "${MAPPINGS_RESPONSE}" <<'PY'
import json
import sys
from pathlib import Path

template_file, template_response_file, mappings_response_file = map(Path, sys.argv[1:])
expected_template = json.loads(template_file.read_text(encoding="utf-8"))
template_response = json.loads(template_response_file.read_text(encoding="utf-8"))
index_mappings = json.loads(mappings_response_file.read_text(encoding="utf-8"))

expected_properties = expected_template["template"]["mappings"]["properties"]
installed_properties = template_response["index_templates"][0]["index_template"]["template"][
    "mappings"
]["properties"]

expected_types = {
    field: definition["type"] for field, definition in expected_properties.items()
}
installed_types = {
    field: definition["type"] for field, definition in installed_properties.items()
}
if installed_types != expected_types:
    raise SystemExit("The installed index template mapping does not match the versioned mapping.")

for index_name, index_definition in index_mappings.items():
    properties = index_definition["mappings"].get("properties", {})
    actual_types = {
        field: properties.get(field, {}).get("type") for field in expected_types
    }
    mismatches = {
        field: (expected_type, actual_types[field])
        for field, expected_type in expected_types.items()
        if actual_types[field] != expected_type
    }
    if mismatches:
        raise SystemExit(
            f"Existing index {index_name} has an incompatible mapping: {mismatches}"
        )
PY

success "The template and existing IncidentOps log mappings are compatible"
printf '\nIncidentOps Elasticsearch initialization succeeded.\n'
