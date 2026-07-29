#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEST_TOKEN="$(date +%s)-$$"
readonly RUN_ID="log-pipeline-check-${TEST_TOKEN}"
readonly TEST_TOPIC="orders.log-pipeline-check.${TEST_TOKEN}"
readonly TEST_GROUP="log-pipeline-check-${TEST_TOKEN}"
readonly MESSAGE_COUNT=6
readonly WAIT_TIMEOUT_SECONDS="${LOG_PIPELINE_TIMEOUT:-120}"
readonly TEMP_DIR="$(mktemp -d -t incidentops-log-pipeline.XXXXXX)"
readonly CONSUMER_STDOUT="${TEMP_DIR}/consumer.jsonl"
readonly SEARCH_RESULT="${TEMP_DIR}/search-result.json"
readonly AGGREGATION_RESULT="${TEMP_DIR}/aggregation-result.json"
readonly RUN_LOG_DIR="${PROJECT_DIR}/logs/${RUN_ID}"

consumer_pid=""
topic_created=false
group_created=false

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

# Remove only resources identified by this run's unique token.
cleanup() {
  local exit_status=$?
  local cleanup_failed=false

  trap - EXIT

  if [[ -n "${consumer_pid}" ]] && kill -0 "${consumer_pid}" 2>/dev/null; then
    log "Stopping temporary consumer ${consumer_pid}"
    kill -TERM "${consumer_pid}"
    if ! wait "${consumer_pid}"; then
      error "The temporary consumer did not stop cleanly during cleanup."
      cleanup_failed=true
    fi
  fi

  if [[ "${group_created}" == "true" ]]; then
    log "Deleting temporary consumer group ${TEST_GROUP}"
    if ! docker compose exec -T kafka \
      /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:29092 \
      --delete \
      --group "${TEST_GROUP}" >/dev/null 2>&1; then
      error "Could not delete temporary consumer group ${TEST_GROUP}."
      cleanup_failed=true
    fi
  fi

  if docker compose ps --status running --quiet postgres | grep -q .; then
    if ! docker compose exec -T postgres sh -c \
      "psql --set ON_ERROR_STOP=1 --username \"\$POSTGRES_USER\" --dbname \"\$POSTGRES_DB\" \
      --command \"DELETE FROM processed_orders WHERE customer_id LIKE '${RUN_ID}-%';\"" \
      >/dev/null 2>&1; then
      error "Could not delete this run's PostgreSQL rows."
      cleanup_failed=true
    fi
  fi

  if [[ "${topic_created}" == "true" ]]; then
    log "Deleting temporary topic ${TEST_TOPIC}"
    if ! docker compose exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:29092 \
      --delete \
      --topic "${TEST_TOPIC}" >/dev/null 2>&1; then
      error "Could not delete temporary topic ${TEST_TOPIC}."
      cleanup_failed=true
    fi
  fi

  if [[ -d "${RUN_LOG_DIR}" ]]; then
    log "Deleting isolated JSONL directory ${RUN_LOG_DIR}"
    rm -rf -- "${RUN_LOG_DIR}"
  fi

  if ! RUN_ID_TO_DELETE="${RUN_ID}" uv run python - <<'PY'
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
    error "Could not delete Elasticsearch documents for run_id ${RUN_ID}."
    cleanup_failed=true
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

log "Checking the existing infrastructure"
./scripts/check-infrastructure.sh

log "Installing and verifying the Elasticsearch index template"
./scripts/initialize-elasticsearch.sh

log "Applying the idempotent database migration"
./scripts/initialize-database.sh

filebeat_container="$(docker compose ps --status running --quiet filebeat)"
if [[ -z "${filebeat_container}" ]]; then
  error "Filebeat has no running container."
  exit 1
fi
filebeat_health="$(
  docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "${filebeat_container}"
)"
if [[ "${filebeat_health}" != "healthy" ]]; then
  error "Filebeat is not healthy: ${filebeat_health}"
  docker compose logs --tail=100 filebeat >&2
  exit 1
fi
docker compose exec -T filebeat \
  filebeat test output -c /usr/share/filebeat/filebeat.yml >/dev/null
success "Filebeat is healthy and can reach Elasticsearch"

log "Creating isolated Kafka topic ${TEST_TOPIC}"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.producer \
  --count 0 \
  --rate 1 \
  --seed 20260729 \
  --run-id "${RUN_ID}" \
  --topic "${TEST_TOPIC}"
topic_created=true

log "Starting temporary consumer; stdout: ${CONSUMER_STDOUT}"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.consumer \
  --topic "${TEST_TOPIC}" \
  --group "${TEST_GROUP}" \
  --run-id "${RUN_ID}" \
  --max-messages "${MESSAGE_COUNT}" \
  --idle-timeout 60 \
  >"${CONSUMER_STDOUT}" 2>&1 &
consumer_pid=$!
group_created=true

assignment_deadline=$((SECONDS + 30))
while ! grep -F '"event_type":"partitions_assigned"' "${CONSUMER_STDOUT}" >/dev/null 2>&1; do
  if ! kill -0 "${consumer_pid}" 2>/dev/null; then
    error "The consumer exited before receiving a partition assignment."
    sed -n '1,200p' "${CONSUMER_STDOUT}" >&2
    exit 1
  fi
  if (( SECONDS >= assignment_deadline )); then
    error "Timed out waiting for the consumer partition assignment."
    sed -n '1,200p' "${CONSUMER_STDOUT}" >&2
    exit 1
  fi
  sleep 1
done
success "Temporary consumer received its partition assignment"

log "Producing ${MESSAGE_COUNT} deterministic order events"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.producer \
  --count "${MESSAGE_COUNT}" \
  --rate 100 \
  --seed 20260729 \
  --run-id "${RUN_ID}" \
  --topic "${TEST_TOPIC}"

consumer_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while kill -0 "${consumer_pid}" 2>/dev/null; do
  if (( SECONDS >= consumer_deadline )); then
    error "Timed out waiting for the temporary consumer."
    sed -n '1,240p' "${CONSUMER_STDOUT}" >&2
    exit 1
  fi
  sleep 1
done
if ! wait "${consumer_pid}"; then
  error "The temporary consumer exited with an error."
  sed -n '1,240p' "${CONSUMER_STDOUT}" >&2
  exit 1
fi
consumer_pid=""
success "Temporary consumer processed ${MESSAGE_COUNT} messages"

log "Waiting for Filebeat to index both services under run_id ${RUN_ID}"
index_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while true; do
  if uv run python -m incidentops.log_search search \
    --run-id "${RUN_ID}" \
    --minutes 10 \
    --limit 100 >"${SEARCH_RESULT}"; then
    if python3 - "${SEARCH_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
services = {entry["service"] for entry in result["logs"]}
raise SystemExit(0 if {"order-producer", "order-consumer"} <= services else 1)
PY
    then
      break
    fi
  fi
  if (( SECONDS >= index_deadline )); then
    error "Timed out waiting for producer and consumer logs in Elasticsearch."
    sed -n '1,240p' "${SEARCH_RESULT}" >&2
    docker compose logs --tail=100 filebeat >&2
    exit 1
  fi
  sleep 1
done
success "Elasticsearch contains producer and consumer logs for this run"

python3 - "${SEARCH_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
if not any(entry.get("event_id") or entry.get("order_id") for entry in result["logs"]):
    raise SystemExit("No indexed log contains an event_id or order_id.")
PY
success "At least one indexed log carries an event_id or order_id"

uv run python -m incidentops.log_search aggregate \
  --group-by service \
  --run-id "${RUN_ID}" \
  --minutes 10 >"${AGGREGATION_RESULT}"

python3 - "${AGGREGATION_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
counts = {bucket["key"]: bucket["count"] for bucket in result["buckets"]}
if not {"order-producer", "order-consumer"} <= counts.keys():
    raise SystemExit("The service aggregation is missing an application service.")
print(
    "[INFO] Indexed service counts: "
    f"order-producer={counts['order-producer']}, "
    f"order-consumer={counts['order-consumer']}"
)
PY
success "Service aggregation contains both applications"

indexed_count="$(
  python3 - "${SEARCH_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(result["total"])
PY
)"

printf '\nIncidentOps log pipeline validation succeeded.\n'
printf 'Run ID: %s\n' "${RUN_ID}"
printf 'Messages processed: %s\n' "${MESSAGE_COUNT}"
printf 'Indexed logs found: %s\n' "${indexed_count}"
