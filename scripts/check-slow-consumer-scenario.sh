#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
retain_investigation_data=false
output_metadata=""

usage() {
  cat <<'EOF'
Usage: check-slow-consumer-scenario.sh [OPTIONS]

Options:
  --retain-investigation-data  Keep this run's Elasticsearch documents for a follow-up workflow.
  --output-metadata PATH       Write exact scenario timestamps, identifiers, and observations as JSON.
  --help                       Show this help message.
EOF
}

while (( $# > 0 )); do
  case "$1" in
    --retain-investigation-data)
      retain_investigation_data=true
      shift
      ;;
    --output-metadata)
      if (( $# < 2 )); then
        printf '[ERROR] --output-metadata requires a path.\n' >&2
        exit 2
      fi
      output_metadata="$2"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      printf '[ERROR] Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "${retain_investigation_data}" == "true" && -z "${output_metadata}" ]]; then
  printf '[ERROR] --retain-investigation-data requires --output-metadata.\n' >&2
  exit 2
fi

readonly TEST_TOKEN="$(date +%s)-$$"
readonly RUN_ID="slow-consumer-${TEST_TOKEN}"
readonly TEST_TOPIC="orders.slow-consumer.${TEST_TOKEN}"
readonly TEST_GROUP="slow-consumer-${TEST_TOKEN}"
readonly EVENT_COUNT=80
readonly PRODUCER_RATE=10
readonly PROCESSING_DELAY_MS=800
readonly SLOW_THRESHOLD_MS=500
readonly MINIMUM_LAG=30
readonly MINIMUM_P95_SECONDS=0.7
readonly WAIT_TIMEOUT_SECONDS="${SLOW_CONSUMER_TIMEOUT:-120}"
readonly TEMP_DIR="$(mktemp -d -t incidentops-slow-consumer.XXXXXX)"
readonly PRODUCER_STDOUT="${TEMP_DIR}/producer.jsonl"
readonly CONSUMER_STDOUT="${TEMP_DIR}/consumer.jsonl"
readonly METRIC_RESULT="${TEMP_DIR}/metrics.json"
readonly LOG_RESULT="${TEMP_DIR}/logs.json"
readonly ERROR_LOG_RESULT="${TEMP_DIR}/error-logs.json"
readonly RUN_LOG_DIR="${PROJECT_DIR}/logs/${RUN_ID}"

producer_pid=""
consumer_pid=""
topic_created=false
group_created=false
scenario_start=""
scenario_end=""
investigation_data_ready=false

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

stop_process() {
  local pid="$1"
  local name="$2"

  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    log "Stopping temporary ${name} ${pid}"
    kill -TERM "${pid}"
    local deadline=$((SECONDS + 15))
    while kill -0 "${pid}" 2>/dev/null; do
      if (( SECONDS >= deadline )); then
        error "Temporary ${name} ${pid} did not stop within 15 seconds."
        return 1
      fi
      sleep 1
    done
    wait "${pid}" || return 1
  fi
}

# Delete only resources carrying this scenario run's unique token.
cleanup() {
  local exit_status=$?
  local cleanup_failed=false

  trap - EXIT

  if ! stop_process "${producer_pid}" "producer"; then
    cleanup_failed=true
  fi
  if ! stop_process "${consumer_pid}" "consumer"; then
    cleanup_failed=true
  fi
  producer_pid=""
  consumer_pid=""

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

  if [[ "${retain_investigation_data}" == "true" && "${investigation_data_ready}" == "true" ]]; then
    log "Retaining Elasticsearch documents for bounded follow-up investigation"
  elif ! RUN_ID_TO_DELETE="${RUN_ID}" uv run python - <<'PY'
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

log "Checking Prometheus health"
prometheus_container="$(docker compose ps --status running --quiet prometheus)"
if [[ -z "${prometheus_container}" ]]; then
  error "Prometheus has no running container."
  exit 1
fi
prometheus_health="$(
  docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "${prometheus_container}"
)"
if [[ "${prometheus_health}" != "healthy" ]]; then
  error "Prometheus is not healthy: ${prometheus_health}"
  docker compose logs --tail=100 prometheus >&2
  exit 1
fi
curl --fail --silent --show-error http://localhost:9090/-/healthy >/dev/null
success "Prometheus is healthy"

log "Initializing PostgreSQL and Elasticsearch idempotently"
./scripts/initialize-database.sh
./scripts/initialize-elasticsearch.sh

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
  exit 1
fi
docker compose exec -T filebeat \
  filebeat test output -c /usr/share/filebeat/filebeat.yml >/dev/null
success "Filebeat is healthy and can reach Elasticsearch"

uv run python - "${PROJECT_DIR}/scenarios/slow_consumer.yaml" <<'PY'
import sys
from pathlib import Path

from incidentops.scenarios import load_scenario_manifest

manifest = load_scenario_manifest(Path(sys.argv[1]))
if manifest.id != "slow_consumer_v1":
    raise SystemExit("Unexpected scenario manifest ID")
PY
success "The slow_consumer_v1 ground-truth manifest is valid"

log "Creating isolated Kafka topic ${TEST_TOPIC}"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.producer \
  --count 0 \
  --rate 1 \
  --seed 20260801 \
  --run-id "${RUN_ID}" \
  --topic "${TEST_TOPIC}" \
  --no-metrics \
  >"${PRODUCER_STDOUT}" 2>&1
topic_created=true

scenario_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

log "Starting delayed consumer (${PROCESSING_DELAY_MS} ms per event)"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.consumer \
  --topic "${TEST_TOPIC}" \
  --group "${TEST_GROUP}" \
  --run-id "${RUN_ID}" \
  --processing-delay-ms "${PROCESSING_DELAY_MS}" \
  --slow-processing-threshold-ms "${SLOW_THRESHOLD_MS}" \
  --lag-update-interval-seconds 1 \
  --metrics-port 8002 \
  >"${CONSUMER_STDOUT}" 2>&1 &
consumer_pid=$!
group_created=true

assignment_deadline=$((SECONDS + 30))
while ! grep -F '"event_type":"partitions_assigned"' "${CONSUMER_STDOUT}" >/dev/null 2>&1; do
  if ! kill -0 "${consumer_pid}" 2>/dev/null; then
    error "The consumer exited before partition assignment."
    sed -n '1,240p' "${CONSUMER_STDOUT}" >&2
    exit 1
  fi
  if (( SECONDS >= assignment_deadline )); then
    error "Timed out waiting for consumer partition assignment."
    exit 1
  fi
  sleep 1
done
success "Delayed consumer received its partition assignment"

log "Starting producer (${EVENT_COUNT} events at ${PRODUCER_RATE}/s)"
LOG_DIRECTORY="${RUN_LOG_DIR}" uv run python -m incidentops.producer \
  --count "${EVENT_COUNT}" \
  --rate "${PRODUCER_RATE}" \
  --seed 20260801 \
  --run-id "${RUN_ID}" \
  --topic "${TEST_TOPIC}" \
  --metrics-port 8001 \
  >"${PRODUCER_STDOUT}" 2>&1 &
producer_pid=$!

log "Waiting for Prometheus to report both WSL targets as healthy"
targets_deadline=$((SECONDS + 30))
while ! curl --fail --silent http://localhost:9090/api/v1/targets | uv run python -c '
import json, sys
payload = json.load(sys.stdin)
healthy = {
    target.get("labels", {}).get("job")
    for target in payload["data"]["activeTargets"]
    if target.get("health") == "up"
}
raise SystemExit(0 if {"incidentops-producer", "incidentops-consumer"} <= healthy else 1)
'; do
  if ! kill -0 "${producer_pid}" 2>/dev/null; then
    error "Producer exited before both Prometheus targets became healthy."
    sed -n '1,240p' "${PRODUCER_STDOUT}" >&2
    exit 1
  fi
  if (( SECONDS >= targets_deadline )); then
    error "Timed out waiting for healthy Prometheus application targets."
    curl --silent http://localhost:9090/api/v1/targets >&2
    exit 1
  fi
  sleep 1
done
success "Prometheus reports producer and consumer targets as healthy"

docker compose exec -T prometheus \
  wget -qO- http://host.docker.internal:8001/metrics | \
  grep -F 'incidentops_orders_produced_total' >/dev/null
docker compose exec -T prometheus \
  wget -qO- http://host.docker.internal:8002/metrics | \
  grep -F 'incidentops_kafka_consumer_lag' >/dev/null
success "Both WSL metrics endpoints are reachable from inside Prometheus"

producer_deadline=$((SECONDS + 30))
while kill -0 "${producer_pid}" 2>/dev/null; do
  if (( SECONDS >= producer_deadline )); then
    error "Timed out waiting for the deterministic producer batch."
    exit 1
  fi
  sleep 1
done
if ! wait "${producer_pid}"; then
  error "The producer exited with an error."
  sed -n '1,240p' "${PRODUCER_STDOUT}" >&2
  exit 1
fi
producer_pid=""
success "Producer delivered all ${EVENT_COUNT} events"

log "Polling bounded metric summaries until the incident evidence is complete"
metric_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while true; do
  if SCENARIO_START="${scenario_start}" uv run python - <<'PY' >"${METRIC_RESULT}" 2>/dev/null
import os
from datetime import UTC, datetime

from incidentops.metric_query import (
    PrometheusClient,
    compare_production_and_processing_rates,
    get_consumer_lag_summary,
    get_processing_latency_summary,
)

start = datetime.fromisoformat(os.environ["SCENARIO_START"].replace("Z", "+00:00"))
end = datetime.now(UTC)
client = PrometheusClient("http://localhost:9090")
lag = get_consumer_lag_summary(client, start=start, end=end, step_seconds=2)
latency = get_processing_latency_summary(
    client,
    percentile=0.95,
    start=start,
    end=end,
    step_seconds=2,
)
rates = compare_production_and_processing_rates(client, start=start, end=end, step_seconds=2)
print(
    "{" +
    f'"maximum_lag":{lag.maximum},' +
    f'"lag_start":{lag.start_value},' +
    f'"lag_end":{lag.end_value},' +
    f'"lag_trend":"{lag.trend}",' +
    f'"lag_samples":{lag.sample_count},' +
    f'"p95_seconds":{latency.duration_seconds},' +
    f'"latency_samples":{latency.sample_count},' +
    f'"producer_rate":{rates.producer_rate},' +
    f'"consumer_rate":{rates.consumer_rate},' +
    f'"consumer_is_slower":{str(rates.consumer_is_slower).lower()}' +
    "}"
)
PY
  then
    if uv run python - "${METRIC_RESULT}" "${MINIMUM_LAG}" "${MINIMUM_P95_SECONDS}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
minimum_lag = float(sys.argv[2])
minimum_p95 = float(sys.argv[3])
valid = (
    result["maximum_lag"] >= minimum_lag
    and result["lag_end"] > result["lag_start"]
    and result["lag_trend"] == "increasing"
    and result["lag_samples"] >= 4
    and result["p95_seconds"] >= minimum_p95
    and result["latency_samples"] >= 2
    and result["consumer_is_slower"]
    and result["producer_rate"] > result["consumer_rate"]
)
raise SystemExit(0 if valid else 1)
PY
    then
      break
    fi
  fi
  if (( SECONDS >= metric_deadline )); then
    error "Timed out waiting for lag, latency, and rate evidence."
    if [[ -f "${METRIC_RESULT}" ]]; then
      sed -n '1,240p' "${METRIC_RESULT}" >&2
    fi
    sed -n '1,240p' "${CONSUMER_STDOUT}" >&2
    exit 1
  fi
  if ! kill -0 "${consumer_pid}" 2>/dev/null; then
    error "The consumer exited before scenario evidence was complete."
    exit 1
  fi
  sleep 2
done
success "Lag, P95 latency, and throughput assertions passed"

processed_during_incident="$(
  awk '/"event_type":"order_processed"/ { count++ } END { print count + 0 }' \
    "${CONSUMER_STDOUT}"
)"

log "Waiting for structured scenario logs in Elasticsearch"
log_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while true; do
  if uv run python -m incidentops.log_search search \
    --run-id "${RUN_ID}" \
    --service order-consumer \
    --event-type slow_processing \
    --minutes 10 \
    --limit 500 >"${LOG_RESULT}"; then
    slow_log_count="$(uv run python - "${LOG_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["total"])
PY
)"
    if (( slow_log_count > 0 )); then
      break
    fi
  fi
  if (( SECONDS >= log_deadline )); then
    error "Timed out waiting for slow_processing logs in Elasticsearch."
    exit 1
  fi
  sleep 2
done
success "Elasticsearch contains ${slow_log_count} slow_processing logs"

uv run python -m incidentops.log_search search \
  --run-id "${RUN_ID}" \
  --event-type database_connection_failed \
  --event-type database_write_failed \
  --event-type consumer_error \
  --event-type producer_error \
  --event-type delivery_failed \
  --event-type topic_error \
  --minutes 10 \
  --limit 500 >"${ERROR_LOG_RESULT}"

read -r database_error_count kafka_error_count < <(
  uv run python - "${ERROR_LOG_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

logs = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["logs"]
database_events = {"database_connection_failed", "database_write_failed"}
kafka_events = {"consumer_error", "producer_error", "delivery_failed", "topic_error"}
print(
    sum(item["event_type"] in database_events for item in logs),
    sum(item["event_type"] in kafka_events for item in logs),
)
PY
)
if [[ "${database_error_count}" != "0" || "${kafka_error_count}" != "0" ]]; then
  error "Unexpected database or Kafka error evidence was indexed."
  exit 1
fi
success "No database or Kafka broker error logs were introduced"

log "Stopping the delayed consumer without waiting for an unbounded catch-up"
stop_process "${consumer_pid}" "consumer"
consumer_pid=""

if pgrep -f "incidentops\.(producer|consumer).*${RUN_ID}" >/dev/null 2>&1; then
  error "A scenario producer or consumer process is still running."
  exit 1
fi
success "All temporary application processes are stopped"
scenario_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

read -r maximum_lag p95_seconds producer_rate consumer_rate < <(
  uv run python - "${METRIC_RESULT}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(
    result["maximum_lag"],
    result["p95_seconds"],
    result["producer_rate"],
    result["consumer_rate"],
)
PY
)

if [[ -n "${output_metadata}" ]]; then
  uv run python - \
    "${output_metadata}" \
    "${METRIC_RESULT}" \
    "${RUN_ID}" \
    "${TEST_TOPIC}" \
    "${TEST_GROUP}" \
    "${scenario_start}" \
    "${scenario_end}" \
    "${slow_log_count}" \
    "${database_error_count}" \
    "${kafka_error_count}" <<'PY'
import json
import sys
import tempfile
from pathlib import Path

output_path = Path(sys.argv[1])
metric_path = Path(sys.argv[2])
payload = {
    "schema_version": 1,
    "scenario_id": "slow_consumer_v1",
    "run_id": sys.argv[3],
    "topic": sys.argv[4],
    "consumer_group": sys.argv[5],
    "start_time": sys.argv[6],
    "end_time": sys.argv[7],
    "observations": {
        **json.loads(metric_path.read_text(encoding="utf-8")),
        "slow_processing_log_count": int(sys.argv[8]),
        "database_error_count": int(sys.argv[9]),
        "kafka_error_count": int(sys.argv[10]),
    },
}
output_path.parent.mkdir(parents=True, exist_ok=True)
with tempfile.NamedTemporaryFile(
    mode="w",
    encoding="utf-8",
    dir=output_path.parent,
    prefix=f".{output_path.name}.",
    delete=False,
) as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
    temporary_path = Path(handle.name)
temporary_path.replace(output_path)
PY
  investigation_data_ready=true
  success "Scenario metadata written to ${output_metadata}"
fi

printf '\nIncidentOps slow-consumer scenario validation succeeded.\n'
printf 'Run ID: %s\n' "${RUN_ID}"
printf 'Produced events: %s\n' "${EVENT_COUNT}"
printf 'Processed events during incident: %s\n' "${processed_during_incident}"
printf 'Maximum consumer lag: %s\n' "${maximum_lag}"
printf 'P95 processing duration (seconds): %s\n' "${p95_seconds}"
printf 'Producer rate (events/second): %s\n' "${producer_rate}"
printf 'Consumer rate (events/second): %s\n' "${consumer_rate}"
printf 'Slow-processing log count: %s\n' "${slow_log_count}"
printf 'Database error count: %s\n' "${database_error_count}"
printf 'Kafka error count: %s\n' "${kafka_error_count}"
printf 'Catch-up: skipped; the consumer is stopped within the bounded incident window.\n'
