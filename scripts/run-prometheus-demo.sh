#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly DEMO_TOKEN="$(date +%s)-$$"
readonly RUN_ID="prometheus-demo-${DEMO_TOKEN}"
readonly DEMO_TOPIC="orders.prometheus-demo.${DEMO_TOKEN}"
readonly DEMO_GROUP="prometheus-demo-${DEMO_TOKEN}"
readonly EVENT_COUNT="${PROMETHEUS_DEMO_EVENT_COUNT:-120}"
readonly PRODUCER_RATE="${PROMETHEUS_DEMO_PRODUCER_RATE:-6}"
readonly WAIT_TIMEOUT_SECONDS="${PROMETHEUS_DEMO_TIMEOUT:-120}"
readonly TEMP_DIR="$(mktemp -d -t incidentops-prometheus-demo.XXXXXX)"
readonly PRODUCER_STDOUT="${TEMP_DIR}/producer.jsonl"
readonly CONSUMER_STDOUT="${TEMP_DIR}/consumer.jsonl"

producer_pid=""
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

# Keep Prometheus samples, but remove only Kafka and SQL resources created by this demo.
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

  if [[ "${group_created}" == "true" ]]; then
    log "Deleting temporary consumer group ${DEMO_GROUP}"
    if ! docker compose exec -T kafka \
      /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:29092 \
      --delete \
      --group "${DEMO_GROUP}" >/dev/null 2>&1; then
      error "Could not delete temporary consumer group ${DEMO_GROUP}."
      cleanup_failed=true
    fi
  fi

  if docker compose ps --status running --quiet postgres | grep -q .; then
    if ! docker compose exec -T postgres sh -c \
      "psql --set ON_ERROR_STOP=1 --username \"\$POSTGRES_USER\" --dbname \"\$POSTGRES_DB\" \
      --command \"DELETE FROM processed_orders WHERE customer_id LIKE '${RUN_ID}-%';\"" \
      >/dev/null 2>&1; then
      error "Could not delete this demo's PostgreSQL rows."
      cleanup_failed=true
    fi
  fi

  if [[ "${topic_created}" == "true" ]]; then
    log "Deleting temporary topic ${DEMO_TOPIC}"
    if ! docker compose exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:29092 \
      --delete \
      --topic "${DEMO_TOPIC}" >/dev/null 2>&1; then
      error "Could not delete temporary topic ${DEMO_TOPIC}."
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

if ! [[ "${EVENT_COUNT}" =~ ^[1-9][0-9]*$ ]]; then
  error "PROMETHEUS_DEMO_EVENT_COUNT must be a positive integer."
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  error "uv is required but was not found."
  exit 1
fi

log "Validating and starting the Docker Compose infrastructure"
docker compose config --quiet
docker compose up -d
./scripts/check-infrastructure.sh
./scripts/initialize-database.sh

log "Creating isolated Kafka topic ${DEMO_TOPIC}"
LOG_FILE_ENABLED=false uv run python -m incidentops.producer \
  --count 0 \
  --rate 1 \
  --seed 20260801 \
  --run-id "${RUN_ID}" \
  --topic "${DEMO_TOPIC}" \
  --no-metrics \
  >"${PRODUCER_STDOUT}" 2>&1
topic_created=true

log "Starting the WSL consumer metrics endpoint on port 8002"
LOG_FILE_ENABLED=false uv run python -m incidentops.consumer \
  --topic "${DEMO_TOPIC}" \
  --group "${DEMO_GROUP}" \
  --run-id "${RUN_ID}" \
  --max-messages "${EVENT_COUNT}" \
  --idle-timeout 60 \
  --metrics-port 8002 \
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
    exit 1
  fi
  sleep 1
done
success "Consumer received its partition assignment"

log "Starting ${EVENT_COUNT} events at ${PRODUCER_RATE} events/second on port 8001"
LOG_FILE_ENABLED=false uv run python -m incidentops.producer \
  --count "${EVENT_COUNT}" \
  --rate "${PRODUCER_RATE}" \
  --seed 20260801 \
  --run-id "${RUN_ID}" \
  --topic "${DEMO_TOPIC}" \
  --metrics-port 8001 \
  >"${PRODUCER_STDOUT}" 2>&1 &
producer_pid=$!

log "Waiting for both Prometheus targets to become healthy"
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
    error "The producer exited before both Prometheus targets became healthy."
    sed -n '1,200p' "${PRODUCER_STDOUT}" >&2
    exit 1
  fi
  if (( SECONDS >= targets_deadline )); then
    error "Timed out waiting for healthy Prometheus targets."
    exit 1
  fi
  sleep 1
done
success "Producer and consumer targets are UP"

printf '\nPrometheus is collecting this demo now:\n'
printf '  UI:      http://localhost:9090\n'
printf '  Targets: http://localhost:9090/targets\n'
printf '  Run ID:  %s\n\n' "${RUN_ID}"

process_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while kill -0 "${producer_pid}" 2>/dev/null || kill -0 "${consumer_pid}" 2>/dev/null; do
  if (( SECONDS >= process_deadline )); then
    error "Timed out waiting for the producer and consumer."
    exit 1
  fi
  sleep 1
done

if ! wait "${producer_pid}"; then
  error "The producer exited with an error."
  sed -n '1,200p' "${PRODUCER_STDOUT}" >&2
  exit 1
fi
producer_pid=""
if ! wait "${consumer_pid}"; then
  error "The consumer exited with an error."
  sed -n '1,240p' "${CONSUMER_STDOUT}" >&2
  exit 1
fi
consumer_pid=""
success "The producer delivered and the consumer processed all ${EVENT_COUNT} events"

printf '\nLag summary:\n'
uv run python -m incidentops.metric_query lag --minutes 10

printf '\nRate comparison:\n'
uv run python -m incidentops.metric_query rates --minutes 10

printf '\nP95 processing latency:\n'
uv run python -m incidentops.metric_query latency --percentile 0.95 --minutes 10

printf '\nPrometheus demo completed successfully.\n'
printf 'The application targets are expected to become DOWN after these short-lived processes stop.\n'
printf 'Recent samples remain available in Prometheus for up to six hours.\n'
