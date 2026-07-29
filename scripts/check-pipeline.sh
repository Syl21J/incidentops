#!/usr/bin/env bash

set -Eeuo pipefail

# Resolve the repository and create identifiers unique to this validation run.
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEST_TOKEN="$(date +%s)-$$"
readonly RUN_ID="pipeline-check-${TEST_TOKEN}"
readonly TEST_TOPIC="orders.pipeline-check.${TEST_TOKEN}"
readonly TEST_GROUP="pipeline-check-${TEST_TOKEN}"
readonly EXPECTED_ROWS=20
readonly PRODUCED_MESSAGES=40
readonly WAIT_TIMEOUT_SECONDS="${PIPELINE_TIMEOUT:-120}"
readonly TEMP_DIR="$(mktemp -d -t incidentops-pipeline.XXXXXX)"
readonly CONSUMER_LOG="${TEMP_DIR}/consumer.log"

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

# Remove only the process, rows, topic, and temporary files created by this run.
cleanup() {
  local cleanup_failed=false

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

  rm -rf -- "${TEMP_DIR}"

  if [[ "${cleanup_failed}" == "true" ]]; then
    error "One or more scoped cleanup operations failed."
  fi
}

trap cleanup EXIT

cd "${PROJECT_DIR}"

if ! command -v uv >/dev/null 2>&1; then
  error "uv is required but was not found."
  exit 1
fi

log "Checking the existing infrastructure"
./scripts/check-infrastructure.sh

log "Applying the idempotent database migration"
./scripts/initialize-database.sh

# A zero-event producer call creates the isolated topic through application code.
log "Creating isolated Kafka topic ${TEST_TOPIC}"
LOG_FILE_ENABLED=false uv run python -m incidentops.producer \
  --count 0 \
  --rate 1 \
  --seed 20260729 \
  --run-id "${RUN_ID}" \
  --topic "${TEST_TOPIC}"
topic_created=true

# Start a bounded consumer and keep its JSON output for failure diagnostics.
log "Starting temporary consumer; log: ${CONSUMER_LOG}"
LOG_FILE_ENABLED=false uv run python -m incidentops.consumer \
  --topic "${TEST_TOPIC}" \
  --group "${TEST_GROUP}" \
  --run-id "${RUN_ID}" \
  --max-messages "${PRODUCED_MESSAGES}" \
  --idle-timeout 60 \
  >"${CONSUMER_LOG}" 2>&1 &
consumer_pid=$!
group_created=true

# Wait for an actual partition assignment instead of using an arbitrary sleep.
assignment_deadline=$((SECONDS + 30))
while ! grep -F '"event_type":"partitions_assigned"' "${CONSUMER_LOG}" >/dev/null 2>&1; do
  if ! kill -0 "${consumer_pid}" 2>/dev/null; then
    error "The consumer exited before receiving a partition assignment."
    sed -n '1,200p' "${CONSUMER_LOG}" >&2
    exit 1
  fi
  if (( SECONDS >= assignment_deadline )); then
    error "Timed out waiting for the consumer partition assignment."
    sed -n '1,200p' "${CONSUMER_LOG}" >&2
    exit 1
  fi
  sleep 1
done
success "Temporary consumer received its partition assignment"

# Send the same deterministic batch twice to exercise database idempotency.
for batch in 1 2; do
  log "Producing deterministic batch ${batch}/2"
  LOG_FILE_ENABLED=false uv run python -m incidentops.producer \
    --count "${EXPECTED_ROWS}" \
    --rate 100 \
    --seed 20260729 \
    --run-id "${RUN_ID}" \
    --topic "${TEST_TOPIC}"
done

# The consumer must process the bounded input and exit on its own.
consumer_deadline=$((SECONDS + WAIT_TIMEOUT_SECONDS))
while kill -0 "${consumer_pid}" 2>/dev/null; do
  if (( SECONDS >= consumer_deadline )); then
    error "Timed out waiting for the temporary consumer."
    sed -n '1,240p' "${CONSUMER_LOG}" >&2
    exit 1
  fi
  sleep 1
done

if ! wait "${consumer_pid}"; then
  error "The temporary consumer exited with an error."
  sed -n '1,240p' "${CONSUMER_LOG}" >&2
  exit 1
fi
consumer_pid=""
success "Temporary consumer stopped cleanly after ${PRODUCED_MESSAGES} messages"

# Count only rows carrying this run's unique customer prefix.
row_count="$(
  docker compose exec -T postgres sh -c \
    "psql --username \"\$POSTGRES_USER\" --dbname \"\$POSTGRES_DB\" \
    --tuples-only --no-align \
    --command \"SELECT COUNT(*) FROM processed_orders WHERE customer_id LIKE '${RUN_ID}-%';\""
)"
if [[ "${row_count}" != "${EXPECTED_ROWS}" ]]; then
  error "Expected ${EXPECTED_ROWS} rows, found ${row_count}."
  sed -n '1,240p' "${CONSUMER_LOG}" >&2
  exit 1
fi
success "PostgreSQL contains exactly ${EXPECTED_ROWS} rows from this run"

# A primary-key check plus the duplicate batch must still result in unique events.
duplicate_count="$(
  docker compose exec -T postgres sh -c \
    "psql --username \"\$POSTGRES_USER\" --dbname \"\$POSTGRES_DB\" \
    --tuples-only --no-align \
    --command \"SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM processed_orders \
    WHERE customer_id LIKE '${RUN_ID}-%';\""
)"
if [[ "${duplicate_count}" != "0" ]]; then
  error "Found ${duplicate_count} duplicate event IDs."
  exit 1
fi
success "Repeated events did not create duplicate database rows"

# Describe the isolated group after shutdown and sum lag across its partitions.
group_lag="$(
  docker compose exec -T kafka \
    /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:29092 \
    --describe \
    --group "${TEST_GROUP}" |
    awk -v group="${TEST_GROUP}" '$1 == group {lag += $6} END {print lag + 0}'
)"
if [[ "${group_lag}" != "0" ]]; then
  error "Consumer group lag is ${group_lag}, expected 0."
  exit 1
fi
success "Consumer group lag is zero"

printf '\nIncidentOps order pipeline validation succeeded.\n'
