#!/usr/bin/env bash

set -Eeuo pipefail

# Resolve the repository independently from the caller's current directory.
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
# Allow slower machines to override the health-check deadline.
readonly TIMEOUT_SECONDS="${INFRA_HEALTH_TIMEOUT:-180}"

test_topic=""
test_topic_created=false

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

# Delete only the temporary Kafka topic created by this script.
cleanup() {
  if [[ "${test_topic_created}" == "true" && -n "${test_topic}" ]]; then
    log "Deleting temporary topic ${test_topic}"
    if ! docker compose exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:29092 \
      --delete \
      --topic "${test_topic}" >/dev/null 2>&1; then
      error "Could not delete temporary topic ${test_topic}"
    fi
  fi
}

trap cleanup EXIT

cd "${PROJECT_DIR}"

# Confirm that both the Docker client and daemon are available.
if ! command -v docker >/dev/null 2>&1; then
  error "The docker command was not found."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  error "Docker is unavailable. Check Docker Desktop and WSL integration."
  exit 1
fi
success "Docker is available"

# Render Compose before using it so interpolation or syntax failures are explicit.
if ! docker compose config --quiet; then
  error "The Docker Compose configuration is invalid."
  exit 1
fi
success "The Docker Compose configuration is valid"

log "Compose service status:"
docker compose ps

# Poll Docker's real health status instead of relying on a fixed startup delay.
wait_for_healthy() {
  local service="$1"
  local container_id
  local status
  local started_at

  container_id="$(docker compose ps --quiet "${service}")"
  if [[ -z "${container_id}" ]]; then
    error "${service} has no running container."
    return 1
  fi

  started_at="${SECONDS}"
  while (( SECONDS - started_at < TIMEOUT_SECONDS )); do
    status="$(
      docker inspect \
        --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "${container_id}"
    )"

    case "${status}" in
      healthy)
        success "${service} is healthy"
        return 0
        ;;
      exited | dead)
        error "${service} is ${status}."
        docker compose logs --tail=100 "${service}" >&2
        return 1
        ;;
    esac

    sleep 2
  done

  error "Timed out after ${TIMEOUT_SECONDS}s waiting for ${service}."
  docker inspect --format '{{json .State.Health}}' "${container_id}" >&2
  docker compose logs --tail=100 "${service}" >&2
  return 1
}

for service in postgres elasticsearch kafka; do
  wait_for_healthy "${service}"
done

# Execute a real SQL query, not only PostgreSQL's process-level health check.
postgres_result="$(
  docker compose exec -T postgres sh -c \
    'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align --command "SELECT 1;"'
)"
if [[ "${postgres_result}" != "1" ]]; then
  error "The PostgreSQL query returned an unexpected value."
  exit 1
fi
success "PostgreSQL accepts a connection and answers SELECT 1"

# Resolve the configured host binding so custom .env ports are respected.
elasticsearch_binding="$(docker compose port elasticsearch 9200 | tail -n 1)"
if [[ -z "${elasticsearch_binding}" ]]; then
  error "Could not resolve the Elasticsearch host port."
  exit 1
fi
if ! curl --fail --silent --show-error "http://${elasticsearch_binding}/" >/dev/null; then
  error "Elasticsearch did not answer on http://${elasticsearch_binding}/."
  exit 1
fi
success "Elasticsearch answers on http://${elasticsearch_binding}/"

# A TCP connection from WSL verifies the externally published Kafka listener.
kafka_binding="$(docker compose port kafka 9092 | tail -n 1)"
if [[ -z "${kafka_binding}" ]]; then
  error "Could not resolve the Kafka host port."
  exit 1
fi
kafka_host="${kafka_binding%:*}"
kafka_port="${kafka_binding##*:}"
if ! timeout 5 bash -c "exec 3<>/dev/tcp/${kafka_host}/${kafka_port}"; then
  error "Kafka is not reachable from WSL on ${kafka_binding}."
  exit 1
fi
success "Kafka is reachable from WSL on ${kafka_binding}"

# Create and describe a unique topic to prove that the broker handles requests.
test_topic="incidentops-infra-check-$(date +%s)-$$"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --create \
  --topic "${test_topic}" \
  --partitions 1 \
  --replication-factor 1 >/dev/null
test_topic_created=true

if ! docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --describe \
  --topic "${test_topic}" | grep -F "Topic: ${test_topic}" >/dev/null; then
  error "Kafka did not return the temporary topic it created."
  exit 1
fi
success "Kafka can create and describe a temporary topic"

printf '\nIncidentOps infrastructure validation succeeded.\n'
