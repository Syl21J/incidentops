#!/usr/bin/env bash

set -Eeuo pipefail

# Resolve paths so the migration works from any current directory.
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly MIGRATION_FILE="${PROJECT_DIR}/sql/001_create_processed_orders.sql"

cd "${PROJECT_DIR}"

if ! docker info >/dev/null 2>&1; then
  printf '[ERROR] Docker is unavailable.\n' >&2
  exit 1
fi

if [[ ! -r "${MIGRATION_FILE}" ]]; then
  printf '[ERROR] Migration file is missing: %s\n' "${MIGRATION_FILE}" >&2
  exit 1
fi

# Use the credentials already present inside the PostgreSQL container.
if ! docker compose exec -T postgres sh -c \
  'pg_isready --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' >/dev/null; then
  printf '[ERROR] PostgreSQL is not ready.\n' >&2
  exit 1
fi

# ON_ERROR_STOP prevents a partial migration from being reported as successful.
docker compose exec -T postgres sh -c \
  'psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB"' \
  <"${MIGRATION_FILE}"

printf '[OK] Database migration applied successfully.\n'
