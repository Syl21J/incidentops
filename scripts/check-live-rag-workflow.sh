#!/usr/bin/env bash

# Purpose: end-to-end RAG validation with the configured live OpenAI-compatible model.
# This script uses real infrastructure and may make up to four billable model calls. It requires
# valid LLM_MODEL and LLM_API_KEY settings, and the model must support strict structured output.
# Use it only when explicitly validating real-model behavior after the deterministic check passes.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_DIR="$(mktemp -d -t incidentops-live-rag-workflow.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly REPORT_FILE="${TEMP_DIR}/live-rag-report.json"

scenario_run_id=""
evaluation_file="${TEMP_DIR}/live-rag-evaluation.json"

log() {
  printf '[INFO] %s\n' "$*"
}

success() {
  printf '[OK]   %s\n' "$*"
}

error() {
  printf '[ERROR] %s\n' "$*" >&2
}

cleanup() {
  local exit_status=$?
  local cleanup_failed=false
  local recovered_run_id=""

  trap - EXIT
  if [[ -z "${scenario_run_id}" && -f "${METADATA_FILE}" ]]; then
    if recovered_run_id="$(uv run python - "${METADATA_FILE}" <<'PY'
import json
import sys
from pathlib import Path

print(json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))["run_id"])
PY
)"; then
      scenario_run_id="${recovered_run_id}"
    else
      error "Could not recover the retained scenario run identifier."
      cleanup_failed=true
    fi
  fi

  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting only retained log documents for run_id ${scenario_run_id}"
    if ! RUN_ID_TO_DELETE="${scenario_run_id}" uv run python - <<'PY'
import os

from elasticsearch import Elasticsearch

from incidentops.config import Settings

client = Elasticsearch(Settings().elasticsearch_url, request_timeout=10)
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
      error "Could not remove the retained run-scoped log documents."
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

if ! command -v uv >/dev/null 2>&1; then
  error "uv is required but was not found."
  exit 1
fi

log "Validating the live model configuration without exposing credentials"
LLM_PROVIDER=openai-compatible uv run python - <<'PY'
import sys

from pydantic import ValidationError

from incidentops.config import Settings
from incidentops.investigation.model import ModelConfigurationError, OpenAICompatibleModelProvider

try:
    settings = Settings(llm_provider="openai-compatible")
    OpenAICompatibleModelProvider(settings)
except (ModelConfigurationError, ValidationError) as error:
    print(f"[ERROR] {error}", file=sys.stderr)
    raise SystemExit(2) from error
print(f"[OK]   Live model configuration is valid for model={settings.llm_model}")
PY

log "Validating Compose and required infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Ingesting the controlled corpus with production CPU embeddings"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge ingest --knowledge-directory knowledge

log "Running slow_consumer_v1 once and retaining its bounded log window"
./scripts/check-slow-consumer-scenario.sh \
  --retain-investigation-data \
  --output-metadata "${METADATA_FILE}"

read -r scenario_run_id scenario_start scenario_end < <(
  uv run python - "${METADATA_FILE}" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(metadata["run_id"], metadata["start_time"], metadata["end_time"])
PY
)

if [[ -z "${scenario_run_id}" || -z "${scenario_start}" || -z "${scenario_end}" ]]; then
  error "Scenario metadata is incomplete."
  exit 1
fi
success "Captured exact scenario window ${scenario_start} to ${scenario_end}"

artifact_directory="$(uv run python -c '
from incidentops.config import Settings
print(Settings().investigation_artifact_directory)
')"
evaluation_file="${artifact_directory}/${scenario_run_id}.live-rag.evaluation.json"

log "Executing LangGraph with the live model and required hybrid retrieval"
LLM_PROVIDER=openai-compatible \
KNOWLEDGE_ENABLED=true \
KNOWLEDGE_REQUIRED=true \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
EMBEDDING_PROVIDER=sentence-transformers \
  uv run python -m incidentops.investigation.cli investigate \
    --description "Orders were delayed during the bounded scenario window." \
    --start-time "${scenario_start}" \
    --end-time "${scenario_end}" \
    --run-id "${scenario_run_id}" \
    --affected-service order-consumer \
    --output-format json \
    --output-file "${REPORT_FILE}" \
    --model-provider openai-compatible \
    --persist-artifacts
success "The live model produced a locally validated structured report"

log "Evaluating the live report outside the investigation graph"
uv run python -m incidentops.evaluation.cli \
  --report "${REPORT_FILE}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${evaluation_file}"

uv run python - \
  "${REPORT_FILE}" \
  "${evaluation_file}" \
  "${artifact_directory}" <<'PY'
import json
import sys
from pathlib import Path

report = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
evaluation = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
artifact_directory = Path(sys.argv[3])

assert report["status"] == "diagnosed"
assert report["primary_root_cause"]["cause_code"] == "slow_consumer_processing"
assert report["knowledge_retrieval_count"] in {1, 2}
assert len(report["knowledge_references"]) > 0
assert evaluation["root_cause_exact_match"] is True
assert evaluation["root_cause_rank"] == 1
assert evaluation["expected_metric_evidence_recall"] == 1.0
assert evaluation["expected_log_evidence_recall"] == 1.0
assert evaluation["negative_evidence_recall"] == 1.0
assert evaluation["unsupported_evidence_reference_count"] == 0
assert evaluation["unsupported_knowledge_reference_count"] == 0
assert evaluation["forbidden_action_count"] == 0
assert evaluation["tool_call_count"] <= 10
assert evaluation["investigation_attempt_count"] <= 2
assert report["model_call_count"] <= 4

report_artifact = artifact_directory / f'{report["investigation_id"]}.report.json'
trace_artifact = artifact_directory / f'{report["investigation_id"]}.trace.jsonl'
assert report_artifact.is_file()
assert trace_artifact.is_file()

print("\nIncidentOps live-model RAG validation succeeded.")
print(f'Root cause: {report["primary_root_cause"]["cause_code"]}')
print(f'Model calls: {report["model_call_count"]}/4')
print(f'Tool calls: {report["tool_call_count"]}/10')
print(f'Investigation attempts: {report["investigation_attempts"]}/2')
print(f'Knowledge references: {len(report["knowledge_references"])}')
print(f'Persisted report: {report_artifact}')
print(f'Persisted trace: {trace_artifact}')
print(f'Persisted evaluation: {Path(sys.argv[2])}')
PY
