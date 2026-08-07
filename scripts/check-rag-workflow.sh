#!/usr/bin/env bash

# Purpose: deterministic end-to-end RAG validation with the scripted-test model.
# This script uses real infrastructure, metrics, logs, embeddings, and retrieval, but it never
# contacts an external LLM endpoint and does not require an API key. Use it for repeatable local
# validation and regression testing.

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly TEMP_DIR="$(mktemp -d -t incidentops-rag-workflow.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly BASELINE_REPORT="${TEMP_DIR}/baseline-report.json"
readonly RAG_REPORT="${TEMP_DIR}/rag-report.json"
readonly BASELINE_EVALUATION="${TEMP_DIR}/baseline-evaluation.json"
readonly RAG_EVALUATION="${TEMP_DIR}/rag-evaluation.json"
readonly RETRIEVAL_EVALUATION="${TEMP_DIR}/retrieval-evaluation.json"

scenario_run_id=""

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
      exit_status=1
    fi
  fi
  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting only retained log documents for run_id ${scenario_run_id}"
    if ! RUN_ID_TO_DELETE="${scenario_run_id}" uv run python - <<'PY'
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
      error "Could not remove the retained run-scoped log documents."
      exit_status=1
    fi
  fi
  rm -rf -- "${TEMP_DIR}"
  exit "${exit_status}"
}

trap cleanup EXIT
cd "${PROJECT_DIR}"

log "Validating Compose and required infrastructure"
docker compose config --quiet
./scripts/check-infrastructure.sh

log "Ingesting the complete controlled corpus with CPU embeddings"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge ingest --knowledge-directory knowledge

log "Running the ten-case hybrid retrieval benchmark"
EMBEDDING_PROVIDER=sentence-transformers \
  uv run incidentops-knowledge evaluate \
    --cases evaluation/retrieval_cases.yaml \
    --mode hybrid >"${RETRIEVAL_EVALUATION}"

uv run python - "${RETRIEVAL_EVALUATION}" <<'PY'
import json
import sys
from pathlib import Path

result = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
assert result["case_count"] == 10
assert result["recall_at_k"] >= 0.8
assert result["precision_at_k"] >= 0.3
assert result["mrr"] >= 0.8
assert result["excluded_document_count"] == 0
PY
success "Hybrid retrieval benchmark passed"

log "Running slow_consumer_v1 once and retaining only its bounded log window"
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

common_arguments=(
  investigate
  --description "Orders were delayed during the bounded scenario window."
  --start-time "${scenario_start}"
  --end-time "${scenario_end}"
  --run-id "${scenario_run_id}"
  --affected-service order-consumer
  --output-format json
  --model-provider scripted-test
)

log "Running the baseline graph against the retained live evidence"
KNOWLEDGE_ENABLED=false \
  uv run python -m incidentops.investigation.cli \
    "${common_arguments[@]}" \
    --output-file "${BASELINE_REPORT}"

log "Running the RAG graph against exactly the same retained live evidence"
KNOWLEDGE_ENABLED=true \
KNOWLEDGE_REQUIRED=true \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
EMBEDDING_PROVIDER=sentence-transformers \
  uv run python -m incidentops.investigation.cli \
    "${common_arguments[@]}" \
    --output-file "${RAG_REPORT}"

uv run python -m incidentops.evaluation.cli \
  --report "${BASELINE_REPORT}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${BASELINE_EVALUATION}"
uv run python -m incidentops.evaluation.cli \
  --report "${RAG_REPORT}" \
  --scenario scenarios/slow_consumer.yaml \
  --output-file "${RAG_EVALUATION}"

uv run python - \
  "${BASELINE_REPORT}" \
  "${RAG_REPORT}" \
  "${BASELINE_EVALUATION}" \
  "${RAG_EVALUATION}" <<'PY'
import json
import sys
from pathlib import Path

baseline = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
rag = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
baseline_evaluation = json.loads(Path(sys.argv[3]).read_text(encoding="utf-8"))
rag_evaluation = json.loads(Path(sys.argv[4]).read_text(encoding="utf-8"))

assert baseline["status"] == "diagnosed"
assert rag["status"] == "diagnosed"
assert baseline["primary_root_cause"]["cause_code"] == "slow_consumer_processing"
assert rag["primary_root_cause"]["cause_code"] == "slow_consumer_processing"
assert baseline["tool_call_count"] == rag["tool_call_count"] == 6
assert baseline["supporting_evidence"] == rag["supporting_evidence"]
assert baseline["negative_evidence"] == rag["negative_evidence"]
assert baseline["knowledge_references"] == []
assert baseline["knowledge_retrieval_count"] == 0
assert len(rag["knowledge_references"]) > 0
assert rag["knowledge_retrieval_count"] == 1
assert baseline_evaluation["root_cause_exact_match"] is True
assert rag_evaluation["root_cause_exact_match"] is True
assert baseline_evaluation["forbidden_action_count"] == 0
assert rag_evaluation["forbidden_action_count"] == 0
assert rag_evaluation["unsupported_knowledge_reference_count"] == 0

print("\nIncidentOps RAG workflow validation succeeded.")
print(f'Baseline root cause: {baseline["primary_root_cause"]["cause_code"]}')
print(f'RAG root cause: {rag["primary_root_cause"]["cause_code"]}')
print(f'Live tool calls in both runs: {rag["tool_call_count"]}')
print(f'Validated knowledge references: {len(rag["knowledge_references"])}')
PY
