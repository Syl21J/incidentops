#!/usr/bin/env bash

# Validate the single-incident workflow with baseline, deterministic RAG, or a live model.
# All modes reuse the common scenario runner and delete only their retained run's logs.

set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: check-investigation.sh [baseline|rag|live]

Prepare the local environment and validate one bounded slow-consumer investigation.

Modes:
  baseline  Validate the scripted model, evidence, evaluation, and persisted trace (default).
  rag       Also validate hybrid retrieval and compare baseline/RAG over the same window.
  live      Validate the configured OpenAI-compatible model with required hybrid retrieval.
            This explicit mode may incur API costs; it is never part of deterministic checks.
  --help    Show this help without starting services or making model calls.
EOF
}

if (( $# > 1 )); then
  usage >&2
  exit 2
fi
mode="${1:-baseline}"
case "${mode}" in
  baseline|rag|live) ;;
  --help|-h) usage; exit 0 ;;
  *) printf '[ERROR] Unknown mode: %s\n' "${mode}" >&2; usage >&2; exit 2 ;;
esac

source "$(dirname -- "${BASH_SOURCE[0]}")/lib/workflow.sh"

live_model=false
model_provider=scripted-test
if [[ "${mode}" == "live" ]]; then
  live_model=true
  model_provider=openai-compatible
fi
rag_dependencies=false
if [[ "${mode}" != "baseline" ]]; then
  rag_dependencies=true
fi
prepare_environment "${live_model}" "${rag_dependencies}"

readonly TEMP_DIR="$(mktemp -d -t incidentops-investigation.XXXXXX)"
readonly METADATA_FILE="${TEMP_DIR}/scenario-metadata.json"
readonly BASELINE_REPORT="${TEMP_DIR}/baseline-report.json"
readonly RAG_REPORT="${TEMP_DIR}/rag-report.json"
scenario_run_id=""

cleanup() {
  local exit_status=$?
  local recovered_run_id=""
  trap - EXIT
  if [[ -z "${scenario_run_id}" && -f "${METADATA_FILE}" ]]; then
    if recovered_run_id="$(
      uv run python -m incidentops.validation.cli metadata-run-id "${METADATA_FILE}"
    )"; then
      scenario_run_id="${recovered_run_id}"
    else
      error "Could not recover the retained scenario run identifier."
      exit_status=1
    fi
  fi
  if [[ -n "${scenario_run_id}" ]]; then
    log "Deleting only retained log documents for run_id ${scenario_run_id}"
    if ! uv run python -m incidentops.validation.cli delete-run-logs "${scenario_run_id}"
    then
      error "Could not remove the retained run-scoped log documents."
      exit_status=1
    fi
  fi
  rm -rf -- "${TEMP_DIR}"
  exit "${exit_status}"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "${mode}" != "baseline" ]]; then
  ingest_knowledge
fi
if [[ "${mode}" == "rag" ]]; then
  log "Running the ten-case hybrid retrieval benchmark"
  EMBEDDING_PROVIDER=sentence-transformers \
    uv run incidentops-knowledge evaluate \
      --cases evaluation/retrieval_cases.yaml \
      --mode hybrid >"${TEMP_DIR}/retrieval-evaluation.json"
  uv run python -m incidentops.validation.cli \
    validate-retrieval-benchmark "${TEMP_DIR}/retrieval-evaluation.json"
fi

log "Running slow_consumer_v1 once and retaining its bounded log window"
uv run incidentops-scenario run \
  --scenario slow_consumer_v1 \
  --retain-evidence \
  --output-metadata "${METADATA_FILE}"

scenario_window="$(uv run python -m incidentops.validation.cli scenario-window "${METADATA_FILE}")"
read -r scenario_run_id scenario_start scenario_end <<< "${scenario_window}"
if [[ -z "${scenario_run_id}" || -z "${scenario_start}" || -z "${scenario_end}" ]]; then
  error "Scenario metadata is incomplete."
  exit 1
fi
log "Waiting for retained Filebeat evidence to settle"
uv run python -m incidentops.validation.cli wait-run-logs-stable "${scenario_run_id}"

artifact_directory="$(uv run python -m incidentops.validation.cli artifact-directory)"
baseline_evaluation="${artifact_directory}/${scenario_run_id}.baseline.evaluation.json"
rag_evaluation="${artifact_directory}/${scenario_run_id}.${mode}.evaluation.json"

# Only neutral incident details cross the investigation boundary.
common_arguments=(
  investigate
  --description "Orders were delayed during the bounded scenario window."
  --start-time "${scenario_start}"
  --end-time "${scenario_end}"
  --run-id "${scenario_run_id}"
  --affected-service order-consumer
  --output-format json
  --model-provider "${model_provider}"
  --persist-artifacts
)

investigate() {
  local knowledge_enabled="$1"
  local report_file="$2"
  # Pin both flags so .env cannot enable knowledge in a baseline or disable required RAG.
  KNOWLEDGE_ENABLED="${knowledge_enabled}" \
  KNOWLEDGE_REQUIRED="${knowledge_enabled}" \
  KNOWLEDGE_RETRIEVAL_MODE=hybrid \
  EMBEDDING_PROVIDER=sentence-transformers \
    uv run python -m incidentops.investigation.cli \
      "${common_arguments[@]}" --output-file "${report_file}"
}

evaluate() {
  uv run python -m incidentops.evaluation.cli \
    --report "$1" \
    --scenario scenarios/slow_consumer.yaml \
    --output-file "$2"
}

investigate_and_evaluate() {
  local knowledge_enabled="$1"
  local report_file="$2"
  local evaluation_file="$3"
  local investigation_exit=0

  if investigate "${knowledge_enabled}" "${report_file}"; then
    :
  else
    investigation_exit=$?
  fi
  if [[ ! -f "${report_file}" ]]; then
    error "The investigation did not produce a report."
    if (( investigation_exit == 0 )); then
      investigation_exit=1
    fi
    return "${investigation_exit}"
  fi
  if (( investigation_exit != 0 )); then
    uv run python -m incidentops.investigation.cli summarize "${report_file}"
  fi
  evaluate "${report_file}" "${evaluation_file}"
  return "${investigation_exit}"
}

if [[ "${mode}" != "live" ]]; then
  log "Investigating the retained window with the scripted model and knowledge disabled"
  investigate_and_evaluate false "${BASELINE_REPORT}" "${baseline_evaluation}"
fi
if [[ "${mode}" != "baseline" ]]; then
  log "Investigating the same retained window with required hybrid retrieval"
  investigate_and_evaluate true "${RAG_REPORT}" "${rag_evaluation}"
fi

if [[ "${mode}" == "baseline" ]]; then
  uv run python -m incidentops.investigation.cli summarize "${BASELINE_REPORT}"
else
  uv run python -m incidentops.investigation.cli summarize "${RAG_REPORT}"
fi

case "${mode}" in
  baseline)
    uv run python -m incidentops.validation.cli validate-agent-workflow \
      --report "${BASELINE_REPORT}" \
      --evaluation "${baseline_evaluation}" \
      --metadata "${METADATA_FILE}" \
      --artifact-directory "${artifact_directory}"
    ;;
  rag)
    uv run python -m incidentops.validation.cli validate-rag-workflow \
      --baseline-report "${BASELINE_REPORT}" \
      --rag-report "${RAG_REPORT}" \
      --baseline-evaluation "${baseline_evaluation}" \
      --rag-evaluation "${rag_evaluation}"
    ;;
  live)
    uv run python -m incidentops.validation.cli validate-live-rag-workflow \
      --report "${RAG_REPORT}" \
      --evaluation "${rag_evaluation}" \
      --artifact-directory "${artifact_directory}"
    ;;
esac
printf 'Investigation reports, traces, and evaluations: %s\n' "${artifact_directory}"
