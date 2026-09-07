#!/usr/bin/env bash

# Prepare local services and compare all four incidents with and without RAG.
# External model calls require the explicit --live option.

set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage: run-benchmark.sh [--live] [--output-file PATH]

Sync dependencies, start and check Compose services, initialize storage, ingest knowledge,
and investigate each of four incidents twice over the same retained telemetry window.

Options:
  --live              Use the OpenAI-compatible model configured in .env (may incur API costs).
                      Without this option, use the deterministic test model and validate results.
  --output-file PATH  Write results here instead of the timestamped artifacts/benchmarks file.
                      BENCHMARK_OUTPUT is also supported. Relative paths use the project root.
  --help              Show this help without starting services or making model calls.
EOF
}

live_model=false
model_provider=deterministic-test
result_file="${BENCHMARK_OUTPUT:-}"

while (( $# > 0 )); do
  case "$1" in
    --live)
      live_model=true
      model_provider=openai-compatible
      shift
      ;;
    --output-file)
      if (( $# < 2 )) || [[ -z "$2" || "$2" == --* ]]; then
        printf '[ERROR] --output-file requires a path.\n' >&2
        exit 2
      fi
      result_file="$2"
      shift 2
      ;;
    --help|-h)
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

source "$(dirname -- "${BASH_SOURCE[0]}")/lib/workflow.sh"

if [[ -z "${result_file}" ]]; then
  result_prefix=multi-incident
  if [[ "${live_model}" == "true" ]]; then
    result_prefix=live-comparison
  fi
  result_file="${PROJECT_DIR}/artifacts/benchmarks/${result_prefix}-$(date -u +%Y%m%dT%H%M%SZ).json"
fi

prepare_environment "${live_model}" true
ingest_knowledge

log "Running four incidents with model provider ${model_provider}, with and without RAG"
log "Benchmark output: ${result_file}"
EMBEDDING_PROVIDER=sentence-transformers \
KNOWLEDGE_RETRIEVAL_MODE=hybrid \
  uv run incidentops-benchmark run \
    --all \
    --model-provider "${model_provider}" \
    --knowledge-mode compare \
    --output-format summary \
    --output-file "${result_file}"

if [[ "${live_model}" == "true" ]]; then
  log "Live comparison completed. Read the results to assess diagnosis quality."
else
  uv run incidentops-benchmark validate "${result_file}"
fi

printf 'Benchmark result: %s\n' "${result_file}"
printf 'Investigation reports and traces: %s\n' \
  "$(uv run python -m incidentops.validation.cli artifact-directory)"
