#!/usr/bin/env bash

# Run one or more bounded incident scenarios and show their metric windows in Grafana.

set -Eeuo pipefail

readonly SCENARIOS=(
  slow_consumer_v1
  database_latency_v1
  traffic_spike_v1
  malformed_events_v1
)
readonly NORMAL_PHASE_SECONDS=6
readonly MINIMUM_INCIDENT_SECONDS=18

usage() {
  cat <<'EOF'
Usage: run-observability-demo.sh [SELECTION] [OPTIONS]

Start and check the local stack, run bounded incident scenarios, and annotate their exact
metric windows in Grafana. With no selection in a terminal, an interactive menu is shown.

Selections:
  slow_consumer_v1     Consumer processing is slower than incoming traffic.
  database_latency_v1 PostgreSQL latency slows the consumer.
  traffic_spike_v1    Producer traffic suddenly exceeds normal throughput.
  malformed_events_v1 Invalid messages raise processing errors.
  random               Run one randomly selected scenario.
  all                  Run all four scenarios in a stable order.

Options:
  --scenario ID  Run one of the four allow-listed scenario IDs.
  --random       Run one randomly selected scenario.
  --all          Run all scenarios.
  --open         Try to open the live Grafana dashboard in the default browser.
  --no-open      Print Grafana links without opening a browser.
  --no-pause     Do not wait for Enter between scenarios when running all.
  --help         Show this help without starting services or running a scenario.

Examples:
  ./scripts/run-observability-demo.sh
  ./scripts/run-observability-demo.sh --scenario database_latency_v1
  ./scripts/run-observability-demo.sh --random --no-open
  ./scripts/run-observability-demo.sh --all --no-pause

This launcher does not call an LLM. Each scenario owns and cleans its temporary resources;
its Prometheus samples and Grafana annotation remain available for inspection. Each run shows
a short healthy idle phase before traffic starts and keeps the incident visible for a minimum
bounded duration.
EOF
}

is_known_scenario() {
  local candidate="$1"
  local scenario
  for scenario in "${SCENARIOS[@]}"; do
    if [[ "${candidate}" == "${scenario}" ]]; then
      return 0
    fi
  done
  return 1
}

selection=""
browser_mode="auto"
pause_between=true

set_selection() {
  local requested="$1"
  if [[ -n "${selection}" ]]; then
    printf '[ERROR] Choose only one scenario selection.\n' >&2
    usage >&2
    exit 2
  fi
  selection="${requested}"
}

while (( $# > 0 )); do
  case "$1" in
    --scenario)
      if (( $# < 2 )) || [[ -z "$2" || "$2" == --* ]]; then
        printf '[ERROR] --scenario requires an allow-listed scenario ID.\n' >&2
        exit 2
      fi
      set_selection "$2"
      shift 2
      ;;
    --random)
      set_selection random
      shift
      ;;
    --all)
      set_selection all
      shift
      ;;
    --open)
      browser_mode=open
      shift
      ;;
    --no-open)
      browser_mode=closed
      shift
      ;;
    --no-pause)
      pause_between=false
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --*)
      printf '[ERROR] Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
    *)
      set_selection "$1"
      shift
      ;;
  esac
done

if [[ -z "${selection}" ]]; then
  if [[ ! -t 0 ]]; then
    printf '[ERROR] Choose a scenario, --random, or --all when input is not interactive.\n' >&2
    usage >&2
    exit 2
  fi
  cat <<'EOF'
Choose an incident scenario:
  1) Slow consumer
  2) Database latency
  3) Traffic spike
  4) Malformed events
  r) Random scenario
  a) All scenarios
EOF
  read -r -p 'Selection: ' answer
  case "${answer}" in
    1) selection=slow_consumer_v1 ;;
    2) selection=database_latency_v1 ;;
    3) selection=traffic_spike_v1 ;;
    4) selection=malformed_events_v1 ;;
    r|R) selection=random ;;
    a|A) selection=all ;;
    *)
      printf '[ERROR] Invalid menu selection: %s\n' "${answer}" >&2
      exit 2
      ;;
  esac
fi

if [[ "${selection}" != "random" && "${selection}" != "all" ]] \
  && ! is_known_scenario "${selection}"; then
  printf '[ERROR] Unsupported scenario: %s\n' "${selection}" >&2
  usage >&2
  exit 2
fi

if [[ "${browser_mode}" == "auto" ]]; then
  if [[ -t 0 && -t 1 ]]; then
    browser_mode=open
  else
    browser_mode=closed
  fi
fi

source "$(dirname -- "${BASH_SOURCE[0]}")/lib/workflow.sh"

scenario_title() {
  case "$1" in
    slow_consumer_v1) printf 'Slow consumer' ;;
    database_latency_v1) printf 'Database latency' ;;
    traffic_spike_v1) printf 'Traffic spike' ;;
    malformed_events_v1) printf 'Malformed events' ;;
  esac
}

print_expected_signature() {
  case "$1" in
    slow_consumer_v1)
      printf 'Expected signal: consumer lag and processing P95 rise; database P95 stays low.\n'
      ;;
    database_latency_v1)
      printf 'Expected signal: consumer lag, processing P95, and database P95 rise together.\n'
      ;;
    traffic_spike_v1)
      printf 'Expected signal: producer rate bursts and lag rises while latencies stay low.\n'
      ;;
    malformed_events_v1)
      printf 'Expected signal: processing errors rise while valid-message throughput continues.\n'
      ;;
  esac
}

open_dashboard() {
  local url="$1"
  if command -v wslview >/dev/null 2>&1 && wslview "${url}" >/dev/null 2>&1; then
    return 0
  fi
  if command -v powershell.exe >/dev/null 2>&1 \
    && powershell.exe -NoProfile -NonInteractive \
      -Command 'Start-Process -FilePath $args[0]' "${url}" >/dev/null 2>&1; then
    return 0
  fi
  if command -v xdg-open >/dev/null 2>&1 && xdg-open "${url}" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

prepare_environment false false

grafana_binding="$(docker compose port grafana 3000)"
case "${grafana_binding}" in
  0.0.0.0:*) grafana_binding="localhost:${grafana_binding#*:}" ;;
  \[::\]:*) grafana_binding="localhost:${grafana_binding##*:}" ;;
esac
readonly grafana_root_url="http://${grafana_binding}"
readonly live_dashboard_url="${grafana_root_url}/d/incidentops-pipeline/incidentops-order-pipeline?from=now-5m&to=now&timezone=browser&refresh=5s"

printf '\nLive Grafana dashboard:\n  %s\n\n' "${live_dashboard_url}"
if [[ "${browser_mode}" == "open" ]]; then
  if open_dashboard "${live_dashboard_url}"; then
    log "Opened the live dashboard in the default browser"
  else
    log "Could not open a browser automatically; use the link above"
  fi
fi

if [[ "${selection}" == "all" ]]; then
  selected_scenarios=("${SCENARIOS[@]}")
elif [[ "${selection}" == "random" ]]; then
  selected_scenarios=("${SCENARIOS[RANDOM % ${#SCENARIOS[@]}]}")
  log "Randomly selected ${selected_scenarios[0]}"
else
  selected_scenarios=("${selection}")
fi

temp_directory="$(mktemp -d -t incidentops-grafana-demo.XXXXXX)"
cleanup() {
  rm -rf -- "${temp_directory}"
}
trap cleanup EXIT

scenario_count="${#selected_scenarios[@]}"
for scenario_index in "${!selected_scenarios[@]}"; do
  scenario_id="${selected_scenarios[scenario_index]}"
  display_index=$((scenario_index + 1))
  metadata_file="${temp_directory}/${display_index}-${scenario_id}.json"
  scenario_output="${temp_directory}/${display_index}-${scenario_id}.out"

  printf '\n=== Scenario %d/%d: %s (%s) ===\n' \
    "${display_index}" "${scenario_count}" "$(scenario_title "${scenario_id}")" "${scenario_id}"
  print_expected_signature "${scenario_id}"
  printf 'Normal phase: %s seconds, then at least %s seconds of incident observation.\n' \
    "${NORMAL_PHASE_SECONDS}" "${MINIMUM_INCIDENT_SECONDS}"
  printf 'Watch the dashboard while the scenario runs.\n\n'

  if ! uv run incidentops-scenario run \
    --scenario "${scenario_id}" \
    --start-delay-seconds "${NORMAL_PHASE_SECONDS}" \
    --minimum-incident-seconds "${MINIMUM_INCIDENT_SECONDS}" \
    --output-metadata "${metadata_file}" >"${scenario_output}"; then
    cat "${scenario_output}" >&2
    error "Scenario ${scenario_id} failed."
    exit 1
  fi

  GRAFANA_URL="${grafana_root_url}" \
    uv run python -m incidentops.grafana annotate-scenario --metadata "${metadata_file}"

  if (( display_index < scenario_count )) && [[ "${pause_between}" == "true" && -t 0 ]]; then
    printf '\nPress Enter to run the next scenario...'
    read -r _
  fi
done

printf '\nObservability demo completed successfully.\n'
printf 'Scenario processes stopped and cleaned their run-owned resources.\n'
printf 'Annotated metric windows remain available in Prometheus for up to six hours.\n'
printf 'No language model was called.\n'
