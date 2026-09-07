# IncidentOps

IncidentOps is a local, educational incident-investigation project built around an
idempotent order-processing pipeline. It combines Kafka and PostgreSQL with structured logs,
Prometheus metrics, Grafana dashboards, a bounded LangGraph workflow, and optional retrieval
from a controlled operational knowledge base.

The project is intentionally not production-ready. Production access control, TLS, high availability,
external secret management, retention operations, and automatic remediation are outside its
current scope.

## Architecture

```text
                         order events
Python producer  ------------------------------>  Kafka
                                                     |
                                                     v
                                              Python consumer
                                                     |
                                                     v
                                                PostgreSQL

producer + consumer                producer + consumer
        |                                   |
        v                                   v
 JSONL log files                      /metrics endpoints
        |                                   |
        v                                   v
    Filebeat                            Prometheus ------> Grafana
        |                                   |
        v                                   |
 Elasticsearch logs -----------------------+
                                            | bounded typed queries
                                            v
                                 LangGraph investigation
                                            |
controlled knowledge ----------------------+ optional bounded RAG
                                            |
                                            v
                                 validated incident report
```

The Python producer, consumer, and command-line tools run on the host. Docker Compose runs
PostgreSQL, Kafka, Elasticsearch, Filebeat, Prometheus, and Grafana. Named volumes preserve service
state across ordinary container restarts.

## How the services work

- **Producer:** validates generated order events, publishes them to Kafka, emits JSON logs,
  and exposes production metrics on port `8001`. Explicit test modes can execute one bounded
  baseline/burst schedule or interleave a fixed number of malformed payloads.
- **Consumer:** reads Kafka events, writes them idempotently to PostgreSQL, reports consumer
  lag, processing, database, and validation metrics on port `8002`, and emits correlated JSON
  logs. Scenario-only processing and database delays are disabled by default.
- **Filebeat and Elasticsearch:** Filebeat tails `logs/*.jsonl`, keeps its read position in a
  persistent registry, and indexes structured events in `incidentops-logs-*`.
- **Prometheus:** scrapes both application endpoints and retains local samples for six hours,
  with a 512 MB size limit.
- **Grafana:** visualizes those samples in the provisioned order-pipeline dashboard on port
  `3000`, with persistent settings and a Prometheus data source configured automatically.
- **Investigation workflow:** a fixed LangGraph collects allow-listed metric and log evidence,
  asks the configured model for structured hypotheses, verifies the evidence in Python, and
  produces a validated report.
- **Knowledge retrieval:** an optional hybrid search indexes the controlled documents under
  `knowledge/`. Retrieved text is untrusted context and cannot replace live evidence.

The workflow accepts no raw PromQL or Elasticsearch DSL. Incident windows, result sizes,
tool calls, attempts, rechecks, and model calls are hard-bounded. The model cannot run shell,
Docker, Kafka administration, database-write, or remediation tools.

## Requirements

- Docker with Compose v2
- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/)
- At least 4 GiB of available memory
- `vm.max_map_count >= 262144` for Elasticsearch

## Setup

Create the local configuration and install the locked Python environment:

```bash
cp .env.example .env
chmod 600 .env
uv sync --frozen
mkdir -p logs
```

The base environment excludes the local embedding stack. Install it only for RAG ingestion,
retrieval evaluation, or RAG benchmarks:

```bash
uv sync --frozen --extra rag
```

The RAG-aware launchers select this extra automatically.

The values in `.env.example` are for local development. Keep `.env` untracked and replace the
placeholder database password if the environment is exposed beyond your machine.

Validate Compose, start the infrastructure, and initialize Elasticsearch and PostgreSQL:

```bash
docker compose config
docker compose up -d
./scripts/check-infrastructure.sh
./scripts/initialize-elasticsearch.sh
./scripts/initialize-database.sh
```

Run `docker compose config` again after every Compose or environment change. To stop the
project while preserving all named volumes:

```bash
docker compose down
```

## Run the order pipeline

Start the consumer:

```bash
uv run incidentops-consumer --run-id manual-example
```

In another terminal, produce a batch:

```bash
uv run incidentops-producer \
  --count 50 \
  --rate 10 \
  --run-id manual-example
```

Use the same `run-id` to correlate database activity, logs, and investigation evidence. For
an isolated end-to-end check of Kafka, the consumer, and PostgreSQL, run:

```bash
./scripts/check-pipeline.sh
```

## Inspect logs and metrics

Application logs are written to stdout and, by default, to:

```text
logs/order-producer.jsonl
logs/order-consumer.jsonl
```

Search and aggregate them through the bounded CLI:

```bash
uv run incidentops-log-search search \
  --run-id manual-example \
  --minutes 30

uv run incidentops-log-search aggregate \
  --group-by event_type \
  --minutes 30
```

Query the predefined Prometheus summaries:

```bash
uv run incidentops-metric-query lag --minutes 10
uv run incidentops-metric-query rates --minutes 10
uv run incidentops-metric-query latency --percentile 0.95 --minutes 10
```

The observability demo prepares the complete local stack, opens Grafana when a desktop browser
is available, runs a bounded incident, and annotates its exact time window. With no argument in
an interactive terminal, it presents a menu. Each scenario exposes six seconds of healthy idle
metrics before incident traffic starts, then remains observable for at least eighteen seconds:

```bash
./scripts/run-observability-demo.sh
./scripts/run-observability-demo.sh --scenario database_latency_v1
./scripts/run-observability-demo.sh --random
./scripts/run-observability-demo.sh --all
```

The available scenarios are `slow_consumer_v1`, `database_latency_v1`, `traffic_spike_v1`, and
`malformed_events_v1`. Use `--no-open` to print links without opening a browser and `--no-pause`
to run all four without waiting for Enter between them. The launcher does not call an LLM.

| Scenario | Main dashboard change |
| --- | --- |
| Slow consumer | Backlog and processing P95 rise while database P95 stays low. |
| Database latency | Backlog, processing P95, and database P95 rise together. |
| Traffic spike | Producer rate bursts and backlog rises while latency stays low. |
| Malformed events | Processing errors rise while valid-message throughput continues. |

### Grafana dashboard

Grafana starts with the stack, including `run-benchmark.sh` and `check-investigation.sh`.
To add it to an already running stack:

```bash
docker compose config --quiet
docker compose up -d grafana
```

Open [the pipeline dashboard](http://localhost:3000/d/incidentops-pipeline/incidentops-order-pipeline).
On first start,
the default login is `admin` / `change-me-local-only`. Configure `GRAFANA_ADMIN_USER`,
`GRAFANA_ADMIN_PASSWORD`, and `GRAFANA_PORT` in `.env` as needed. Existing `.env` files work
without adding these settings because Compose provides the same defaults. `GRAFANA_VERSION`
defaults to the locally verified `13.2.1` release tag.

The **IncidentOps / Order pipeline** dashboard opens by default after login. It shows producer
and consumer scrape status, Kafka backlog, observed production/consumption/processing rates,
processing and database P95 latency, and error rates. The data source uses the internal address
`http://prometheus:9090`, so changing the host Prometheus port does not require dashboard edits.

Run the observability demo to produce fresh telemetry. Its live link refreshes every five seconds,
and each completed scenario creates a red dashboard annotation plus a link to its exact window.
After the short-lived producer and consumer stop, their scrape status becomes `DOWN`; the samples
remain visible in the annotated window. Rates and percentiles use 30-second windows. Empty panels
mean no samples are available, not zero activity. Metrics have no `run_id` labels, so the launcher
runs scenarios sequentially and marks their time boundaries. History is limited by the existing
six-hour / 512 MB Prometheus retention.

Provisioning lives in `grafana/provisioning/`, and the dashboard is versioned in
`grafana/dashboards/incidentops-pipeline.json`. Edit the JSON to change the provisioned dashboard;
UI edits are disabled for it. Grafana's own settings are retained in `grafana_data`. Grafana
visualizes telemetry; benchmark diagnoses and RAG scores remain in the JSON reports.

### Local endpoints

| Service | Endpoint |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| Kafka | `localhost:9092` |
| Elasticsearch | `http://localhost:9200` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Producer metrics | `http://localhost:8001/metrics` |
| Consumer metrics | `http://localhost:8002/metrics` |

## Executable incident scenarios

Four versioned manifests under `scenarios/` combine bounded execution parameters with ground
truth used only after an investigation. Several share increasing Kafka lag, so a diagnosis must
also use the latency, rate, error, and log profile.

| Scenario | Explicit fault mechanism | Expected distinguishing signature | Root cause |
| --- | --- | --- | --- |
| `slow_consumer_v1` | 800 ms delay in the complete consumer path | lag and processing latency increase; database latency stays normal; `slow_processing` appears | `slow_consumer_processing` |
| `database_latency_v1` | bounded `pg_sleep()` inside the measured order transaction | lag, processing latency, and database latency increase; `database_operation_slow` appears | `database_latency` |
| `traffic_spike_v1` | observed baseline followed by a bounded producer burst | producer counter rate surges and lag increases while processing and database latency stay normal | `traffic_spike` |
| `malformed_events_v1` | deterministic mixture of valid and invalid Kafka payloads | processing errors and `invalid_event_skipped` increase while valid events continue and database latency stays normal | `malformed_event` |

Every fault is activated only by explicit scenario CLI arguments. Normal producer and consumer
defaults do not inject malformed messages, rate schedules, processing delays, or database
delays. Run one scenario through the common harness with:

```bash
uv run python -m incidentops.scenario_runner.cli run \
  --scenario database_latency_v1 \
  --output-metadata /tmp/database-latency-metadata.json
```

The harness creates a unique `run_id`, topic, consumer group, SQL row prefix, log directory,
and exact time window. It stops owned processes and deletes only those resources. Add
`--retain-evidence` only when a follow-up investigation needs that run's Elasticsearch
documents; topic, group, SQL, and JSONL cleanup still occurs. Exported metadata contains
operational observations but no root cause or expected evidence.

Run the slow-consumer evidence check through the same harness:

```bash
uv run incidentops-scenario run --scenario slow_consumer_v1
```

Run the complete investigation with the explicit deterministic test model and real
Prometheus and Elasticsearch data:

```bash
./scripts/check-investigation.sh baseline
```

Artifacts requested with `--persist-artifacts` are written under the ignored
`artifacts/investigations/` directory. Each investigation receives a unique identifier and
writes a report, a JSONL trace, a replayable evidence bundle, and the parsed model proposal.
These files are not removed automatically. Scenario cleanup applies only to resources owned by
the run, such as its topic, consumer group, SQL rows, and JSONL directory.

### Multi-incident benchmark

The primary Stage Seven validation prepares the local environment and injects all four scenarios
sequentially. Each incident is injected once, then the same retained telemetry window is
investigated with knowledge disabled and with knowledge required:

```bash
./scripts/run-benchmark.sh
```

The launcher installs the locked dependencies, validates and starts Compose, checks service
health, initializes PostgreSQL and Elasticsearch, and ingests the knowledge corpus with CPU
embeddings. It then runs the deterministic comparison and validates its acceptance criteria.
It can be rerun against existing services and reads the existing `.env` without modifying it.
Docker services remain running after the benchmark; scenario resources are cleaned by the runner.

The benchmark gives LangGraph only a neutral description, `order-consumer`, the exact window,
and the run identifier. The deterministic test provider sees the same structured evidence and
retrieved references as a live model; it receives neither the scenario identifier nor the
manifest. Deterministic verification still decides whether the proposed cause is supported.

RAG investigations share one lazily loaded embedding model for the duration of a benchmark.
Each investigation keeps its own retrieval service and graph state. Runs with knowledge disabled
do not create an embedding provider.

Results under the ignored `artifacts/benchmarks/` directory include per-scenario diagnoses, a
true-by-predicted confusion matrix, root-cause accuracy and rank, macro positive and negative
evidence recall, knowledge recall@k, unsupported reference and forbidden-action counts,
insufficient-evidence rate, call counts, and duration. RAG deltas are always reported as
required-RAG minus no-RAG. Four scenarios are a functional comparison, not evidence of
statistical significance; equal diagnosis accuracy is reported as equal, with citations and
operational context assessed separately.

New benchmark results also expose the decision funnel instead of treating every non-accepted
report as the same model failure: provider availability, structured-response validity, proposed
root-cause accuracy among valid proposals, citation coverage, verifier acceptance, and final
accepted accuracy. Parsed proposal references are measured before report sanitization. Provider
availability and LLM call counts are reported as not applicable and zero for the rules reference.

### Optional knowledge retrieval

Validate and ingest the controlled corpus, then run a bounded hybrid search:

```bash
uv run incidentops-knowledge validate --knowledge-directory knowledge
uv run incidentops-knowledge ingest --knowledge-directory knowledge --dry-run
uv run incidentops-knowledge ingest --knowledge-directory knowledge
uv run incidentops-knowledge search \
  --query "increasing consumer lag and slow processing" \
  --mode hybrid \
  --service order-consumer
```

The production embedding provider uses `all-MiniLM-L6-v2` on CPU. Its model files may need to
be downloaded on first use. Run the deterministic baseline-versus-RAG validation with:

```bash
./scripts/check-investigation.sh rag
```

Live evidence builds different bounded retrieval queries. The evaluator, and never the graph,
holds these expected document targets:

| Scenario | Expected relevant documents |
| --- | --- |
| Slow consumer | `incident_slow_consumer_processing`, `metric_processing_duration` |
| Database latency | `incident_database_latency_backlog` |
| Traffic spike | `incident_traffic_spike_backlog`, `metric_producer_consumer_rates` |
| Malformed events | `runbook_malformed_order_events` |

### Live LLM validation

Set these values in `.env` for an OpenAI-compatible model that supports strict structured
output:

```dotenv
LLM_PROVIDER=openai-compatible
LLM_MODEL=replace-with-model-name
LLM_BASE_URL=
LLM_API_KEY=replace-with-api-key
```

Leave `LLM_BASE_URL` empty for the provider default. For Gemini, use the model identifier
available to your account and `https://generativelanguage.googleapis.com/v1beta/openai/` as
`LLM_BASE_URL`.

Run setup and the full live comparison with one command:

```bash
./scripts/run-benchmark.sh --live
```

This validates the local model settings before starting services, performs the same setup as
the deterministic launcher, and runs eight live investigations: all four scenarios, each with
and without RAG over the same telemetry window. It uses CPU embeddings and hybrid retrieval.
External API calls may incur costs. The JSON result is written to
`artifacts/benchmarks/live-comparison-<UTC timestamp>.json`; the launcher prints its path and
the report/trace directory at the end. It also prints compact case and aggregate tables with
status, diagnosis, exact-match result, call count, duration, evidence scores, verification
rejections, and safe model-error categories. A completed live run does not imply that its
diagnoses passed evaluation; inspect the recorded results. Strict deterministic acceptance is
applied only to runs without `--live`.

Provider availability is reported separately from structured-output validity, proposed-cause
accuracy, verifier acceptance, and final accepted accuracy. A provider failure therefore does
not count as a wrong diagnosis. A run interrupted before aggregation may leave complete
per-investigation artifacts without writing the requested benchmark result; those evidence
bundles can be replayed instead of reinjecting the incident.

### Latest live-model result

The Gemini comparison run reached 75% first-pass provider availability and
75% final accepted accuracy in both the LLM and LLM-with-RAG modes. All responses actually
received were structurally valid, proposed the expected cause, passed deterministic verification,
and cited all available evidence. Both calls for the slow-consumer scenario failed with the safe
`request_failed` category before producing a proposal.

Replaying that scenario from its saved evidence bundle succeeded for rules, LLM, and LLM with
RAG. After this targeted retry, all four scenario causes had been diagnosed correctly in both LLM
modes, while the original run's first-pass availability remained 75%. RAG added cited operational
references but did not improve cause accuracy on this four-scenario sample. These results are a
functional observation from a small local benchmark, not evidence of statistical significance.

Reprint the terminal tables from any saved result with:

```bash
uv run incidentops-benchmark summarize artifacts/benchmarks/live-comparison-<timestamp>.json
```

Use `--output-file PATH` (or `BENCHMARK_OUTPUT`) to select the result file. Relative paths are
resolved from the project root. `--help` starts no services and makes no API calls.

The embedding model is loaded once in the ingestion process, then once in the benchmark
process and shared by all RAG investigations in that benchmark. Its downloaded files are cached.

For a focused acceptance check on just the slow-consumer scenario with required RAG:

```bash
./scripts/check-investigation.sh live
```

This prepares the environment, evaluates the report, and keeps the report, trace, and evaluation
under `artifacts/investigations/`. It fails if the diagnosis or evidence does not meet the live
acceptance checks. A compact terminal summary shows the status, diagnosis, evidence counts,
knowledge references, model/tool calls, verifier rejections, and safe model-error categories.
Both live commands are optional and excluded from automated validation.

Each persisted investigation consists of four files with the same identifier:

- `<investigation-id>.report.json` contains the final deterministic report.
- `<investigation-id>.trace.jsonl` contains the bounded workflow trace.
- `<investigation-id>.evidence.json` contains replayable structured observations and saved RAG
  references, with no scenario manifest or expected root cause.
- `<investigation-id>.proposal.json` contains the bounded parsed model proposal before report
  filtering, including its original short `reasoning_summary` and safe failure metadata.

Benchmark results and investigation artifacts are intentionally retained and ignored by Git.
Use a new timestamped benchmark output for each comparison. If disk usage grows, keep the
investigation prefixes needed for audit or replay and remove selected older prefixes manually;
removing a benchmark summary does not remove its investigation artifacts, and removing an
investigation bundle prevents later evidence replay.

Re-run deterministic verification without calling a model:

```bash
uv run incidentops-benchmark rescore \
  artifacts/investigations/<investigation-id>.evidence.json \
  artifacts/investigations/<investigation-id>.proposal.json
```

Generate a fresh proposal from the same evidence without querying Prometheus or Elasticsearch:

```bash
uv run incidentops-benchmark replay \
  artifacts/investigations/<investigation-id>.evidence.json \
  --approach rules
```

The other approaches are `llm` and `llm-rag`. Those two contact the configured external provider;
`llm-rag` uses only the knowledge references already stored in the bundle. Compare all three on
one bundle while keeping the scenario manifest on the evaluator side:

```bash
uv run incidentops-benchmark compare-evidence \
  artifacts/investigations/<investigation-id>.evidence.json \
  --scenario-path scenarios/slow_consumer.yaml
```

Live commands send the neutral incident description, affected service, exact bounded time
window, run identifier, structured metric/log evidence summaries, and retrieved knowledge
snippets to the configured external model provider. They do not send API keys, raw PromQL,
Elasticsearch query DSL, scenario ground truth, or unrestricted log searches.

Provider failures are retained using bounded categories: `quota_exceeded`, `timeout`,
`request_rejected`, `invalid_structured_response`, `request_failed`, and `call_limit`. Structured
validation failures also retain only the schema field and validation rule; provider response
bodies, invalid model values, and credentials are excluded. Final JSON and Markdown reports
include deterministic verifier issues so rejected diagnoses remain directly explainable.
The hypothesis contract requires each positive diagnosis to account for every available evidence
identifier exactly once, including completed zero-result checks, and forbids duplicate causes or
references that appear as both supporting and contradicting evidence.
Evidence collection uses a fixed deterministic six-task plan. The model is called only to propose
root-cause hypotheses, reducing the normal live path from two model calls to one. Its short
`reasoning_summary` is retained only in the proposal audit artifact and is never treated as
verified evidence; final report wording remains deterministic.
The default live-model timeout is 60 seconds, bounded to at most 120 seconds. This accommodates
larger structured hypothesis requests while the hard call and retry limits remain unchanged.

For a manually selected incident window, use the investigation CLI directly:

```bash
uv run python -m incidentops.investigation.cli investigate \
  --description "Orders were delayed during the selected window." \
  --start-time "2026-08-01T13:00:00Z" \
  --end-time "2026-08-01T13:10:00Z" \
  --run-id manual-example \
  --affected-service order-consumer \
  --output-format markdown \
  --output-file incident-report.md \
  --persist-artifacts
```

## Validation commands

Fast checks that do not require running services:

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest tests/unit
```

Integration tests are explicit and require the corresponding initialized services:

```bash
uv run pytest tests/integration -m elasticsearch
uv run pytest tests/integration -m prometheus
uv run pytest tests/integration -m integration
```

The regular CI runs the unit suite. The separate `Integration` GitHub Actions workflow can be
started manually when Elasticsearch, Prometheus, or their clients and contracts change.

The benchmark and investigation launchers prepare their own environment. The lower-level checks
and direct scenario CLI expect initialized services:

| Scope | Command | External LLM |
| --- | --- | --- |
| Infrastructure health | `./scripts/check-infrastructure.sh` | No |
| Kafka/PostgreSQL pipeline | `./scripts/check-pipeline.sh` | No |
| JSONL/Filebeat/Elasticsearch logs | `./scripts/check-log-pipeline.sh` | No |
| Slow-consumer evidence | `uv run incidentops-scenario run --scenario slow_consumer_v1` | No |
| Interactive scenario metrics | `./scripts/run-observability-demo.sh` | No |
| Bounded investigation | `./scripts/check-investigation.sh baseline` | No |
| Baseline and RAG comparison | `./scripts/check-investigation.sh rag` | No |
| Four-scenario RAG/no-RAG benchmark | `./scripts/run-benchmark.sh` | No |
| Live four-scenario comparison | `./scripts/run-benchmark.sh --live` | Yes |
| Live-model RAG acceptance check | `./scripts/check-investigation.sh live` | Yes |

The three former `check-*-workflow.sh` entry points are consolidated into
`check-investigation.sh`. `run-benchmark.sh` replaces `check-multi-incident-benchmark.sh`, and
the scenario CLI replaces `check-slow-consumer-scenario.sh`. Shared setup lives in
`scripts/lib/workflow.sh`, which is sourced by the launchers.

The infrastructure, pipeline, and log checks remain separate because they test service health,
delivery/idempotency, and log ingestion/correlation respectively. The two initialization scripts
are reusable setup steps. `run-observability-demo.sh` reuses the common scenario runner as the single
launcher for interactive, random, and four-scenario Grafana demonstrations.

The scripts use unique topics, groups, run identifiers, and row prefixes. Their cleanup is
scoped to the resources created by that run; it does not remove Docker volumes or complete
Elasticsearch indices.

## Configuration

All settings and local defaults are documented in [`.env.example`](.env.example). The most
important groups are:

- PostgreSQL, Kafka, Elasticsearch, and Prometheus endpoints
- application logging, metrics ports, and local Grafana annotation settings
- the disabled-by-default processing and database delays used only by explicit scenarios
- embedding and optional knowledge-retrieval settings
- live model credentials and timeouts
- hard investigation limits

Command-specific options are available through `--help`, for example:

```bash
uv run incidentops-producer --help
uv run incidentops-consumer --help
uv run incidentops-log-search --help
uv run incidentops-metric-query --help
uv run incidentops-knowledge --help
uv run incidentops-scenario --help
uv run incidentops-benchmark --help
```

## Troubleshooting

Start with service health and the logs of the failing component:

```bash
docker compose ps
docker compose logs --tail=100 <service>
./scripts/check-infrastructure.sh
```

- If Elasticsearch is unhealthy, check available memory and `vm.max_map_count` before changing
  configuration.
- If Filebeat receives no logs, confirm that `logs/*.jsonl` exists, then inspect the Filebeat
  health check and logs. Preserve the `filebeat_data` volume because it contains read offsets.
- If a Prometheus target is down, confirm the corresponding Python application is running and
  its `/metrics` endpoint is reachable.
- If a check times out, inspect the temporary process-log path printed by the script and the
  relevant Kafka, PostgreSQL, Filebeat, Elasticsearch, or Prometheus logs.
- If a port is already in use, change its local value in `.env`, validate Compose, and restart
  only the affected project services.

## License

IncidentOps is available under the [MIT License](LICENSE).
