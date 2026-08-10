# IncidentOps

IncidentOps is a local, educational incident-investigation project built around an
idempotent order-processing pipeline. It combines Kafka and PostgreSQL with structured logs,
Prometheus metrics, a bounded LangGraph workflow, and optional retrieval from a controlled
operational knowledge base.

The project is intentionally not production-ready. Authentication, TLS, high availability,
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
    Filebeat                            Prometheus
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
PostgreSQL, Kafka, Elasticsearch, Filebeat, and Prometheus. Named volumes preserve service
state across ordinary container restarts.

## How the services work

- **Producer:** validates generated order events, publishes them to Kafka, emits JSON logs,
  and exposes production metrics on port `8001`.
- **Consumer:** reads Kafka events, writes them idempotently to PostgreSQL, reports consumer
  lag and processing metrics on port `8002`, and emits correlated JSON logs.
- **Filebeat and Elasticsearch:** Filebeat tails `logs/*.jsonl`, keeps its read position in a
  persistent registry, and indexes structured events in `incidentops-logs-*`.
- **Prometheus:** scrapes both application endpoints and retains local samples for six hours,
  with a 512 MB size limit.
- **Investigation workflow:** a fixed LangGraph collects allow-listed metric and log evidence,
  asks the configured model for structured planning and hypotheses, verifies the evidence in
  Python, and produces a validated report.
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

The Prometheus demo starts an isolated producer and consumer, verifies both scrape targets,
and prints the main summaries:

```bash
./scripts/run-prometheus-demo.sh
```

### Local endpoints

| Service | Endpoint |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| Kafka | `localhost:9092` |
| Elasticsearch | `http://localhost:9200` |
| Prometheus | `http://localhost:9090` |
| Producer metrics | `http://localhost:8001/metrics` |
| Consumer metrics | `http://localhost:8002/metrics` |

## Run an investigation

The implemented scenario is `slow_consumer_v1`. It creates a bounded consumer slowdown and
expects correlated lag, processing latency, throughput, and log evidence without database or
Kafka errors.

Run the scenario alone:

```bash
./scripts/check-slow-consumer-scenario.sh
```

Run the complete investigation with the explicit deterministic test model and real
Prometheus and Elasticsearch data:

```bash
./scripts/check-agent-workflow.sh
```

Artifacts requested with `--persist-artifacts` are written under the ignored
`artifacts/investigations/` directory.

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
./scripts/check-rag-workflow.sh
```

### Live LLM validation

Set these values in `.env` for an OpenAI-compatible model that supports strict structured
output:

```dotenv
LLM_PROVIDER=openai-compatible
LLM_MODEL=replace-with-model-name
LLM_BASE_URL=
LLM_API_KEY=replace-with-api-key
```

Leave `LLM_BASE_URL` empty for the provider default. Then run:

```bash
./scripts/check-live-rag-workflow.sh
```

This is the only validation script that contacts an external model. It uses the real scenario,
requires hybrid retrieval, may make up to four billable model calls, evaluates the resulting
report, and keeps the generated report, trace, and evaluation under
`artifacts/investigations/`.

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

With the initialized services running, use the following progression:

| Scope | Command | External LLM |
| --- | --- | --- |
| Infrastructure health | `./scripts/check-infrastructure.sh` | No |
| Kafka/PostgreSQL pipeline | `./scripts/check-pipeline.sh` | No |
| JSONL/Filebeat/Elasticsearch logs | `./scripts/check-log-pipeline.sh` | No |
| Slow-consumer evidence | `./scripts/check-slow-consumer-scenario.sh` | No |
| Bounded investigation | `./scripts/check-agent-workflow.sh` | No |
| Baseline and RAG comparison | `./scripts/check-rag-workflow.sh` | No |
| Live-model RAG workflow | `./scripts/check-live-rag-workflow.sh` | Yes |

The scripts use unique topics, groups, run identifiers, and row prefixes. Their cleanup is
scoped to the resources created by that run; it does not remove Docker volumes or complete
Elasticsearch indices.

## Configuration

All settings and local defaults are documented in [`.env.example`](.env.example). The most
important groups are:

- PostgreSQL, Kafka, Elasticsearch, and Prometheus endpoints
- application logging and metrics ports
- the disabled-by-default consumer delay used by the test scenario
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
