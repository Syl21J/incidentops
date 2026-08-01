# IncidentOps

IncidentOps implements a local order-processing path with logs and Prometheus metrics:

```text
Order producer -> Kafka -> Order consumer -> PostgreSQL

Producer and consumer in WSL
              |
              v
  logs/*.jsonl (one file per service)
              |
              v
     Filebeat 8.19.17 in Docker
              |
              v
 incidentops-logs-YYYY.MM.DD in Elasticsearch
              |
              v
 bounded Python search and aggregation tools in WSL

Producer and consumer in WSL
              |
              +-- HTTP :8001/metrics and :8002/metrics
              |
              v
 Prometheus 3.12.0 in Docker Compose
              |
              v
 bounded typed metric tools in WSL
```

The Python applications are not containerized. They expose metrics directly from WSL and
always write JSON logs to stdout and can
also append the same records to local JSON Lines files. Filebeat is the only log shipper: it
tails those files, remembers offsets in a persistent registry, and sends parsed fields to
Elasticsearch. The applications have no Elasticsearch dependency and continue to process
orders when Filebeat or Elasticsearch is unavailable.

## Project status

IncidentOps is an educational local-development project. The first four implementation
stages are complete:

- PostgreSQL, Kafka, Elasticsearch, and Filebeat run as healthy Docker Compose services.
- The Python producer and consumer run directly in WSL and implement an idempotent
  Kafka-to-PostgreSQL order pipeline.
- Application logs are validated JSON, written to stdout and optional JSONL files, shipped by
  Filebeat, and searchable through bounded typed Python tools.
- Unit tests cover configuration, logging, query construction, response validation, and
  processing behavior. Local integration and end-to-end scripts cover the real services.
- Prometheus scrapes producer and consumer endpoints, typed tools expose bounded metric
  summaries, and the versioned `slow_consumer_v1` scenario validates correlated metrics and
  Elasticsearch logs.
- GitHub Actions runs formatting, linting, type checking, and tests for every push to `main`
  and every pull request.

This is not a production deployment. Authentication, TLS, external secret management,
retention policies, high availability, and production operations are intentionally outside
the current local MVP.

## Prerequisites

- Docker Compose v2
- Python 3.12 or newer
- [`uv`](https://docs.astral.sh/uv/)
- At least 4 GiB of available memory
- `vm.max_map_count` greater than or equal to `262144`

## Setup

Create the local configuration and install the locked Python environment:

```bash
cp .env.example .env
uv sync
mkdir -p logs
```

The example password is only a local placeholder. Change it before using this setup beyond
local development.

Validate and start the infrastructure:

```bash
docker compose config
docker compose up -d
./scripts/check-infrastructure.sh
```

Install the versioned Elasticsearch index template and apply the database migration:

```bash
./scripts/initialize-elasticsearch.sh
./scripts/initialize-database.sh
```

Initialize the template before producing application logs on a new environment. The script
waits for Elasticsearch, creates or updates the `incidentops-logs` template, and verifies both
the template and any existing matching index mappings. It never deletes an index.

Stop the containers without deleting their named volumes:

```bash
docker compose down
```

## Run the order pipeline

Start the consumer directly in WSL:

```bash
uv run python -m incidentops.consumer --run-id manual-example
```

Produce orders from another WSL terminal:

```bash
uv run python -m incidentops.producer \
  --count 50 \
  --rate 10 \
  --run-id manual-example
```

Run the reproducible Kafka/PostgreSQL end-to-end check:

```bash
./scripts/check-pipeline.sh
```

The check uses an isolated Kafka topic, consumer group, and customer prefix. It removes only
the topic, group, process, and database rows that it creates.

## Log files and Filebeat

File logging is enabled by default:

```text
logs/order-producer.jsonl
logs/order-consumer.jsonl
```

Each line is one complete UTF-8 JSON object. The application timestamp is an ISO 8601 UTC
value in `@timestamp`; exceptions remain escaped inside a single valid JSON line. Filebeat
mounts `./logs` read-only, uses a `filestream` input with an NDJSON parser, and keeps decoded
application fields at the document root.

Filebeat state is stored in the `filebeat_data` named volume. Its stable input ID and
content-based file fingerprints prevent ordinary container restarts from replaying every
previously acknowledged line. The registry is operational state and must not be deleted as a
routine troubleshooting step.

Check Filebeat explicitly:

```bash
docker compose ps filebeat
docker compose exec -T filebeat \
  filebeat test config -c /usr/share/filebeat/filebeat.yml
docker compose exec -T filebeat \
  filebeat test output -c /usr/share/filebeat/filebeat.yml
docker compose logs --tail=100 filebeat
```

## Elasticsearch log mapping

Filebeat writes daily regular indices named `incidentops-logs-YYYY.MM.DD`. ILM and data
streams are intentionally disabled for this local MVP. The versioned template at
`elasticsearch/index-template.json` matches `incidentops-logs-*` and defines:

| Field | Elasticsearch type | Purpose |
| --- | --- | --- |
| `@timestamp` | `date` | Application event time |
| `level` | `keyword` | Exact severity filter |
| `service` | `keyword` | Exact service filter |
| `event_type` | `keyword` | Exact event category and aggregation |
| `logger` | `keyword` | Exact logger filter |
| `message` | `text` | Full-text search |
| `event_id` | `keyword` | Exact event correlation |
| `order_id` | `keyword` | Exact order correlation |
| `duration_ms` | `float` | Numeric processing duration |
| `error_type` | `keyword` | Exact error category |
| `run_id` | `keyword` | Exact end-to-end run correlation |

The template also types the existing operational context fields (`topic`, `consumer_group`,
counts, insertion status, and readable exception text). Unknown Filebeat metadata remains in
`_source` but is not added dynamically to the mapping.

## Search and aggregation CLI

All searches use allow-listed filters, a mandatory or default recent time window, and a
maximum result limit of 500. The CLI never accepts raw Elasticsearch JSON.

Search recent consumer `INFO` logs:

```bash
uv run python -m incidentops.log_search search \
  --service order-consumer \
  --level INFO \
  --minutes 15
```

Correlate a complete run:

```bash
uv run python -m incidentops.log_search search \
  --run-id pipeline-check-example
```

Run a full-text search:

```bash
uv run python -m incidentops.log_search search \
  --message "order event processed" \
  --minutes 30
```

Count logs by an allow-listed keyword field:

```bash
uv run python -m incidentops.log_search aggregate \
  --group-by event_type \
  --minutes 30
```

Build a one-minute consumer timeline:

```bash
uv run python -m incidentops.log_search timeline \
  --service order-consumer \
  --interval 1m \
  --minutes 30
```

The Python API in `incidentops.log_search` exposes the typed functions `search_logs`,
`count_logs_by_event_type`, and `get_log_timeline`. Pydantic validates query parameters and
Elasticsearch responses.

### Real Elasticsearch search example

The following result was returned by the local Elasticsearch instance after producing the
deterministic run `demo-elasticsearch-1`:

```bash
uv run python -m incidentops.log_search search \
  --run-id demo-elasticsearch-1 \
  --service order-producer \
  --event-type production_summary \
  --minutes 10080 \
  --limit 1
```

```json
{
  "total": 1,
  "logs": [
    {
      "@timestamp": "2026-07-29T13:20:49.117000Z",
      "level": "INFO",
      "service": "order-producer",
      "event_type": "production_summary",
      "message": "Order production completed",
      "logger": "order-producer",
      "event_id": null,
      "order_id": null,
      "duration_ms": 1858.55,
      "error_type": null,
      "run_id": "demo-elasticsearch-1"
    }
  ]
}
```

The exact `run_id`, `service`, and `event_type` filters select the final producer summary.
`duration_ms` shows that producing and acknowledging the ten-event batch took about 1.86
seconds.

### Real Elasticsearch aggregation example

```bash
uv run python -m incidentops.log_search aggregate \
  --group-by event_type \
  --run-id demo-elasticsearch-1 \
  --minutes 10080
```

```json
{
  "group_by": "event_type",
  "buckets": [
    {"key": "order_processed", "count": 10},
    {"key": "consumer_error", "count": 2},
    {"key": "consumer_started", "count": 2},
    {"key": "consumer_summary", "count": 2},
    {"key": "shutdown_requested", "count": 2},
    {"key": "partitions_assigned", "count": 1},
    {"key": "production_summary", "count": 1},
    {"key": "topic_created", "count": 1}
  ]
}
```

The ten `order_processed` documents correspond to the ten generated orders. The startup,
topic, partition, shutdown, and summary buckets describe the surrounding application
lifecycle. The two early `consumer_error` documents record attempts made before the Kafka
topic existed; they remain searchable operational evidence rather than being hidden.

## End-to-end log validation

Run the complete collection and search check:

```bash
./scripts/check-log-pipeline.sh
```

It checks the existing infrastructure, installs the template, validates Filebeat, starts a
bounded consumer, produces a deterministic batch, waits with explicit deadlines, finds both
services by a unique `run_id`, verifies event correlation fields, and runs a service
aggregation. Cleanup targets only that run's Kafka topic and group, PostgreSQL rows, temporary
processes/files, isolated JSONL subdirectory, and Elasticsearch documents selected by its exact
`run_id`. It never deletes an application index or Docker volume.

## Prometheus metrics architecture

Prometheus 3.12.0 runs in Docker Compose with a six-hour/512 MB local retention limit and the
named `prometheus_data` volume. It listens only on `127.0.0.1:9090`. The Python applications
continue to run directly in WSL and bind their metrics endpoints to `0.0.0.0` so the
Prometheus container can scrape them through `host.docker.internal` and the Compose
`host-gateway` mapping. A stopped application can make its target temporarily `down`; this
does not make Prometheus unhealthy.

Start the infrastructure and open the UI at [http://localhost:9090](http://localhost:9090):

```bash
docker compose config
docker compose up -d
docker compose ps prometheus
curl --fail http://localhost:9090/-/healthy
```

Start both application endpoints from WSL:

```bash
uv run python -m incidentops.consumer --run-id metrics-example

uv run python -m incidentops.producer \
  --count 100 \
  --rate 10 \
  --run-id metrics-example
```

The producer endpoint uses port 8001 and the consumer endpoint uses port 8002 by default.
`--metrics-host`, `--metrics-port`, and `--no-metrics` provide explicit CLI control. Merely
importing a module never starts a server, collectors use isolated registries, and no custom
metric has an application label. In particular, `event_id`, `order_id`, `run_id`, and
exception messages are never metric labels.

### Custom metrics

| Metric | Type | Meaning |
| --- | --- | --- |
| `incidentops_orders_produced_total` | Counter | Events whose Kafka delivery callback confirmed success |
| `incidentops_order_production_errors_total` | Counter | Validation, production, delivery, topic, or flush failures |
| `incidentops_order_production_duration_seconds` | Histogram | Produce-to-Kafka-confirmation delivery duration |
| `incidentops_producer_target_rate` | Gauge | Configured target events per second |
| `incidentops_orders_consumed_total` | Counter | Non-error Kafka messages received, including malformed messages |
| `incidentops_orders_processed_total` | Counter | Valid messages completed after the database transaction, including idempotent duplicates |
| `incidentops_order_processing_errors_total` | Counter | Malformed messages and processing/database failures |
| `incidentops_order_processing_duration_seconds` | Histogram | Complete per-message path, including an enabled development delay |
| `incidentops_database_operation_duration_seconds` | Histogram | PostgreSQL transaction duration only |
| `incidentops_kafka_consumer_lag` | Gauge | Aggregate authoritative consumer-group lag across assigned partitions |

Kafka lag is calculated per assigned partition as:

```text
max(0, Kafka high watermark - committed consumer-group offset)
```

Both Kafka values are next offsets: the high watermark is the next not-yet-written offset and
the committed group offset is the next message to consume. If the group has no committed
offset, the retained low watermark is used, so all currently retained records count as lag.
The consumer refreshes this total at a bounded configurable interval. Collection failures
produce a warning log and do not stop message processing.

Useful manual PromQL in the UI:

```promql
incidentops_kafka_consumer_lag
```

```promql
rate(incidentops_orders_produced_total[1m])
```

```promql
rate(incidentops_orders_processed_total[1m])
```

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(incidentops_order_processing_duration_seconds_bucket[5m])
  )
)
```

## Bounded metric CLI

The metric client uses the Prometheus HTTP API with a five-second timeout, a six-hour maximum
window, steps from 1 to 300 seconds, and response limits of 2 MB, 50 series, and 10,000
samples. Public range queries accept only documented IncidentOps metric names and exact
`job`/`instance` filters. The CLI exposes only predefined operations; arbitrary PromQL is not
accepted.

```bash
uv run python -m incidentops.metric_query lag --minutes 10
uv run python -m incidentops.metric_query rates --minutes 10
uv run python -m incidentops.metric_query latency \
  --percentile 0.95 \
  --minutes 10
```

Every result is a Pydantic model rendered as readable JSON. The commands return non-zero for
an unavailable Prometheus server, invalid input, oversized response, or missing samples.

## Slow-consumer scenario

The tracked ground truth is `scenarios/slow_consumer.yaml`. Its schema version, identity,
root cause, expected metric behavior, expected logs, negative evidence, acceptable actions,
and forbidden actions are validated by Pydantic. Run the single automated scenario with:

```bash
./scripts/check-slow-consumer-scenario.sh
```

It creates a unique topic, consumer group, `run_id`, JSONL directory, SQL row set, and
Elasticsearch document set. It sends 80 deterministic events at 10/s while the consumer has
an 800 ms development-only delay (about 1.25/s capacity) and a 500 ms slow threshold. The
validation requires at least 30 messages of lag, P95 processing duration of at least 0.7 s,
producer throughput above consumer throughput, `slow_processing` logs, and zero database or
Kafka application errors. All waits use explicit deadlines.

The delay defaults to zero and is accepted only from 0 through 5000 ms. It occurs inside the
processing timer. The scenario stops the consumer within the bounded incident window instead
of waiting for a full catch-up, then removes only resources created by its unique token. It
does not delete any Docker volume, complete Elasticsearch index, shared topic, or unrelated
database row.

## Local endpoints

| Service | WSL endpoint | Container endpoint |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `postgres:5432` |
| Elasticsearch | `http://localhost:9200` | `http://elasticsearch:9200` |
| Kafka | `localhost:9092` | `kafka:29092` |
| Prometheus | `http://localhost:9090` | `prometheus:9090` |
| Producer metrics | `http://localhost:8001/metrics` | `host.docker.internal:8001/metrics` |
| Consumer metrics | `http://localhost:8002/metrics` | `host.docker.internal:8002/metrics` |

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka endpoint used by Python applications |
| `KAFKA_TOPIC` | `orders.v1` | Order event topic |
| `KAFKA_CONSUMER_GROUP` | `incidentops-order-consumer-v1` | Consumer group |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host used by Python |
| `POSTGRES_PORT` | `5432` | PostgreSQL host port |
| `POSTGRES_USER` | `incidentops` | PostgreSQL user |
| `POSTGRES_PASSWORD` | local placeholder | PostgreSQL password |
| `POSTGRES_DB` | `incidentops` | PostgreSQL database |
| `ELASTICSEARCH_URL` | `http://localhost:9200` | WSL search-client endpoint |
| `PROMETHEUS_URL` | `http://localhost:9090` | WSL typed metric-client endpoint |
| `PROMETHEUS_PORT` | `9090` | Prometheus loopback host port |
| `PROMETHEUS_SCRAPE_INTERVAL` | `2s` | Documented development scrape interval |
| `METRICS_HOST` | `0.0.0.0` | WSL bind address reachable from Docker |
| `PRODUCER_METRICS_PORT` | `8001` | Producer metrics port |
| `CONSUMER_METRICS_PORT` | `8002` | Consumer metrics port |
| `CONSUMER_LAG_UPDATE_INTERVAL_SECONDS` | `2` | Authoritative lag refresh interval |
| `CONSUMER_PROCESSING_DELAY_MS` | `0` | Development-only bounded delay; disabled by default |
| `SLOW_PROCESSING_THRESHOLD_MS` | `500` | Threshold for `slow_processing` logs |
| `LOG_LEVEL` | `INFO` | Application JSON log level |
| `THIRD_PARTY_LOG_LEVEL` | `WARNING` | Separate Kafka/library log level |
| `LOG_FILE_ENABLED` | `true` | Enable per-service JSONL files |
| `LOG_DIRECTORY` | `logs` | JSONL directory mounted by Filebeat |
| `RUN_ID` | `local` | Default cross-service log correlation ID |
| `ORDER_RANDOM_SEED` | `42` | Deterministic event generation seed |

Tests call the logging configuration with file output disabled or point it at pytest temporary
directories, so unit tests do not write to the real `logs/` directory.

## Quality checks

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
```

The workflow in `.github/workflows/ci.yml` executes these checks with Python 3.12 and the
locked dependencies on every push to `main` and every pull request. Elasticsearch-dependent
integration coverage skips when Elasticsearch is unavailable in the CI runner. Run
`./scripts/check-pipeline.sh`, `./scripts/check-log-pipeline.sh`, and
`./scripts/check-slow-consumer-scenario.sh` locally for real service validation.

## Next steps

The current milestone stops after Prometheus metrics and the single slow-consumer scenario.
Potential future
stages, each requiring an explicit implementation request, are:

1. Add Grafana only after an explicit implementation request.
2. Define additional incident scenarios and service-level indicators separately.
3. Add runbook retrieval, LangGraph, and agent workflows only after the observability foundations are
   validated.
4. Evaluate OpenTelemetry, MCP, Kubernetes, and application containerization separately
   rather than expanding the local MVP implicitly.

## License

IncidentOps is available under the [MIT License](LICENSE).

## Troubleshooting

- **Filebeat does not read files:** confirm that `LOG_FILE_ENABLED=true`, the applications
  created `logs/*.jsonl`, and each file contains at least 128 bytes for the configured
  fingerprint. Run `filebeat test config` and inspect the Filebeat logs for filestream
  harvester activity.
- **The `logs/` directory has permission errors:** create it from WSL with `mkdir -p logs`,
  verify directory traversal and file readability with `ls -ld logs logs/*.jsonl`, and do not
  make the read-only Filebeat mount writable.
- **Elasticsearch receives no document:** run `filebeat test output`, verify the template with
  `./scripts/initialize-elasticsearch.sh`, inspect `docker compose logs --tail=100 filebeat`,
  then use a bounded CLI search with the correct time window.
- **An index has the wrong mapping:** the initialization script reports the incompatible field
  without deleting data. Correct the versioned template and use a new index or an explicitly
  authorized scoped migration; never delete an application index as an automatic fix.
- **Logs appear duplicated:** preserve the `filebeat_data` volume, filestream input ID, and
  fingerprint settings. Deleting the registry or changing file identity makes Filebeat treat
  old files as new.
- **The same log appears on stdout and in a file:** this is expected dual-output behavior, not
  duplicate Elasticsearch ingestion. Set `LOG_FILE_ENABLED=false` when only stdout is wanted;
  Filebeat reads only the files.
- **Elasticsearch is not healthy:** inspect `docker compose logs elasticsearch`, available
  memory, and `sysctl vm.max_map_count`.
- **A port is already used:** change the corresponding host port in `.env`, then run
  `docker compose config` before restarting.
- **Prometheus cannot reach WSL services:** confirm the application binds `METRICS_HOST` to
  `0.0.0.0`, verify `host.docker.internal` resolves inside the container, and test
  `wget -qO- http://host.docker.internal:8001/metrics` from `docker compose exec prometheus`.
- **Producer or consumer target is down:** start that Python process, open its `/metrics`
  endpoint from WSL, then inspect `http://localhost:9090/targets`. A stopped application target
  is expected and does not affect the Prometheus container healthcheck.
- **Port 8001, 8002, or 9090 is occupied:** stop the unrelated owner or configure an unused
  application/host port. If application ports change, update the versioned Prometheus scrape
  targets consistently, run `docker compose config`, and restart Prometheus.
- **No lag samples are available:** wait for the consumer partition assignment and at least
  two scrapes, check the consumer `lag_collection_failed` warnings, and confirm the topic and
  consumer group match the running process.
- **Histogram queries return no value:** process multiple messages, wait for at least two
  scrapes within the 30-second or manual query rate window, and verify the consumer target is
  healthy during collection.
- **`slow_processing` logs are missing:** ensure the explicit delay exceeds the configured
  threshold, `LOG_FILE_ENABLED=true`, Filebeat is healthy, and the log query uses the scenario
  `run_id` and a current time window.
- **Kafka is unavailable through localhost:** WSL applications must use
  `localhost:${KAFKA_EXTERNAL_PORT}`; containers must use `kafka:29092`.
- **A pipeline check times out:** inspect the temporary consumer stdout path printed by the
  script and the relevant Kafka, Filebeat, Elasticsearch, or PostgreSQL service logs.
