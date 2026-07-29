# IncidentOps

IncidentOps implements a local order-processing path and centralized application-log search:

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
```

The Python applications are not containerized. They always write JSON logs to stdout and can
also append the same records to local JSON Lines files. Filebeat is the only log shipper: it
tails those files, remembers offsets in a persistent registry, and sends parsed fields to
Elasticsearch. The applications have no Elasticsearch dependency and continue to process
orders when Filebeat or Elasticsearch is unavailable.

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

## Local endpoints

| Service | WSL endpoint | Container endpoint |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `postgres:5432` |
| Elasticsearch | `http://localhost:9200` | `http://elasticsearch:9200` |
| Kafka | `localhost:9092` | `kafka:29092` |

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
- **Kafka is unavailable through localhost:** WSL applications must use
  `localhost:${KAFKA_EXTERNAL_PORT}`; containers must use `kafka:29092`.
- **A pipeline check times out:** inspect the temporary consumer stdout path printed by the
  script and the relevant Kafka, Filebeat, Elasticsearch, or PostgreSQL service logs.
