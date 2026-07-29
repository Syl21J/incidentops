# IncidentOps

IncidentOps currently implements one minimal, local order-processing path:

```text
Order producer -> Kafka -> Order consumer -> PostgreSQL
```

Elasticsearch remains available from the infrastructure step but is not connected to the application yet.

## Prerequisites

- WSL2 Ubuntu
- Docker Desktop with WSL integration enabled
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
```

The example password is only a local placeholder. Change it before using this setup beyond local development.

## Infrastructure

Validate and start the existing infrastructure:

```bash
docker compose config
docker compose up -d
./scripts/check-infrastructure.sh
```

Apply the idempotent database migration:

```bash
./scripts/initialize-database.sh
```

Stop the containers without deleting their volumes:

```bash
docker compose down
```

## Run the pipeline

Start the consumer in one terminal:

```bash
uv run python -m incidentops.consumer
```

Produce orders from another terminal:

```bash
uv run python -m incidentops.producer --count 50 --rate 10
```

Run the reproducible end-to-end check:

```bash
./scripts/check-pipeline.sh
```

The check uses an isolated Kafka topic, consumer group, and customer prefix. It removes only the topic, process, and database rows that it creates.

## Local endpoints

| Service | WSL endpoint | Container endpoint |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | `postgres:5432` |
| Elasticsearch | http://localhost:9200 | `http://elasticsearch:9200` |
| Kafka | `localhost:9092` | `kafka:29092` |

## Application configuration

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
| `LOG_LEVEL` | `INFO` | JSON log level |
| `ORDER_RANDOM_SEED` | `42` | Deterministic event generation seed |

## Quality checks

```bash
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
```

## Troubleshooting

- **Docker is unavailable from WSL:** start Docker Desktop, enable the distribution under *Settings > Resources > WSL Integration*, then run `docker version`.
- **Elasticsearch is not healthy:** inspect `docker compose logs elasticsearch`, available memory, and `sysctl vm.max_map_count`.
- **A port is already used:** change the corresponding host port in `.env`, then run `docker compose config` before restarting.
- **Kafka is unavailable through localhost:** WSL applications must use `localhost:${KAFKA_EXTERNAL_PORT}`; containers must use `kafka:29092`.
- **The pipeline times out:** inspect the temporary consumer log path printed by `scripts/check-pipeline.sh`, then check Kafka and PostgreSQL health.
