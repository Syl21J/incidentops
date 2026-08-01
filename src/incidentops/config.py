"""Application configuration loaded from environment variables."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings shared by the producer and consumer."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "orders.v1"
    kafka_consumer_group: str = "incidentops-order-consumer-v1"

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_user: str = "incidentops"
    postgres_password: SecretStr = SecretStr("change-me-local-only")
    postgres_db: str = "incidentops"

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    third_party_log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "WARNING"
    log_file_enabled: bool = True
    log_directory: Path = Path("logs")
    run_id: str = "local"

    elasticsearch_url: str = "http://localhost:9200"
    prometheus_url: str = "http://localhost:9090"
    order_random_seed: int = 42

    metrics_enabled: bool = True
    metrics_host: str = "0.0.0.0"
    producer_metrics_port: int = Field(default=8001, ge=1, le=65535)
    consumer_metrics_port: int = Field(default=8002, ge=1, le=65535)
    consumer_lag_update_interval_seconds: float = Field(default=2.0, ge=0.5, le=60.0)
    consumer_processing_delay_ms: int = Field(default=0, ge=0, le=5_000)
    slow_processing_threshold_ms: int = Field(default=500, ge=0, le=60_000)

    @field_validator(
        "kafka_bootstrap_servers",
        "kafka_topic",
        "kafka_consumer_group",
        "postgres_host",
        "postgres_user",
        "postgres_db",
        "run_id",
        "elasticsearch_url",
        "prometheus_url",
        "metrics_host",
    )
    @classmethod
    def reject_blank_values(cls, value: str) -> str:
        """Reject empty configuration values early."""

        normalized = value.strip()
        if not normalized:
            raise ValueError("configuration value must not be blank")
        return normalized


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance for command-line applications."""

    return Settings()
