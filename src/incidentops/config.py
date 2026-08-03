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
    kafka_auto_offset_reset: Literal["earliest", "latest", "error"] = "earliest"

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

    llm_provider: Literal["openai-compatible", "scripted-test"] = "openai-compatible"
    llm_model: str | None = None
    llm_base_url: str | None = None
    llm_api_key: SecretStr | None = None
    llm_temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    llm_timeout_seconds: float = Field(default=30.0, gt=0.0, le=120.0)
    llm_max_retries: int = Field(default=1, ge=0, le=2)

    investigation_max_time_range_hours: int = Field(default=6, ge=1, le=6)
    investigation_max_tool_calls: int = Field(default=10, ge=6, le=10)
    investigation_max_attempts: int = Field(default=2, ge=1, le=2)
    investigation_artifact_directory: Path = Path("artifacts/investigations")

    @field_validator("llm_model", "llm_base_url", mode="before")
    @classmethod
    def normalize_optional_llm_values(cls, value: object) -> object:
        """Treat blank optional model configuration as missing."""

        if isinstance(value, str) and not value.strip():
            return None
        return value

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
