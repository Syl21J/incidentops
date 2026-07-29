"""Unit tests for environment-based configuration."""

from pathlib import Path

import pytest

from incidentops.config import Settings


def test_default_application_endpoints_use_localhost(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    settings = Settings()

    assert settings.kafka_bootstrap_servers == "localhost:9092"
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5432


def test_environment_overrides_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KAFKA_TOPIC", "orders.test")
    monkeypatch.setenv("ORDER_RANDOM_SEED", "99")
    monkeypatch.setenv("THIRD_PARTY_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("LOG_FILE_ENABLED", "false")
    monkeypatch.setenv("LOG_DIRECTORY", "temporary-logs")
    monkeypatch.setenv("RUN_ID", "config-test")

    settings = Settings()

    assert settings.kafka_topic == "orders.test"
    assert settings.order_random_seed == 99
    assert settings.third_party_log_level == "ERROR"
    assert settings.log_file_enabled is False
    assert settings.log_directory == Path("temporary-logs")
    assert settings.run_id == "config-test"
