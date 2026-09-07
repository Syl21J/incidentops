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
    assert settings.kafka_auto_offset_reset == "earliest"
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5432
    assert settings.prometheus_url == "http://localhost:9090"
    assert settings.grafana_url == "http://localhost:3000"
    assert settings.grafana_admin_user == "admin"
    assert settings.grafana_timeout_seconds == 5
    assert settings.producer_metrics_port == 8001
    assert settings.consumer_metrics_port == 8002
    assert settings.consumer_processing_delay_ms == 0
    assert settings.consumer_database_delay_ms == 0
    assert settings.slow_database_threshold_ms == 500
    assert settings.llm_provider == "openai-compatible"
    assert settings.embedding_provider == "sentence-transformers"
    assert settings.embedding_model == "all-MiniLM-L6-v2"
    assert settings.embedding_device == "cpu"
    assert settings.knowledge_enabled is False
    assert settings.knowledge_required is False
    assert settings.knowledge_retrieval_mode == "hybrid"
    assert settings.knowledge_top_k == 5
    assert settings.knowledge_candidate_k == 40
    assert settings.llm_temperature == 0
    assert settings.llm_timeout_seconds == 60
    assert settings.investigation_max_tool_calls == 10


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
    monkeypatch.setenv("EMBEDDING_PROVIDER", "deterministic-test")

    settings = Settings()

    assert settings.kafka_topic == "orders.test"
    assert settings.order_random_seed == 99
    assert settings.third_party_log_level == "ERROR"
    assert settings.log_file_enabled is False
    assert settings.log_directory == Path("temporary-logs")
    assert settings.run_id == "config-test"
    assert settings.embedding_provider == "deterministic-test"


def test_processing_delay_configuration_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CONSUMER_PROCESSING_DELAY_MS", "5001")

    with pytest.raises(ValueError, match="less than or equal to 5000"):
        Settings()


def test_database_delay_configuration_is_disabled_by_default_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    assert Settings().consumer_database_delay_ms == 0

    monkeypatch.setenv("CONSUMER_DATABASE_DELAY_MS", "5001")
    with pytest.raises(ValueError, match="less than or equal to 5000"):
        Settings()


def test_kafka_offset_reset_policy_is_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "invalid")

    with pytest.raises(ValueError, match="earliest"):
        Settings()


def test_knowledge_configuration_is_explicit_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KNOWLEDGE_REQUIRED", "true")
    monkeypatch.setenv("KNOWLEDGE_ENABLED", "false")
    with pytest.raises(ValueError, match="KNOWLEDGE_REQUIRED"):
        Settings()

    monkeypatch.setenv("KNOWLEDGE_REQUIRED", "false")
    monkeypatch.setenv("KNOWLEDGE_TOP_K", "6")
    monkeypatch.setenv("KNOWLEDGE_CANDIDATE_K", "5")
    with pytest.raises(ValueError, match="KNOWLEDGE_CANDIDATE_K"):
        Settings()
