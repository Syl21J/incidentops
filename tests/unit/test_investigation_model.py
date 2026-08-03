"""Unit coverage for live and scripted structured model providers."""

from pathlib import Path

import pytest
from pydantic import SecretStr

from incidentops.config import Settings
from incidentops.investigation.model import (
    ModelCallLimitError,
    ModelConfigurationError,
    ScriptedModelProvider,
    StructuredModelError,
    create_model_provider,
)
from incidentops.investigation.models import InvestigationPlan, InvestigationTaskType


def valid_plan_payload() -> dict[str, object]:
    """Return the complete allow-listed plan used by scripted tests."""

    return {
        "tasks": [
            {"task_type": task_type.value, "reason": "Collect bounded evidence."}
            for task_type in InvestigationTaskType
        ]
    }


def test_live_provider_fails_clearly_without_model_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = Settings()

    with pytest.raises(ModelConfigurationError, match="LLM_MODEL"):
        create_model_provider(settings)

    configured_model = Settings(llm_model="compatible-model")
    with pytest.raises(ModelConfigurationError, match="LLM_API_KEY"):
        create_model_provider(configured_model)

    placeholder_settings = Settings(
        llm_model="replace-with-model-name",
        llm_api_key=SecretStr("replace-with-api-key"),
    )
    with pytest.raises(ModelConfigurationError, match="LLM_MODEL"):
        create_model_provider(placeholder_settings)


def test_scripted_provider_must_be_selected_and_supplied_explicitly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = Settings(llm_provider="scripted-test")

    with pytest.raises(ModelConfigurationError, match="explicit"):
        create_model_provider(settings)

    provider = create_model_provider(settings, scripted_responses=[valid_plan_payload()])
    result = provider.invoke_structured(InvestigationPlan, [])

    assert len(result.tasks) == 6
    assert provider.call_count == 1


def test_scripted_provider_uses_the_requested_pydantic_schema() -> None:
    provider = ScriptedModelProvider([{"tasks": [{"task_type": "run_shell"}]}])

    with pytest.raises(StructuredModelError, match="invalid structured output"):
        provider.invoke_structured(InvestigationPlan, [])
    assert provider.call_count == 1


def test_model_call_budget_is_hard_bounded() -> None:
    provider = ScriptedModelProvider(
        [valid_plan_payload(), valid_plan_payload()],
        max_calls=1,
    )
    provider.invoke_structured(InvestigationPlan, [])

    with pytest.raises(ModelCallLimitError, match="limit"):
        provider.invoke_structured(InvestigationPlan, [])
