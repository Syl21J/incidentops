"""Unit coverage for live and scripted structured model providers."""

from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import SecretStr

from incidentops.config import Settings
from incidentops.investigation.model import (
    ModelCallLimitError,
    ModelConfigurationError,
    ModelErrorCategory,
    OpenAICompatibleModelProvider,
    ScriptedModelProvider,
    StructuredModelError,
    create_model_provider,
)
from incidentops.investigation.models import (
    InvestigationPlan,
    InvestigationTaskType,
    RootCauseHypothesis,
)


class ProviderStatusError(RuntimeError):
    """Test double carrying only the HTTP status used by the classifier."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__("provider body with sk-sensitive-value")


class ProviderTimeoutError(RuntimeError):
    """Test double whose safe type name identifies a timeout."""


class OutputParserError(RuntimeError):
    """Test double whose safe type name identifies structured parsing."""


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

    with pytest.raises(StructuredModelError, match="invalid structured output") as captured:
        provider.invoke_structured(InvestigationPlan, [])
    assert captured.value.category == ModelErrorCategory.INVALID_STRUCTURED_RESPONSE
    assert captured.value.field == "tasks.*.task_type"
    assert captured.value.rule == "enum"
    assert "run_shell" not in str(captured.value)
    assert provider.call_count == 1


def test_bounded_reasoning_summary_accepts_labels_without_becoming_a_failure() -> None:
    provider = ScriptedModelProvider(
        [
            {
                "cause_code": "slow_consumer_processing",
                "confidence": 0.8,
                "supporting_evidence_ids": ["metric-consumer-lag-summary"],
                "reasoning_summary": "P95 contains sk-sensitive-value.",
            }
        ]
    )

    result = provider.invoke_structured(RootCauseHypothesis, [])

    assert result.reasoning_summary == "P95 contains sk-sensitive-value."


def test_hypothesis_validation_reports_a_safe_specific_reference_rule() -> None:
    provider = ScriptedModelProvider(
        [
            {
                "cause_code": "slow_consumer_processing",
                "confidence": 0.8,
                "supporting_evidence_ids": ["metric-consumer-lag-summary"],
                "contradicting_evidence_ids": ["metric-consumer-lag-summary"],
                "reasoning_summary": "The bounded evidence supports this cause.",
            }
        ]
    )

    with pytest.raises(StructuredModelError) as captured:
        provider.invoke_structured(RootCauseHypothesis, [])

    assert captured.value.category == ModelErrorCategory.INVALID_STRUCTURED_RESPONSE
    assert captured.value.field is None
    assert captured.value.rule == "overlapping_evidence_reference"


@pytest.mark.parametrize(
    ("provider_error", "category"),
    [
        (ProviderStatusError(429), ModelErrorCategory.QUOTA_EXCEEDED),
        (ProviderTimeoutError("provider body with sk-sensitive-value"), ModelErrorCategory.TIMEOUT),
        (ProviderStatusError(400), ModelErrorCategory.REQUEST_REJECTED),
        (
            OutputParserError("provider body with sk-sensitive-value"),
            ModelErrorCategory.INVALID_STRUCTURED_RESPONSE,
        ),
        (ProviderStatusError(500), ModelErrorCategory.REQUEST_FAILED),
    ],
)
def test_live_provider_classifies_failures_without_retaining_provider_details(
    monkeypatch: pytest.MonkeyPatch,
    provider_error: Exception,
    category: ModelErrorCategory,
) -> None:
    settings = Settings(
        llm_provider="openai-compatible",
        llm_model="compatible-model",
        llm_base_url="http://localhost:9999/v1",
        llm_api_key=SecretStr("sk-sensitive-value"),
    )
    provider = OpenAICompatibleModelProvider(settings)
    runnable = Mock()
    runnable.invoke.side_effect = provider_error
    model = Mock()
    model.with_structured_output.return_value = runnable
    monkeypatch.setattr(provider, "_model", model)

    with pytest.raises(StructuredModelError) as captured:
        provider.invoke_structured(InvestigationPlan, [])

    assert captured.value.category == category
    assert category.value in str(captured.value)
    assert "provider body" not in str(captured.value)
    assert "sk-sensitive-value" not in str(captured.value)
    assert provider.call_count == 1


def test_model_call_budget_is_hard_bounded() -> None:
    provider = ScriptedModelProvider(
        [valid_plan_payload(), valid_plan_payload()],
        max_calls=1,
    )
    provider.invoke_structured(InvestigationPlan, [])

    with pytest.raises(ModelCallLimitError, match="limit"):
        provider.invoke_structured(InvestigationPlan, [])
