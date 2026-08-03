"""Replaceable structured chat-model providers with a hard call budget."""

from __future__ import annotations

from collections import deque
from collections.abc import Sequence
from typing import Protocol, TypeVar, cast

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ValidationError

from incidentops.config import Settings
from incidentops.investigation.models import MAX_MODEL_CALLS

StructuredOutput = TypeVar("StructuredOutput", bound=BaseModel)


class ModelConfigurationError(RuntimeError):
    """Report missing or unsafe live-model configuration without exposing secrets."""


class StructuredModelError(RuntimeError):
    """Report a failed or invalid structured model response."""


class ModelCallLimitError(StructuredModelError):
    """Report exhaustion of the global model-call budget."""


class StructuredModelProvider(Protocol):
    """Small replaceable interface shared by live and scripted providers."""

    @property
    def call_count(self) -> int:
        """Return the number of attempted structured calls."""

        ...

    def invoke_structured(
        self,
        schema: type[StructuredOutput],
        messages: Sequence[BaseMessage],
    ) -> StructuredOutput:
        """Return one response validated against the requested Pydantic schema."""

        ...


def _validate_output[OutputModel: BaseModel](
    schema: type[OutputModel],
    value: object,
) -> OutputModel:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python")
    try:
        return schema.model_validate(value)
    except ValidationError as error:
        raise StructuredModelError("model returned invalid structured output") from error


class _CallBudget:
    def __init__(self, max_calls: int = MAX_MODEL_CALLS) -> None:
        if not 1 <= max_calls <= MAX_MODEL_CALLS:
            raise ValueError(f"max_calls must be between one and {MAX_MODEL_CALLS}")
        self._max_calls = max_calls
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    def _reserve_call(self) -> None:
        if self._call_count >= self._max_calls:
            raise ModelCallLimitError("structured model-call limit reached")
        self._call_count += 1


class OpenAICompatibleModelProvider(_CallBudget):
    """Live provider using only explicit OpenAI-compatible environment configuration."""

    def __init__(self, settings: Settings) -> None:
        if settings.llm_provider != "openai-compatible":
            raise ModelConfigurationError("live provider requires LLM_PROVIDER=openai-compatible")
        if settings.llm_model is None or settings.llm_model == "replace-with-model-name":
            raise ModelConfigurationError("LLM_MODEL is required for live investigations")
        api_key = (
            settings.llm_api_key.get_secret_value().strip()
            if settings.llm_api_key is not None
            else ""
        )
        if not api_key or api_key == "replace-with-api-key":
            raise ModelConfigurationError("LLM_API_KEY is required for live investigations")
        super().__init__()
        self._model = ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            temperature=settings.llm_temperature,
            timeout=settings.llm_timeout_seconds,
            max_retries=settings.llm_max_retries,
        )

    def invoke_structured(
        self,
        schema: type[StructuredOutput],
        messages: Sequence[BaseMessage],
    ) -> StructuredOutput:
        """Invoke native JSON-schema output and validate it again locally."""

        self._reserve_call()
        try:
            runnable = self._model.with_structured_output(
                schema,
                method="json_schema",
                strict=True,
            )
            raw_result = runnable.invoke(list(messages))
        except Exception as error:
            raise StructuredModelError("live structured model request failed") from error
        return _validate_output(schema, raw_result)


class ScriptedModelProvider(_CallBudget):
    """Explicit non-production provider validating queued responses identically."""

    def __init__(self, responses: Sequence[object], *, max_calls: int = MAX_MODEL_CALLS) -> None:
        super().__init__(max_calls=max_calls)
        self._responses = deque(responses)

    def invoke_structured(
        self,
        schema: type[StructuredOutput],
        messages: Sequence[BaseMessage],
    ) -> StructuredOutput:
        """Consume one scripted payload through the same Pydantic schema boundary."""

        del messages
        self._reserve_call()
        if not self._responses:
            raise StructuredModelError("scripted-test response queue is empty")
        response = self._responses.popleft()
        if isinstance(response, Exception):
            raise StructuredModelError("scripted-test response raised an error") from response
        return _validate_output(schema, response)


def create_model_provider(
    settings: Settings,
    *,
    scripted_responses: Sequence[object] | None = None,
) -> StructuredModelProvider:
    """Create the selected provider without any silent fake-model fallback."""

    if settings.llm_provider == "scripted-test":
        if scripted_responses is None:
            raise ModelConfigurationError(
                "scripted-test requires explicit non-production scripted responses"
            )
        return ScriptedModelProvider(scripted_responses)
    return cast(StructuredModelProvider, OpenAICompatibleModelProvider(settings))


def slow_consumer_scripted_responses() -> list[object]:
    """Return deterministic non-production outputs for workflow validation.

    These payloads do not read scenario ground truth. Deterministic verification
    still decides whether the cited live evidence supports the proposed cause.
    """

    plan = {
        "tasks": [
            {"task_type": task_name, "reason": "Collect bounded read-only evidence."}
            for task_name in (
                "check_consumer_lag",
                "check_processing_latency",
                "compare_producer_consumer_rates",
                "find_slow_processing_logs",
                "find_database_errors",
                "find_kafka_errors",
            )
        ]
    }
    hypothesis = {
        "hypotheses": [
            {
                "cause_code": "slow_consumer_processing",
                "confidence": 0.9,
                "supporting_evidence_ids": [
                    "metric-consumer-lag-summary",
                    "metric-processing-latency-p95",
                    "metric-producer-consumer-rate-comparison",
                    "log-slow-processing-summary",
                    "negative-no-database-errors",
                    "negative-no-kafka-errors",
                ],
                "contradicting_evidence_ids": [],
                "reasoning_summary": (
                    "The bounded signals consistently support slow consumer processing."
                ),
            }
        ]
    }
    return [plan, hypothesis, hypothesis, hypothesis]
