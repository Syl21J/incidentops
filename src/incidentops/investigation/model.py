"""Replaceable structured chat-model providers with a hard call budget."""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Sequence
from enum import StrEnum
from typing import Protocol, TypeVar, cast

from langchain_core.messages import BaseMessage
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, ValidationError

from incidentops.config import Settings
from incidentops.investigation.models import (
    MAX_MODEL_CALLS,
    HypothesisSet,
)

StructuredOutput = TypeVar("StructuredOutput", bound=BaseModel)


class ModelConfigurationError(RuntimeError):
    """Report missing or unsafe live-model configuration without exposing secrets."""


class ModelErrorCategory(StrEnum):
    """Safe, stable categories for failures at the structured model boundary."""

    QUOTA_EXCEEDED = "quota_exceeded"
    TIMEOUT = "timeout"
    REQUEST_REJECTED = "request_rejected"
    INVALID_STRUCTURED_RESPONSE = "invalid_structured_response"
    REQUEST_FAILED = "request_failed"
    CALL_LIMIT = "call_limit"


class StructuredModelError(RuntimeError):
    """Report a model failure without retaining provider payloads or credentials."""

    def __init__(
        self,
        message: str,
        *,
        category: ModelErrorCategory = ModelErrorCategory.INVALID_STRUCTURED_RESPONSE,
        field: str | None = None,
        rule: str | None = None,
    ) -> None:
        self.category = category
        self.field = field
        self.rule = rule
        details = []
        if field is not None:
            details.append(f"field={field}")
        if rule is not None:
            details.append(f"rule={rule}")
        suffix = f" ({', '.join(details)})" if details else ""
        super().__init__(f"{category.value}: {message}{suffix}")


class ModelCallLimitError(StructuredModelError):
    """Report exhaustion of the global model-call budget."""

    def __init__(self, message: str) -> None:
        super().__init__(message, category=ModelErrorCategory.CALL_LIMIT)


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
        raise _validation_error(error) from error


def _validation_error(error: ValidationError) -> StructuredModelError:
    """Reduce Pydantic details to a safe field path and violated rule."""

    details = error.errors(
        include_url=False,
        include_context=False,
        include_input=False,
    )
    first = details[0] if details else {}
    location = first.get("loc", ())
    field = ".".join("*" if isinstance(part, int) else str(part) for part in location)
    rule = first.get("type")
    return StructuredModelError(
        "model returned invalid structured output",
        category=ModelErrorCategory.INVALID_STRUCTURED_RESPONSE,
        field=field or None,
        rule=rule if isinstance(rule, str) else None,
    )


def _live_model_error(error: Exception) -> StructuredModelError:
    """Classify a provider exception using metadata only, never its response body."""

    if isinstance(error, ValidationError):
        return _validation_error(error)
    status_code = getattr(error, "status_code", None)
    class_names = {item.__name__.lower() for item in type(error).__mro__}
    if status_code == 429 or "ratelimiterror" in class_names:
        category = ModelErrorCategory.QUOTA_EXCEEDED
        message = "live structured model quota was exceeded"
    elif status_code == 408 or any("timeout" in name for name in class_names):
        category = ModelErrorCategory.TIMEOUT
        message = "live structured model request timed out"
    elif isinstance(status_code, int) and 400 <= status_code < 500:
        category = ModelErrorCategory.REQUEST_REJECTED
        message = "live structured model request was rejected"
    elif any(
        marker in name
        for name in class_names
        for marker in ("outputparser", "jsondecode", "validation")
    ):
        category = ModelErrorCategory.INVALID_STRUCTURED_RESPONSE
        message = "model returned invalid structured output"
    else:
        category = ModelErrorCategory.REQUEST_FAILED
        message = "live structured model request failed"
    return StructuredModelError(message, category=category)


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
            raise _live_model_error(error) from error
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


def _message_payload(messages: Sequence[BaseMessage]) -> dict[str, object]:
    """Load only the structured JSON payload produced by trusted graph code."""

    if not messages or not isinstance(messages[-1].content, str):
        raise StructuredModelError("deterministic-test received no structured payload")
    try:
        payload = json.loads(messages[-1].content)
    except json.JSONDecodeError as error:
        raise StructuredModelError("deterministic-test received invalid JSON") from error
    if not isinstance(payload, dict):
        raise StructuredModelError("deterministic-test payload must be an object")
    return payload


def _evidence_payload(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    evidence = payload.get("evidence", [])
    if not isinstance(evidence, list):
        raise StructuredModelError("deterministic-test evidence must be a list")
    indexed: dict[str, dict[str, object]] = {}
    for item in evidence:
        if not isinstance(item, dict) or not isinstance(item.get("evidence_id"), str):
            raise StructuredModelError("deterministic-test evidence item is invalid")
        indexed[cast(str, item["evidence_id"])] = cast(dict[str, object], item)
    return indexed


def _raw(indexed: dict[str, dict[str, object]], evidence_id: str) -> dict[str, object]:
    item = indexed.get(evidence_id, {})
    raw = item.get("raw_value_summary", {})
    return raw if isinstance(raw, dict) else {}


def _available_ids(indexed: dict[str, dict[str, object]]) -> set[str]:
    return {
        evidence_id
        for evidence_id, item in indexed.items()
        if item.get("availability") == "available"
    }


_CAUSE_DOCUMENTS: dict[str, tuple[str, ...]] = {
    "slow_consumer_processing": (
        "incident_slow_consumer_processing",
        "metric_processing_duration",
    ),
    "database_latency": ("incident_database_latency_backlog",),
    "traffic_spike": (
        "incident_traffic_spike_backlog",
        "metric_producer_consumer_rates",
    ),
    "malformed_event": ("runbook_malformed_order_events",),
}


def _knowledge_ids(payload: dict[str, object], cause: str) -> list[str]:
    references = payload.get("knowledge_references", [])
    expected_documents = set(_CAUSE_DOCUMENTS.get(cause, ()))
    if not isinstance(references, list):
        return []
    return [
        cast(str, item["knowledge_reference_id"])
        for item in references
        if isinstance(item, dict)
        and item.get("document_id") in expected_documents
        and isinstance(item.get("knowledge_reference_id"), str)
    ][:10]


def _deterministic_hypothesis(payload: dict[str, object]) -> dict[str, object]:
    """Classify only the same structured observations supplied to a live model."""

    indexed = _evidence_payload(payload)
    available = _available_ids(indexed)
    lag = _raw(indexed, "metric-consumer-lag-summary")
    latency = _raw(indexed, "metric-processing-latency-p95")
    rates = _raw(indexed, "metric-producer-consumer-rate-comparison")
    logs = _raw(indexed, "log-slow-processing-summary")

    lag_increasing = lag.get("trend") == "increasing"
    processing_elevated = latency.get("processing_state") == "elevated"
    processing_normal = latency.get("processing_state") == "normal"
    database_elevated = latency.get("database_state") == "elevated"
    database_normal = latency.get("database_state") == "normal"
    producer_surge = rates.get("producer_surge") is True
    processing_errors = rates.get("processing_errors_present") is True
    valid_processing = rates.get("valid_processing_present") is True
    slow_count = logs.get("slow_processing_count", 0)
    database_slow_count = logs.get("database_operation_slow_count", 0)
    invalid_count = logs.get("invalid_event_count", 0)
    negative_database = "negative-no-database-errors" in available
    negative_kafka = "negative-no-kafka-errors" in available

    cause = "insufficient_evidence"
    if (
        processing_errors
        and valid_processing
        and isinstance(invalid_count, int | float)
        and invalid_count > 0
        and processing_normal
        and database_normal
        and not producer_surge
        and negative_database
        and negative_kafka
    ):
        cause = "malformed_event"
    elif (
        lag_increasing
        and processing_elevated
        and database_elevated
        and isinstance(database_slow_count, int | float)
        and database_slow_count > 0
        and not producer_surge
        and not processing_errors
        and negative_database
        and negative_kafka
    ):
        cause = "database_latency"
    elif (
        lag_increasing
        and processing_normal
        and database_normal
        and producer_surge
        and not processing_errors
        and negative_database
        and negative_kafka
    ):
        cause = "traffic_spike"
    elif (
        lag_increasing
        and processing_elevated
        and database_normal
        and isinstance(slow_count, int | float)
        and slow_count > 0
        and not producer_surge
        and not processing_errors
        and negative_database
        and negative_kafka
    ):
        cause = "slow_consumer_processing"

    if cause == "insufficient_evidence":
        return {
            "hypotheses": [
                {
                    "cause_code": cause,
                    "confidence": 0.0,
                    "supporting_evidence_ids": [],
                    "contradicting_evidence_ids": [],
                    "knowledge_reference_ids": [],
                    "reasoning_summary": (
                        "The bounded evidence does not isolate one supported cause."
                    ),
                }
            ]
        }
    supporting = sorted(available)
    return {
        "hypotheses": [
            {
                "cause_code": cause,
                "confidence": 0.95,
                "supporting_evidence_ids": supporting,
                "contradicting_evidence_ids": [],
                "knowledge_reference_ids": _knowledge_ids(payload, cause),
                "reasoning_summary": (
                    "The bounded live evidence matches one distinct incident signature."
                ),
            }
        ]
    }


class EvidenceDrivenModelProvider(_CallBudget):
    """Non-production provider driven only by model-visible evidence messages."""

    def invoke_structured(
        self,
        schema: type[StructuredOutput],
        messages: Sequence[BaseMessage],
    ) -> StructuredOutput:
        """Return evidence-derived hypotheses through Pydantic."""

        self._reserve_call()
        payload = _message_payload(messages)
        if schema is not HypothesisSet:
            raise StructuredModelError("deterministic-test received an unsupported schema")
        response: object = _deterministic_hypothesis(payload)
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
    if settings.llm_provider == "deterministic-test":
        if scripted_responses is not None:
            raise ModelConfigurationError(
                "deterministic-test derives responses from evidence and accepts no script"
            )
        return EvidenceDrivenModelProvider()
    return cast(StructuredModelProvider, OpenAICompatibleModelProvider(settings))


def slow_consumer_scripted_responses() -> list[object]:
    """Return deterministic non-production outputs for workflow validation.

    These payloads do not read scenario ground truth. Deterministic verification
    still decides whether the cited live evidence supports the proposed cause.
    """

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
    return [hypothesis, hypothesis, hypothesis]
