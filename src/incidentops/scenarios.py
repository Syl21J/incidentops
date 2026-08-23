"""Versioned and strictly validated executable incident scenario manifests."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{2,79}$")]
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1_000)]


class StrictScenarioModel(BaseModel):
    """Reject unknown fields throughout the scenario boundary."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class ScenarioKind(StrEnum):
    """Closed executable scenario allowlist."""

    SLOW_CONSUMER = "slow_consumer"
    DATABASE_LATENCY = "database_latency"
    TRAFFIC_SPIKE = "traffic_spike"
    MALFORMED_EVENTS = "malformed_events"


class RootCause(StrictScenarioModel):
    """Expected service and root-cause classification."""

    code: Literal[
        "slow_consumer_processing",
        "database_latency",
        "traffic_spike",
        "malformed_event",
    ]
    service: Literal["order-producer", "order-consumer"]


class TelemetrySignal(StrEnum):
    """Closed signals that the evaluator may expect from live evidence."""

    CONSUMER_LAG = "consumer_lag"
    PROCESSING_LATENCY = "processing_latency"
    DATABASE_LATENCY = "database_latency"
    PRODUCER_RATE_CHANGE = "producer_rate_change"
    PROCESSING_ERRORS = "processing_errors"
    VALID_PROCESSING = "valid_processing"


class TelemetryBehavior(StrEnum):
    """Categorical behaviors calculated outside the model."""

    INCREASING = "increasing"
    ELEVATED = "elevated"
    NORMAL = "normal"
    SURGING = "surging"
    STABLE = "stable"
    PRESENT = "present"
    ABSENT = "absent"


_SIGNAL_BEHAVIORS: dict[TelemetrySignal, frozenset[TelemetryBehavior]] = {
    TelemetrySignal.CONSUMER_LAG: frozenset(
        {TelemetryBehavior.INCREASING, TelemetryBehavior.STABLE}
    ),
    TelemetrySignal.PROCESSING_LATENCY: frozenset(
        {TelemetryBehavior.ELEVATED, TelemetryBehavior.NORMAL}
    ),
    TelemetrySignal.DATABASE_LATENCY: frozenset(
        {TelemetryBehavior.ELEVATED, TelemetryBehavior.NORMAL}
    ),
    TelemetrySignal.PRODUCER_RATE_CHANGE: frozenset(
        {TelemetryBehavior.SURGING, TelemetryBehavior.STABLE}
    ),
    TelemetrySignal.PROCESSING_ERRORS: frozenset(
        {TelemetryBehavior.PRESENT, TelemetryBehavior.ABSENT}
    ),
    TelemetrySignal.VALID_PROCESSING: frozenset(
        {TelemetryBehavior.PRESENT, TelemetryBehavior.ABSENT}
    ),
}


class ExpectedMetric(StrictScenarioModel):
    """One categorical telemetry expectation used only by evaluation."""

    signal: TelemetrySignal
    behavior: TelemetryBehavior

    @model_validator(mode="after")
    def validate_signal_behavior(self) -> ExpectedMetric:
        """Reject combinations without deterministic evaluation semantics."""

        if self.behavior not in _SIGNAL_BEHAVIORS[self.signal]:
            raise ValueError(f"{self.behavior.value} is invalid for {self.signal.value}")
        return self


class ExpectedLog(StrictScenarioModel):
    """Expected structured log evidence."""

    service: Literal["order-producer", "order-consumer"]
    event_type: Literal[
        "slow_processing",
        "database_operation_slow",
        "invalid_event_skipped",
    ]


class ExecutionBase(StrictScenarioModel):
    """Bounds shared by every explicit fault-injection run."""

    seed: int = 20260801
    timeout_seconds: int = Field(default=120, ge=30, le=300)
    slow_processing_threshold_ms: int = Field(default=500, ge=50, le=5_000)
    database_slow_threshold_ms: int = Field(default=500, ge=50, le=5_000)


class SlowConsumerExecution(ExecutionBase):
    """Bounded artificial delay in the complete consumer processing path."""

    kind: Literal[ScenarioKind.SLOW_CONSUMER]
    event_count: int = Field(ge=20, le=500)
    producer_rate: float = Field(gt=0, le=100)
    processing_delay_ms: int = Field(ge=50, le=5_000)
    minimum_lag: float = Field(default=20.0, ge=1, le=10_000)


class DatabaseLatencyExecution(ExecutionBase):
    """Bounded PostgreSQL delay applied only inside the measured transaction."""

    kind: Literal[ScenarioKind.DATABASE_LATENCY]
    event_count: int = Field(ge=20, le=500)
    producer_rate: float = Field(gt=0, le=100)
    database_delay_ms: int = Field(ge=50, le=5_000)
    minimum_lag: float = Field(default=20.0, ge=1, le=10_000)


class TrafficSpikeExecution(ExecutionBase):
    """Observed baseline followed by a bounded production burst."""

    kind: Literal[ScenarioKind.TRAFFIC_SPIKE]
    baseline_event_count: int = Field(ge=10, le=300)
    baseline_rate: float = Field(gt=0, le=100)
    burst_event_count: int = Field(ge=20, le=1_000)
    burst_rate: float = Field(gt=0, le=500)
    minimum_lag: float = Field(default=10.0, ge=1, le=10_000)

    @model_validator(mode="after")
    def validate_rate_change(self) -> TrafficSpikeExecution:
        """Make the injected burst meaningfully larger than the baseline."""

        if self.burst_rate < self.baseline_rate * 2:
            raise ValueError("burst_rate must be at least twice baseline_rate")
        return self


class MalformedEventsExecution(ExecutionBase):
    """Deterministic mixture of valid and deliberately invalid Kafka payloads."""

    kind: Literal[ScenarioKind.MALFORMED_EVENTS]
    valid_event_count: int = Field(ge=5, le=500)
    malformed_event_count: int = Field(ge=1, le=500)
    producer_rate: float = Field(gt=0, le=100)

    @model_validator(mode="after")
    def validate_total_count(self) -> MalformedEventsExecution:
        """Keep the mixed batch small enough for a local acceptance run."""

        if self.valid_event_count + self.malformed_event_count > 750:
            raise ValueError("combined valid and malformed event count must not exceed 750")
        return self


ScenarioExecution = Annotated[
    SlowConsumerExecution
    | DatabaseLatencyExecution
    | TrafficSpikeExecution
    | MalformedEventsExecution,
    Field(discriminator="kind"),
]


_EXPECTED_ROOT_CAUSES = {
    ScenarioKind.SLOW_CONSUMER: "slow_consumer_processing",
    ScenarioKind.DATABASE_LATENCY: "database_latency",
    ScenarioKind.TRAFFIC_SPIKE: "traffic_spike",
    ScenarioKind.MALFORMED_EVENTS: "malformed_event",
}


class ScenarioManifest(StrictScenarioModel):
    """Ground truth and bounded execution parameters kept outside LangGraph."""

    schema_version: Literal[2]
    id: Identifier
    title: Text
    description: Text
    execution: ScenarioExecution
    root_cause: RootCause
    expected_metrics: list[ExpectedMetric] = Field(min_length=1, max_length=8)
    expected_logs: list[ExpectedLog] = Field(default_factory=list, max_length=8)
    negative_evidence: list[Identifier] = Field(min_length=1, max_length=8)
    expected_knowledge_documents: list[Identifier] = Field(min_length=1, max_length=10)
    acceptable_actions: list[Identifier] = Field(min_length=1, max_length=8)
    forbidden_actions: list[Identifier] = Field(min_length=1, max_length=8)

    @field_validator(
        "negative_evidence",
        "expected_knowledge_documents",
        "acceptable_actions",
        "forbidden_actions",
    )
    @classmethod
    def require_unique_identifiers(cls, value: list[str]) -> list[str]:
        """Reject duplicate expectations that would distort benchmark recall."""

        if len(value) != len(set(value)):
            raise ValueError("scenario identifier lists must not contain duplicates")
        return value

    @model_validator(mode="after")
    def validate_identity_and_ground_truth(self) -> ScenarioManifest:
        """Bind each executable fault to one stable scenario and root cause."""

        expected_id = f"{self.execution.kind.value}_v1"
        if self.id != expected_id:
            raise ValueError(f"scenario id must be {expected_id}")
        expected_cause = _EXPECTED_ROOT_CAUSES[self.execution.kind]
        if self.root_cause.code != expected_cause:
            raise ValueError(f"{self.execution.kind.value} root cause must be {expected_cause}")
        if len({item.signal for item in self.expected_metrics}) != len(self.expected_metrics):
            raise ValueError("expected metric signals must be unique")
        if len({item.event_type for item in self.expected_logs}) != len(self.expected_logs):
            raise ValueError("expected log event types must be unique")
        return self


def load_scenario_manifest(path: Path) -> ScenarioManifest:
    """Load one UTF-8 YAML manifest and validate its complete structure."""

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return ScenarioManifest.model_validate(payload)
