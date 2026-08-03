"""Strict domain models for bounded incident investigations."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

MAX_TIME_RANGE = timedelta(hours=6)
MAX_HYPOTHESES = 3
MAX_TOOL_CALLS = 10
MAX_INVESTIGATION_ATTEMPTS = 2
MAX_MODEL_CALLS = 4

ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]
Description = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=2_000),
]
Identifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$"),
]
EvidenceIdentifier = Annotated[
    str,
    StringConstraints(pattern=r"^(metric|log|negative)-[a-z0-9]+(?:-[a-z0-9]+)*$"),
]
RawValue = str | int | float | bool | None


class StrictModel(BaseModel):
    """Reject unknown fields and non-finite floating-point values."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class IncidentStatus(StrEnum):
    """Terminal status of a bounded investigation."""

    DIAGNOSED = "diagnosed"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    CONFLICTING_EVIDENCE = "conflicting_evidence"
    OUT_OF_SCOPE = "out_of_scope"
    PIPELINE_ERROR = "pipeline_error"


class ServiceName(StrEnum):
    """Application services supported by the first workflow."""

    ORDER_PRODUCER = "order-producer"
    ORDER_CONSUMER = "order-consumer"


class InvestigationTaskType(StrEnum):
    """Complete tool allowlist available to the planner."""

    CHECK_CONSUMER_LAG = "check_consumer_lag"
    CHECK_PROCESSING_LATENCY = "check_processing_latency"
    COMPARE_PRODUCER_CONSUMER_RATES = "compare_producer_consumer_rates"
    FIND_SLOW_PROCESSING_LOGS = "find_slow_processing_logs"
    FIND_DATABASE_ERRORS = "find_database_errors"
    FIND_KAFKA_ERRORS = "find_kafka_errors"


class RootCauseCode(StrEnum):
    """Root causes representable by the first workflow."""

    SLOW_CONSUMER_PROCESSING = "slow_consumer_processing"
    DATABASE_LATENCY = "database_latency"
    KAFKA_BROKER_FAILURE = "kafka_broker_failure"
    TRAFFIC_SPIKE = "traffic_spike"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"


class RecommendedActionCode(StrEnum):
    """Read-only or human-approved action recommendations."""

    INSPECT_CONSUMER_PROCESSING = "inspect_consumer_processing"
    REDUCE_PROCESSING_LATENCY = "reduce_processing_latency"
    TEMPORARILY_SCALE_CONSUMERS = "temporarily_scale_consumers"
    INSPECT_DATABASE_LATENCY = "inspect_database_latency"
    INSPECT_KAFKA_HEALTH = "inspect_kafka_health"
    COLLECT_MORE_EVIDENCE = "collect_more_evidence"


class EvidenceSource(StrEnum):
    """External read-only evidence sources."""

    PROMETHEUS = "prometheus"
    ELASTICSEARCH = "elasticsearch"


class EvidenceAvailability(StrEnum):
    """Whether an evidence source produced a usable observation."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


class MetricEvidenceType(StrEnum):
    """Metric summaries understood by the verifier."""

    CONSUMER_LAG = "consumer_lag"
    PROCESSING_LATENCY = "processing_latency"
    PRODUCER_CONSUMER_RATES = "producer_consumer_rates"


class LogEvidenceType(StrEnum):
    """Log evidence categories understood by the verifier."""

    SLOW_PROCESSING = "slow_processing"
    DATABASE_ERRORS = "database_errors"
    KAFKA_ERRORS = "kafka_errors"


class NegativeEvidenceType(StrEnum):
    """Explicitly checked absence of expected error signals."""

    NO_DATABASE_ERRORS = "no_database_errors"
    NO_KAFKA_BROKER_ERRORS = "no_kafka_broker_errors"


class VerificationDecision(StrEnum):
    """Deterministic verifier routing outcome."""

    ACCEPTED = "accepted"
    NEEDS_MORE_EVIDENCE = "needs_more_evidence"
    REJECTED = "rejected"


class InvestigationTraceEventType(StrEnum):
    """Safe local investigation lifecycle events."""

    INVESTIGATION_STARTED = "investigation_started"
    NODE_STARTED = "node_started"
    NODE_COMPLETED = "node_completed"
    TOOL_CALLED = "tool_called"
    TOOL_COMPLETED = "tool_completed"
    HYPOTHESES_GENERATED = "hypotheses_generated"
    VERIFICATION_COMPLETED = "verification_completed"
    INVESTIGATION_COMPLETED = "investigation_completed"
    INVESTIGATION_FAILED = "investigation_failed"


class TraceStatus(StrEnum):
    """Low-cardinality trace event outcome."""

    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


class InvestigationWindow(StrictModel):
    """Mandatory UTC interval capped at six hours."""

    start_time: AwareDatetime
    end_time: AwareDatetime

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        """Normalize every accepted aware timestamp to UTC."""

        return _as_utc(value)

    @model_validator(mode="after")
    def validate_bounds(self) -> InvestigationWindow:
        """Reject empty, reversed, and excessive intervals."""

        if self.start_time >= self.end_time:
            raise ValueError("start_time must be earlier than end_time")
        if self.end_time - self.start_time > MAX_TIME_RANGE:
            raise ValueError("investigation window must not exceed six hours")
        return self


class IncidentRequest(StrictModel):
    """Untrusted user request accepted at the workflow boundary."""

    description: Description
    start_time: AwareDatetime | None = None
    end_time: AwareDatetime | None = None
    affected_services: list[ServiceName] = Field(
        default_factory=lambda: [ServiceName.ORDER_CONSUMER],
        min_length=1,
        max_length=2,
    )
    run_id: Identifier | None = None

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_optional_timestamp(cls, value: datetime | None) -> datetime | None:
        """Normalize supplied timestamps while allowing later bounded derivation."""

        return None if value is None else _as_utc(value)

    @model_validator(mode="after")
    def validate_optional_window_and_services(self) -> IncidentRequest:
        """Require complete explicit windows and unique supported services."""

        if (self.start_time is None) != (self.end_time is None):
            raise ValueError("start_time and end_time must be supplied together")
        if self.start_time is not None and self.end_time is not None:
            InvestigationWindow(start_time=self.start_time, end_time=self.end_time)
        if len(set(self.affected_services)) != len(self.affected_services):
            raise ValueError("affected_services must not contain duplicates")
        return self


class InvestigationTask(StrictModel):
    """One allow-listed read-only task and its short planning rationale."""

    task_type: InvestigationTaskType
    reason: ShortText


class InvestigationPlan(StrictModel):
    """A small read-only plan produced through structured model output."""

    tasks: list[InvestigationTask] = Field(min_length=2, max_length=6)

    @model_validator(mode="after")
    def reject_duplicate_tasks(self) -> InvestigationPlan:
        """Prevent repeated tool calls disguised as separate tasks."""

        task_types = [task.task_type for task in self.tasks]
        if len(set(task_types)) != len(task_types):
            raise ValueError("investigation plan must not contain duplicate tasks")
        return self


class EvidenceBase(StrictModel):
    """Fields shared by every traceable evidence item."""

    evidence_id: EvidenceIdentifier
    source: EvidenceSource
    observation: ShortText
    start_time: AwareDatetime
    end_time: AwareDatetime
    raw_value_summary: dict[str, RawValue]
    availability: EvidenceAvailability
    collection_attempt: int = Field(default=1, ge=1, le=MAX_INVESTIGATION_ATTEMPTS)

    @field_validator("start_time", "end_time")
    @classmethod
    def normalize_evidence_timestamp(cls, value: datetime) -> datetime:
        """Persist evidence timestamps in UTC."""

        return _as_utc(value)

    @model_validator(mode="after")
    def validate_evidence_window(self) -> EvidenceBase:
        """Require an ordered evidence interval within the hard safety cap."""

        InvestigationWindow(start_time=self.start_time, end_time=self.end_time)
        return self


class MetricEvidence(EvidenceBase):
    """One deterministic numerical Prometheus summary."""

    evidence_kind: Literal["metric"] = "metric"
    source: EvidenceSource = EvidenceSource.PROMETHEUS
    metric_type: MetricEvidenceType

    @field_validator("source")
    @classmethod
    def require_prometheus(cls, value: EvidenceSource) -> EvidenceSource:
        """Reject metric evidence attributed to any other source."""

        if value != EvidenceSource.PROMETHEUS:
            raise ValueError("metric evidence source must be prometheus")
        return value


class LogEvidence(EvidenceBase):
    """One structured Elasticsearch count and bounded timeline."""

    evidence_kind: Literal["log"] = "log"
    source: EvidenceSource = EvidenceSource.ELASTICSEARCH
    log_type: LogEvidenceType
    matching_log_count: int = Field(ge=0)
    timeline: list[AwareDatetime] = Field(default_factory=list, max_length=100)

    @field_validator("timeline")
    @classmethod
    def normalize_timeline(cls, value: list[datetime]) -> list[datetime]:
        """Normalize and sort the bounded event timeline."""

        return sorted(_as_utc(item) for item in value)

    @field_validator("source")
    @classmethod
    def require_elasticsearch(cls, value: EvidenceSource) -> EvidenceSource:
        """Reject log evidence attributed to any other source."""

        if value != EvidenceSource.ELASTICSEARCH:
            raise ValueError("log evidence source must be elasticsearch")
        return value

    @model_validator(mode="after")
    def validate_timeline_bounds(self) -> LogEvidence:
        """Keep every returned log timestamp inside the evidence interval."""

        if any(not self.start_time <= item <= self.end_time for item in self.timeline):
            raise ValueError("log timeline contains an item outside the evidence window")
        return self


class NegativeEvidence(EvidenceBase):
    """An explicit zero-result check, never an omitted signal."""

    evidence_kind: Literal["negative"] = "negative"
    source: EvidenceSource = EvidenceSource.ELASTICSEARCH
    negative_type: NegativeEvidenceType
    matching_log_count: Literal[0] = 0

    @field_validator("source")
    @classmethod
    def require_elasticsearch(cls, value: EvidenceSource) -> EvidenceSource:
        """Reject negative log evidence attributed to any other source."""

        if value != EvidenceSource.ELASTICSEARCH:
            raise ValueError("negative evidence source must be elasticsearch")
        return value


type Evidence = Annotated[
    MetricEvidence | LogEvidence | NegativeEvidence,
    Field(discriminator="evidence_kind"),
]
type PositiveEvidence = Annotated[
    MetricEvidence | LogEvidence,
    Field(discriminator="evidence_kind"),
]


class RootCauseHypothesis(StrictModel):
    """A model-ranked cause that cites only state evidence identifiers."""

    cause_code: RootCauseCode
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence_ids: list[EvidenceIdentifier] = Field(default_factory=list)
    contradicting_evidence_ids: list[EvidenceIdentifier] = Field(default_factory=list)
    reasoning_summary: ShortText

    @field_validator("reasoning_summary")
    @classmethod
    def reject_unverifiable_numeric_claims(cls, value: str) -> str:
        """Keep numerical facts in deterministic evidence fields, not model prose."""

        if re.search(r"\d", value):
            raise ValueError("reasoning_summary must not contain numerical claims")
        return value

    @model_validator(mode="after")
    def validate_distinct_references(self) -> RootCauseHypothesis:
        """Reject duplicate or simultaneously supporting and contradicting references."""

        supporting = set(self.supporting_evidence_ids)
        contradicting = set(self.contradicting_evidence_ids)
        if self.cause_code != RootCauseCode.INSUFFICIENT_EVIDENCE and not supporting:
            raise ValueError("a positive diagnosis must cite supporting evidence")
        if len(supporting) != len(self.supporting_evidence_ids):
            raise ValueError("supporting evidence identifiers must be unique")
        if len(contradicting) != len(self.contradicting_evidence_ids):
            raise ValueError("contradicting evidence identifiers must be unique")
        if supporting & contradicting:
            raise ValueError("evidence cannot both support and contradict one hypothesis")
        return self


class HypothesisSet(StrictModel):
    """Bounded structured output returned by the hypothesis model call."""

    hypotheses: list[RootCauseHypothesis] = Field(min_length=1, max_length=MAX_HYPOTHESES)

    @model_validator(mode="after")
    def reject_duplicate_causes(self) -> HypothesisSet:
        """Represent each closed cause at most once."""

        causes = [item.cause_code for item in self.hypotheses]
        if len(set(causes)) != len(causes):
            raise ValueError("hypothesis causes must be unique")
        return self


class RecommendedAction(StrictModel):
    """One allow-listed recommendation tied to verified evidence."""

    action_code: RecommendedActionCode
    reason: ShortText
    supporting_evidence_ids: list[EvidenceIdentifier] = Field(default_factory=list)


class VerificationResult(StrictModel):
    """Deterministic validation result used for conditional routing."""

    decision: VerificationDecision
    selected_cause: RootCauseCode | None = None
    verified_evidence_ids: list[EvidenceIdentifier] = Field(default_factory=list)
    missing_tasks: list[InvestigationTaskType] = Field(default_factory=list, max_length=6)
    issues: list[ShortText] = Field(default_factory=list, max_length=20)


class IncidentReport(StrictModel):
    """Persistable final report assembled from verified structured state."""

    investigation_id: Identifier
    status: IncidentStatus
    incident_summary: Description
    primary_root_cause: RootCauseHypothesis | None = None
    alternative_hypotheses: list[RootCauseHypothesis] = Field(
        default_factory=list,
        max_length=MAX_HYPOTHESES - 1,
    )
    supporting_evidence: list[PositiveEvidence] = Field(default_factory=list)
    negative_evidence: list[NegativeEvidence] = Field(default_factory=list)
    recommended_actions: list[RecommendedAction] = Field(default_factory=list, max_length=6)
    limitations: list[ShortText] = Field(default_factory=list, max_length=20)
    tool_call_count: int = Field(ge=0, le=10)
    model_call_count: int = Field(default=0, ge=0, le=MAX_MODEL_CALLS)
    investigation_attempts: int = Field(ge=1, le=2)
    started_at: AwareDatetime
    completed_at: AwareDatetime

    @field_validator("started_at", "completed_at")
    @classmethod
    def normalize_report_timestamp(cls, value: datetime) -> datetime:
        """Normalize report lifecycle timestamps to UTC."""

        return _as_utc(value)

    @model_validator(mode="after")
    def validate_report_times(self) -> IncidentReport:
        """Reject reports that complete before they start."""

        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not be earlier than started_at")
        return self


class InvestigationTraceEvent(StrictModel):
    """One locally persisted, safe, low-cardinality workflow event."""

    event_id: Identifier
    timestamp: AwareDatetime
    event_type: InvestigationTraceEventType
    investigation_id: Identifier
    status: TraceStatus
    node: str | None = Field(default=None, max_length=64, pattern=r"^[a-z_]+$")
    tool_name: InvestigationTaskType | None = None
    duration_ms: float | None = Field(default=None, ge=0)
    tool_call_count: int = Field(default=0, ge=0, le=10)
    investigation_attempt: int = Field(default=1, ge=1, le=2)

    @field_validator("timestamp")
    @classmethod
    def normalize_trace_timestamp(cls, value: datetime) -> datetime:
        """Normalize trace timestamps to UTC."""

        return _as_utc(value)


class EvaluationResult(StrictModel):
    """Structured comparison between a final report and scenario ground truth."""

    root_cause_exact_match: bool
    root_cause_rank: int | None = Field(default=None, ge=1, le=MAX_HYPOTHESES)
    expected_metric_evidence_recall: float = Field(ge=0.0, le=1.0)
    expected_log_evidence_recall: float = Field(ge=0.0, le=1.0)
    negative_evidence_recall: float = Field(ge=0.0, le=1.0)
    unsupported_evidence_reference_count: int = Field(ge=0)
    forbidden_action_count: int = Field(ge=0)
    tool_call_count: int = Field(ge=0, le=10)
    investigation_attempt_count: int = Field(ge=1, le=2)
    workflow_duration_seconds: float = Field(ge=0)


class InvestigationArtifactPaths(StrictModel):
    """Paths written by explicit local investigation persistence."""

    report_path: Path
    trace_path: Path


_EVIDENCE_IDS: dict[tuple[InvestigationTaskType, bool], str] = {
    (InvestigationTaskType.CHECK_CONSUMER_LAG, False): "metric-consumer-lag-summary",
    (InvestigationTaskType.CHECK_PROCESSING_LATENCY, False): ("metric-processing-latency-p95"),
    (InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES, False): (
        "metric-producer-consumer-rate-comparison"
    ),
    (InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS, False): ("log-slow-processing-summary"),
    (InvestigationTaskType.FIND_DATABASE_ERRORS, False): "log-database-errors-summary",
    (InvestigationTaskType.FIND_DATABASE_ERRORS, True): "negative-no-database-errors",
    (InvestigationTaskType.FIND_KAFKA_ERRORS, False): "log-kafka-errors-summary",
    (InvestigationTaskType.FIND_KAFKA_ERRORS, True): "negative-no-kafka-errors",
}


def evidence_id_for_task(task_type: InvestigationTaskType, *, negative: bool = False) -> str:
    """Return the stable evidence identifier for one allow-listed task outcome."""

    try:
        return _EVIDENCE_IDS[(task_type, negative)]
    except KeyError as error:
        raise ValueError(f"task {task_type.value} does not support negative evidence") from error
