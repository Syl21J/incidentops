"""Strict data models exchanged between repository validation scripts."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from incidentops.investigation.models import Identifier, StrictModel

ResourceName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=256),
]


class SlowConsumerMetrics(StrictModel):
    """Backward-compatible complete metric profile captured by scenario runs."""

    maximum_lag: float = Field(ge=0)
    lag_start: float = Field(ge=0)
    lag_end: float = Field(ge=0)
    lag_trend: Literal["increasing", "stable", "decreasing"]
    lag_samples: int = Field(gt=0)
    p95_seconds: float = Field(ge=0)
    latency_samples: int = Field(gt=0)
    database_p95_seconds: float = Field(default=0.0, ge=0)
    database_latency_samples: int = Field(default=1, gt=0)
    processing_state: Literal["normal", "elevated"] = "normal"
    database_state: Literal["normal", "elevated"] = "normal"
    producer_rate: float = Field(ge=0)
    consumer_rate: float = Field(ge=0)
    consumer_is_slower: bool
    producer_baseline_rate: float = Field(default=0.0, ge=0)
    producer_recent_rate: float = Field(default=0.0, ge=0)
    producer_rate_change_ratio: float = Field(default=1.0, ge=0)
    producer_surge: bool = False
    processing_error_rate: float = Field(default=0.0, ge=0)
    processing_errors_present: bool = False
    valid_processing_present: bool = True


class SlowConsumerObservations(SlowConsumerMetrics):
    """Complete metric and log observations persisted for follow-up checks."""

    slow_processing_log_count: int = Field(ge=0)
    database_operation_slow_log_count: int = Field(default=0, ge=0)
    invalid_event_log_count: int = Field(default=0, ge=0)
    database_error_count: int = Field(ge=0)
    kafka_error_count: int = Field(ge=0)


class ScenarioMetadata(StrictModel):
    """Exact identifiers, window, and observations for one retained scenario run."""

    schema_version: Literal[2] = 2
    scenario_id: Identifier = "slow_consumer_v1"
    affected_service: Literal["order-producer", "order-consumer"] = "order-consumer"
    run_id: Identifier
    topic: ResourceName
    consumer_group: ResourceName
    start_time: AwareDatetime
    end_time: AwareDatetime
    observations: SlowConsumerObservations

    @model_validator(mode="after")
    def validate_window(self) -> "ScenarioMetadata":
        """Require a non-empty ordered scenario window."""

        if self.start_time >= self.end_time:
            raise ValueError("scenario start_time must be earlier than end_time")
        return self
