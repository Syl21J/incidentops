"""Strict data models exchanged between repository validation scripts."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from incidentops.investigation.models import Identifier, StrictModel

ResourceName = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=256),
]


class SlowConsumerMetrics(StrictModel):
    """Metric observations captured during the slow-consumer scenario."""

    maximum_lag: float = Field(ge=0)
    lag_start: float = Field(ge=0)
    lag_end: float = Field(ge=0)
    lag_trend: Literal["increasing", "stable", "decreasing"]
    lag_samples: int = Field(gt=0)
    p95_seconds: float = Field(ge=0)
    latency_samples: int = Field(gt=0)
    producer_rate: float = Field(ge=0)
    consumer_rate: float = Field(ge=0)
    consumer_is_slower: bool


class SlowConsumerObservations(SlowConsumerMetrics):
    """Complete metric and log observations persisted for follow-up checks."""

    slow_processing_log_count: int = Field(ge=0)
    database_error_count: int = Field(ge=0)
    kafka_error_count: int = Field(ge=0)


class ScenarioMetadata(StrictModel):
    """Exact identifiers, window, and observations for one retained scenario run."""

    schema_version: Literal[1] = 1
    scenario_id: Literal["slow_consumer_v1"] = "slow_consumer_v1"
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
