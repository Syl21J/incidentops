"""Versioned and strictly validated incident scenario manifests."""

from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{2,63}$")]
Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1_000)]


class RootCause(BaseModel):
    """Expected service and root-cause classification."""

    model_config = ConfigDict(extra="forbid")
    code: Identifier
    service: Literal["order-producer", "order-consumer"]


class ExpectedMetric(BaseModel):
    """Expected behavior of one allow-listed scenario metric."""

    model_config = ConfigDict(extra="forbid")
    metric: Literal[
        "incidentops_kafka_consumer_lag",
        "incidentops_order_processing_duration_seconds",
    ]
    behavior: Literal["increasing", "elevated"]


class ExpectedComparison(BaseModel):
    """Expected relationship between fixed metric signals."""

    model_config = ConfigDict(extra="forbid")
    comparison: Literal["producer_rate_greater_than_consumer_rate"]


class ExpectedLog(BaseModel):
    """Expected structured log evidence."""

    model_config = ConfigDict(extra="forbid")
    service: Literal["order-producer", "order-consumer"]
    event_type: Identifier


class ScenarioManifest(BaseModel):
    """Explicit ground truth consumed by validation and future benchmarks."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    id: Identifier
    title: Text
    description: Text
    root_cause: RootCause
    expected_metrics: list[ExpectedMetric | ExpectedComparison] = Field(min_length=1)
    expected_logs: list[ExpectedLog] = Field(min_length=1)
    negative_evidence: list[Identifier] = Field(min_length=1)
    acceptable_actions: list[Identifier] = Field(min_length=1)
    forbidden_actions: list[Identifier] = Field(min_length=1)


def load_scenario_manifest(path: Path) -> ScenarioManifest:
    """Load one UTF-8 YAML manifest and validate its complete structure."""

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return ScenarioManifest.model_validate(payload)
