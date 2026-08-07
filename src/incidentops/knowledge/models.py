"""Strict models for the controlled operational knowledge corpus."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

DocumentIdentifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, pattern=r"^[a-z][a-z0-9_]{2,79}$"),
]
Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Content = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=64_000)]
Hash = Annotated[str, StringConstraints(pattern=r"^[a-f0-9]{64}$")]


class StrictModel(BaseModel):
    """Reject unknown fields throughout the knowledge ingestion boundary."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class DocumentType(StrEnum):
    """Supported operational knowledge document categories."""

    RUNBOOK = "runbook"
    ARCHITECTURE = "architecture"
    METRIC = "metric"
    LOG_EVENT = "log_event"
    INCIDENT = "incident"


class KnowledgeService(StrEnum):
    """Closed service and infrastructure component allowlist."""

    ORDER_PRODUCER = "order-producer"
    ORDER_CONSUMER = "order-consumer"
    KAFKA = "kafka"
    POSTGRES = "postgres"
    ELASTICSEARCH = "elasticsearch"
    FILEBEAT = "filebeat"
    PROMETHEUS = "prometheus"


class Technology(StrEnum):
    """Closed technology allowlist used by retrieval filters in a later stage."""

    PYTHON = "python"
    KAFKA = "kafka"
    POSTGRES = "postgres"
    ELASTICSEARCH = "elasticsearch"
    FILEBEAT = "filebeat"
    PROMETHEUS = "prometheus"
    DOCKER = "docker"
    LANGGRAPH = "langgraph"


class IncidentType(StrEnum):
    """Closed incident classification allowlist for the initial corpus."""

    CONSUMER_LAG = "consumer_lag"
    SLOW_PROCESSING = "slow_processing"
    DATABASE_LATENCY = "database_latency"
    DATABASE_UNAVAILABLE = "database_unavailable"
    KAFKA_BROKER_FAILURE = "kafka_broker_failure"
    TRAFFIC_SPIKE = "traffic_spike"
    DUPLICATE_PROCESSING = "duplicate_processing"
    LOG_PIPELINE_FAILURE = "log_pipeline_failure"
    METRICS_UNAVAILABLE = "metrics_unavailable"
    MALFORMED_EVENT = "malformed_event"
    CONSUMER_REBALANCE = "consumer_rebalance"


def _require_unique(values: Sequence[StrEnum], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} must not contain duplicate values")


def require_ascii_english(value: str, field_name: str) -> str:
    """Keep the controlled corpus in portable, reviewable English source text."""

    if not value.isascii():
        raise ValueError(f"{field_name} must contain ASCII English text only")
    return value


class KnowledgeMetadata(StrictModel):
    """Versioned front matter shared by every knowledge document."""

    schema_version: Literal[1]
    document_id: DocumentIdentifier
    document_type: DocumentType
    title: Title
    services: list[KnowledgeService] = Field(min_length=1, max_length=len(KnowledgeService))
    technologies: list[Technology] = Field(min_length=1, max_length=len(Technology))
    incident_types: list[IncidentType] = Field(min_length=1, max_length=len(IncidentType))
    status: Literal["active"]
    updated_at: date

    @field_validator("title")
    @classmethod
    def validate_title_language(cls, value: str) -> str:
        """Reject non-portable titles at the schema boundary."""

        return require_ascii_english(value, "title")

    @model_validator(mode="after")
    def validate_unique_lists(self) -> KnowledgeMetadata:
        """Reject ambiguous repeated filter values."""

        _require_unique(self.services, "services")
        _require_unique(self.technologies, "technologies")
        _require_unique(self.incident_types, "incident_types")
        return self


class KnowledgeDocument(StrictModel):
    """One validated Markdown source document."""

    metadata: KnowledgeMetadata
    content: Content
    source_path: Path

    @field_validator("content")
    @classmethod
    def validate_content_language(cls, value: str) -> str:
        """Reject non-portable corpus content before chunking."""

        return require_ascii_english(value, "content")


class KnowledgeChunk(StrictModel):
    """One deterministic, index-ready Markdown chunk before embedding."""

    chunk_id: str = Field(min_length=1, max_length=512)
    content: Content
    metadata: KnowledgeMetadata
    source_path: Path
    headings: list[str] = Field(min_length=1, max_length=6)
    heading_path: str = Field(min_length=1, max_length=512)
    chunk_index: int = Field(ge=0)
    content_hash: Hash


class IndexedChunk(StrictModel):
    """Minimal stored state required for deterministic ingestion planning."""

    chunk_id: str
    document_id: DocumentIdentifier
    indexing_hash: Hash


class IngestionReport(StrictModel):
    """Stable chunk and document counts emitted by ingestion."""

    dry_run: bool
    documents: int = Field(ge=0)
    chunks: int = Field(ge=0)
    created: int = Field(ge=0)
    updated: int = Field(ge=0)
    unchanged: int = Field(ge=0)
    stale: int = Field(ge=0)
    deleted: int = Field(ge=0)


class IndexStatus(StrictModel):
    """Bounded status information for the fixed knowledge index."""

    index_name: str
    exists: bool
    mapping_compatible: bool | None
    documents: int = Field(ge=0)
    chunks: int = Field(ge=0)


class RetrievalMode(StrEnum):
    """Closed retrieval mode allowlist."""

    LEXICAL = "lexical"
    VECTOR = "vector"
    HYBRID = "hybrid"


class KnowledgeFilters(StrictModel):
    """Only metadata filters exposed by the retrieval boundary."""

    services: list[KnowledgeService] = Field(default_factory=list, max_length=4)
    incident_types: list[IncidentType] = Field(default_factory=list, max_length=4)
    document_types: list[DocumentType] = Field(default_factory=list, max_length=5)
    statuses: list[Literal["active"]] = Field(default_factory=list, max_length=1)

    @model_validator(mode="after")
    def validate_unique_filters(self) -> KnowledgeFilters:
        """Reject duplicate filters rather than silently normalizing input."""

        _require_unique(self.services, "services")
        _require_unique(self.incident_types, "incident_types")
        _require_unique(self.document_types, "document_types")
        if len(self.statuses) != len(set(self.statuses)):
            raise ValueError("statuses must not contain duplicate values")
        return self


class KnowledgeSearchRequest(StrictModel):
    """Bounded query accepted by the public retrieval service."""

    query: str = Field(min_length=1, max_length=512)
    mode: RetrievalMode = RetrievalMode.HYBRID
    filters: KnowledgeFilters = Field(default_factory=KnowledgeFilters)
    top_k: int = Field(default=5, ge=1, le=10)
    candidate_k: int = Field(default=40, ge=1, le=100)

    @model_validator(mode="after")
    def validate_candidate_count(self) -> KnowledgeSearchRequest:
        """Candidate collection must cover every requested final result."""

        if self.candidate_k < self.top_k:
            raise ValueError("candidate_k must be greater than or equal to top_k")
        return self


class KnowledgeScores(StrictModel):
    """Raw retrieval scores and deterministic Python RRF score."""

    lexical: float | None = None
    vector: float | None = None
    fused: float | None = None


class KnowledgeReference(StrictModel):
    """One validated bounded retrieval result safe for model context."""

    knowledge_reference_id: str = Field(pattern=r"^knowledge-[a-f0-9]{24}$")
    document_id: DocumentIdentifier
    chunk_id: str = Field(min_length=1, max_length=512)
    title: Title
    snippet: str = Field(min_length=1, max_length=400)
    scores: KnowledgeScores


class KnowledgeSearchResult(StrictModel):
    """Stable search response containing no Elasticsearch query representation."""

    query: str = Field(min_length=1, max_length=512)
    mode: RetrievalMode
    references: list[KnowledgeReference] = Field(default_factory=list, max_length=10)
