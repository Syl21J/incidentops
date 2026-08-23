"""Strict result models for the multi-incident benchmark."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import AwareDatetime, Field

from incidentops.investigation.models import Identifier, RootCauseCode, StrictModel

KnowledgeMode = Literal["disabled", "required"]


class BenchmarkCaseResult(StrictModel):
    """One scenario diagnosis and its ground-truth comparison."""

    scenario_id: Identifier
    knowledge_mode: KnowledgeMode
    expected_root_cause: RootCauseCode
    diagnosed_root_cause: RootCauseCode
    root_cause_exact_match: bool
    root_cause_rank: int | None = Field(default=None, ge=1, le=3)
    evidence_recall: float = Field(ge=0, le=1)
    negative_evidence_recall: float = Field(ge=0, le=1)
    knowledge_recall_at_k: float = Field(ge=0, le=1)
    unsupported_evidence_reference_count: int = Field(ge=0)
    unknown_knowledge_reference_count: int = Field(ge=0)
    forbidden_action_count: int = Field(ge=0)
    insufficient_evidence: bool
    tool_calls: int = Field(ge=0, le=10)
    model_calls: int = Field(ge=0, le=4)
    workflow_duration_seconds: float = Field(ge=0)
    retrieved_document_ids: list[Identifier] = Field(default_factory=list, max_length=10)


class BenchmarkAggregate(StrictModel):
    """Macro benchmark metrics and a true-by-predicted confusion matrix."""

    knowledge_mode: KnowledgeMode
    scenario_count: int = Field(ge=1, le=20)
    root_cause_accuracy: float = Field(ge=0, le=1)
    mean_root_cause_rank: float = Field(ge=1, le=4)
    macro_evidence_recall: float = Field(ge=0, le=1)
    macro_negative_evidence_recall: float = Field(ge=0, le=1)
    macro_knowledge_recall_at_k: float = Field(ge=0, le=1)
    unsupported_evidence_reference_count: int = Field(ge=0)
    unknown_knowledge_reference_count: int = Field(ge=0)
    forbidden_action_count: int = Field(ge=0)
    insufficient_evidence_rate: float = Field(ge=0, le=1)
    average_tool_calls: float = Field(ge=0, le=10)
    average_model_calls: float = Field(ge=0, le=4)
    average_workflow_duration_seconds: float = Field(ge=0)
    confusion_matrix: dict[str, dict[str, int]]
    cases: list[BenchmarkCaseResult] = Field(min_length=1, max_length=20)


class RagComparison(StrictModel):
    """Transparent required-RAG minus no-RAG metric deltas."""

    root_cause_accuracy_delta: float = Field(ge=-1, le=1)
    macro_evidence_recall_delta: float = Field(ge=-1, le=1)
    macro_knowledge_recall_at_k_delta: float = Field(ge=-1, le=1)
    unsupported_evidence_reference_delta: int
    forbidden_action_delta: int
    average_tool_calls_delta: float = Field(ge=-10, le=10)
    average_model_calls_delta: float = Field(ge=-4, le=4)
    average_duration_seconds_delta: float


class MultiIncidentBenchmarkResult(StrictModel):
    """Complete deterministic or optional live benchmark output."""

    schema_version: Literal[1] = 1
    model_provider: Literal["deterministic-test", "openai-compatible"]
    created_at: AwareDatetime
    aggregates: list[BenchmarkAggregate] = Field(min_length=1, max_length=2)
    rag_comparison: RagComparison | None = None

    @classmethod
    def now(
        cls,
        *,
        model_provider: Literal["deterministic-test", "openai-compatible"],
        aggregates: list[BenchmarkAggregate],
        rag_comparison: RagComparison | None,
    ) -> MultiIncidentBenchmarkResult:
        """Build a timestamped result while keeping time construction in one place."""

        from datetime import UTC

        return cls(
            model_provider=model_provider,
            created_at=datetime.now(UTC),
            aggregates=aggregates,
            rag_comparison=rag_comparison,
        )
