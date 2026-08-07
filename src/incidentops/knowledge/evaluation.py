"""Deterministic document-level evaluation for bounded knowledge retrieval."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol

import yaml
from pydantic import Field, model_validator

from incidentops.knowledge.corpus import UniqueKeyLoader
from incidentops.knowledge.models import (
    DocumentIdentifier,
    KnowledgeFilters,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
    RetrievalMode,
    StrictModel,
)

EVALUATION_CHUNK_LIMIT = 10


class RetrievalEvaluationError(RuntimeError):
    """Raised when retrieval ground truth or evaluation execution is invalid."""


class RetrievalEvaluator(Protocol):
    """Narrow search surface required by deterministic evaluation."""

    def search(self, request: KnowledgeSearchRequest) -> KnowledgeSearchResult: ...


class RetrievalCase(StrictModel):
    """One query with explicit relevant and forbidden document ground truth."""

    case_id: DocumentIdentifier
    query: str = Field(min_length=1, max_length=512)
    filters: KnowledgeFilters = Field(default_factory=KnowledgeFilters)
    relevant_document_ids: list[DocumentIdentifier] = Field(min_length=1, max_length=10)
    excluded_document_ids: list[DocumentIdentifier] = Field(default_factory=list, max_length=10)
    top_k: int = Field(default=5, ge=1, le=10)

    @model_validator(mode="after")
    def validate_ground_truth(self) -> RetrievalCase:
        """Require unique and disjoint expected document sets."""

        relevant = set(self.relevant_document_ids)
        excluded = set(self.excluded_document_ids)
        if len(relevant) != len(self.relevant_document_ids):
            raise ValueError("relevant_document_ids must be unique")
        if len(excluded) != len(self.excluded_document_ids):
            raise ValueError("excluded_document_ids must be unique")
        if relevant & excluded:
            raise ValueError("relevant and excluded document identifiers must be disjoint")
        return self


class RetrievalDataset(StrictModel):
    """Versioned retrieval ground truth that is never passed to the investigation graph."""

    schema_version: Literal[1]
    cases: list[RetrievalCase] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def validate_case_ids(self) -> RetrievalDataset:
        """Reject duplicate benchmark case identifiers."""

        identifiers = [case.case_id for case in self.cases]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("retrieval case identifiers must be unique")
        return self


class RetrievalCaseMetrics(StrictModel):
    """Document-level ranked metrics for one retrieval case."""

    case_id: DocumentIdentifier
    recall_at_k: float = Field(ge=0.0, le=1.0)
    precision_at_k: float = Field(ge=0.0, le=1.0)
    reciprocal_rank: float = Field(ge=0.0, le=1.0)
    excluded_document_count: int = Field(ge=0)
    retrieved_document_ids: list[DocumentIdentifier] = Field(default_factory=list, max_length=10)


class RetrievalEvaluationResult(StrictModel):
    """Macro averages and per-case details for one retrieval mode."""

    mode: RetrievalMode
    case_count: int = Field(ge=1, le=50)
    recall_at_k: float = Field(ge=0.0, le=1.0)
    precision_at_k: float = Field(ge=0.0, le=1.0)
    mrr: float = Field(ge=0.0, le=1.0)
    excluded_document_count: int = Field(ge=0)
    cases: list[RetrievalCaseMetrics] = Field(min_length=1, max_length=50)


def load_retrieval_dataset(path: Path) -> RetrievalDataset:
    """Load one UTF-8 retrieval dataset with strict duplicate-key detection."""

    try:
        payload = yaml.load(path.read_text(encoding="utf-8"), Loader=UniqueKeyLoader)
        return RetrievalDataset.model_validate(payload)
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise RetrievalEvaluationError(f"invalid retrieval dataset: {path}") from error


def _case_metrics(case: RetrievalCase, result: KnowledgeSearchResult) -> RetrievalCaseMetrics:
    ranked_documents: list[str] = []
    for reference in result.references:
        if reference.document_id not in ranked_documents:
            ranked_documents.append(reference.document_id)
    ranked_documents = ranked_documents[: case.top_k]
    relevant = set(case.relevant_document_ids)
    found = sum(document_id in relevant for document_id in ranked_documents)
    first_relevant_rank = next(
        (
            rank
            for rank, document_id in enumerate(ranked_documents, start=1)
            if document_id in relevant
        ),
        None,
    )
    return RetrievalCaseMetrics(
        case_id=case.case_id,
        recall_at_k=found / len(relevant),
        precision_at_k=found / case.top_k,
        reciprocal_rank=0.0 if first_relevant_rank is None else 1.0 / first_relevant_rank,
        excluded_document_count=sum(
            document_id in set(case.excluded_document_ids) for document_id in ranked_documents
        ),
        retrieved_document_ids=ranked_documents,
    )


def evaluate_retrieval(
    dataset: RetrievalDataset,
    evaluator: RetrievalEvaluator,
    *,
    mode: RetrievalMode,
    candidate_k: int = 40,
) -> RetrievalEvaluationResult:
    """Execute bounded cases and calculate deterministic document-level macro metrics."""

    metrics: list[RetrievalCaseMetrics] = []
    for case in dataset.cases:
        # Retrieval ranks chunks, while the benchmark ground truth ranks documents.
        # Fetch the bounded maximum so repeated chunks from one document do not
        # consume the complete document-level evaluation window.
        result = evaluator.search(
            KnowledgeSearchRequest(
                query=case.query,
                mode=mode,
                filters=case.filters,
                top_k=EVALUATION_CHUNK_LIMIT,
                candidate_k=max(EVALUATION_CHUNK_LIMIT, candidate_k),
            )
        )
        metrics.append(_case_metrics(case, result))
    count = len(metrics)
    return RetrievalEvaluationResult(
        mode=mode,
        case_count=count,
        recall_at_k=sum(item.recall_at_k for item in metrics) / count,
        precision_at_k=sum(item.precision_at_k for item in metrics) / count,
        mrr=sum(item.reciprocal_rank for item in metrics) / count,
        excluded_document_count=sum(item.excluded_document_count for item in metrics),
        cases=metrics,
    )
