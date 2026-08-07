"""Bounded lexical, vector, and hybrid retrieval with deterministic RRF fusion."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from elasticsearch import Elasticsearch
from incidentops.knowledge.embeddings import EmbeddingProvider
from incidentops.knowledge.index import ElasticsearchKnowledgeIndex
from incidentops.knowledge.models import (
    DocumentIdentifier,
    KnowledgeFilters,
    KnowledgeReference,
    KnowledgeScores,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
    RetrievalMode,
    Title,
)

RRF_K = 60
MAX_SNIPPET_CHARACTERS = 400
MAX_QUERY_TOKENS = 64
SOURCE_FIELDS = ["document_id", "chunk_id", "title", "content"]
QUERY_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{1,39}")
WHITESPACE = re.compile(r"\s+")
INSTRUCTION_PATTERNS = (
    re.compile(r"ignore\s+(?:all\s+)?previous\s+instructions?", re.IGNORECASE),
    re.compile(r"(?:system|developer|assistant)\s*:", re.IGNORECASE),
    re.compile(r"reveal\s+(?:the\s+)?system\s+prompt", re.IGNORECASE),
    re.compile(r"execute\s+(?:this\s+)?command", re.IGNORECASE),
)


class KnowledgeRetrievalError(RuntimeError):
    """Raised when bounded retrieval cannot return validated references."""


class RetrievalSource(BaseModel):
    """Exact stored fields accepted from an Elasticsearch hit."""

    model_config = ConfigDict(extra="forbid")

    document_id: DocumentIdentifier
    chunk_id: str = Field(min_length=1, max_length=512)
    title: Title
    content: str = Field(min_length=1, max_length=1_000)

    @field_validator("content")
    @classmethod
    def validate_controlled_content(cls, value: str) -> str:
        """Reject unbounded or non-portable content injected outside controlled ingestion."""

        if not value.isascii():
            raise ValueError("retrieved knowledge content must be ASCII")
        return value


@dataclass(frozen=True)
class RankedChunk:
    """One validated backend hit with its raw mode-specific score."""

    source: RetrievalSource
    score: float


def clean_query_text(value: str) -> str:
    """Reduce untrusted query text to bounded search terms, never prompt instructions."""

    cleaned = value
    for pattern in INSTRUCTION_PATTERNS:
        cleaned = pattern.sub(" ", cleaned)
    tokens = QUERY_TOKEN.findall(cleaned)[:MAX_QUERY_TOKENS]
    if not tokens:
        raise ValueError("knowledge query contains no searchable terms")
    return " ".join(tokens)[:512].strip()


def _filter_clauses(filters: KnowledgeFilters) -> list[dict[str, object]]:
    clauses: list[dict[str, object]] = []
    values: tuple[tuple[str, list[str]], ...] = (
        ("metadata.services", [item.value for item in filters.services]),
        ("metadata.incident_types", [item.value for item in filters.incident_types]),
        ("metadata.document_type", [item.value for item in filters.document_types]),
        ("metadata.status", [str(item) for item in filters.statuses]),
    )
    for field, selected in values:
        if selected:
            clauses.append({"terms": {field: selected}})
    return clauses


def _response_body(response: Any) -> Any:
    return response.body if hasattr(response, "body") else response


def _parse_hits(response: object, maximum: int) -> list[RankedChunk]:
    body = _response_body(response)
    try:
        hits = body["hits"]["hits"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise KnowledgeRetrievalError("Elasticsearch returned invalid knowledge hits") from error
    results: list[RankedChunk] = []
    if not isinstance(hits, list) or len(hits) > maximum:
        raise KnowledgeRetrievalError("Elasticsearch returned too many knowledge hits")
    try:
        for hit in hits:
            score = float(hit["_score"])
            if not math.isfinite(score) or score < 0:
                raise ValueError("invalid retrieval score")
            results.append(
                RankedChunk(
                    source=RetrievalSource.model_validate(hit["_source"]),
                    score=score,
                )
            )
    except (KeyError, TypeError, ValueError) as error:
        raise KnowledgeRetrievalError("Elasticsearch returned invalid knowledge hits") from error
    identifiers = [item.source.chunk_id for item in results]
    if len(identifiers) != len(set(identifiers)):
        raise KnowledgeRetrievalError("Elasticsearch returned duplicate knowledge chunks")
    return results


def _reference_id(chunk_id: str) -> str:
    digest = hashlib.sha256(chunk_id.encode("utf-8")).hexdigest()[:24]
    return f"knowledge-{digest}"


def _snippet(content: str, query: str) -> str:
    normalized = WHITESPACE.sub(" ", content).strip()
    if len(normalized) <= MAX_SNIPPET_CHARACTERS:
        return normalized
    positions = [
        normalized.casefold().find(token.casefold())
        for token in QUERY_TOKEN.findall(query)
        if len(token) >= 3
    ]
    positions = [position for position in positions if position >= 0]
    center = min(positions) if positions else 0
    start = max(0, center - 100)
    if start:
        next_space = normalized.find(" ", start)
        start = next_space + 1 if next_space >= 0 else start
    end = min(len(normalized), start + MAX_SNIPPET_CHARACTERS - 4)
    snippet = normalized[start:end].strip()
    prefix = "... " if start else ""
    suffix = " ..." if end < len(normalized) else ""
    return f"{prefix}{snippet}{suffix}"[:MAX_SNIPPET_CHARACTERS]


def _as_reference(
    item: RankedChunk,
    query: str,
    *,
    lexical: float | None,
    vector: float | None,
    fused: float | None,
) -> KnowledgeReference:
    return KnowledgeReference(
        knowledge_reference_id=_reference_id(item.source.chunk_id),
        document_id=item.source.document_id,
        chunk_id=item.source.chunk_id,
        title=item.source.title,
        snippet=_snippet(item.source.content, query),
        scores=KnowledgeScores(lexical=lexical, vector=vector, fused=fused),
    )


def rrf_fuse(
    lexical: list[RankedChunk],
    vector: list[RankedChunk],
    query: str,
    top_k: int,
    *,
    rrf_k: int = RRF_K,
) -> list[KnowledgeReference]:
    """Fuse ranks by chunk ID with stable tie-breaking independent of backend timing."""

    if rrf_k != RRF_K:
        raise ValueError(f"rrf_k must equal the fixed value {RRF_K}")
    by_id = {item.source.chunk_id: item for item in [*lexical, *vector]}
    lexical_scores = {item.source.chunk_id: item.score for item in lexical}
    vector_scores = {item.source.chunk_id: item.score for item in vector}
    fused_scores: dict[str, float] = {}
    for ranking in (lexical, vector):
        for rank, item in enumerate(ranking, start=1):
            fused_scores[item.source.chunk_id] = fused_scores.get(item.source.chunk_id, 0.0) + (
                1.0 / (rrf_k + rank)
            )
    ranked_ids = sorted(fused_scores, key=lambda chunk_id: (-fused_scores[chunk_id], chunk_id))
    return [
        _as_reference(
            by_id[chunk_id],
            query,
            lexical=lexical_scores.get(chunk_id),
            vector=vector_scores.get(chunk_id),
            fused=fused_scores[chunk_id],
        )
        for chunk_id in ranked_ids[:top_k]
    ]


class KnowledgeSearchService:
    """Typed search facade that keeps every Elasticsearch query internal and bounded."""

    def __init__(
        self,
        client: Elasticsearch,
        embedding_provider: EmbeddingProvider,
        *,
        owns_client: bool = False,
    ) -> None:
        self._client = client
        self._index = ElasticsearchKnowledgeIndex(client)
        self._embedding_provider = embedding_provider
        self._owns_client = owns_client
        self._embedding_identity_validated = False

    def close(self) -> None:
        """Close only a client explicitly owned by this service."""

        if self._owns_client:
            self._client.close()

    def _lexical(self, request: KnowledgeSearchRequest) -> list[RankedChunk]:
        filters = _filter_clauses(request.filters)
        response = self._client.search(
            index=self._index.index_name,
            query={
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": request.query,
                                "fields": ["title^3", "headings^2", "content"],
                                "type": "best_fields",
                            }
                        }
                    ],
                    "filter": filters,
                }
            },
            size=request.candidate_k,
            source_includes=SOURCE_FIELDS,
            track_total_hits=False,
        )
        return _parse_hits(response, request.candidate_k)

    def _vector(self, request: KnowledgeSearchRequest) -> list[RankedChunk]:
        if not self._embedding_identity_validated:
            indexed_identity = self._index.embedding_identity()
            configured_identity = (
                self._embedding_provider.name,
                self._embedding_provider.model_name,
            )
            if indexed_identity != configured_identity:
                raise KnowledgeRetrievalError(
                    "configured embedding provider and model do not match the knowledge index"
                )
            self._embedding_identity_validated = True
        vector = self._embedding_provider.embed_batch([request.query])[0]
        filters = _filter_clauses(request.filters)
        knn: dict[str, object] = {
            "field": "embedding",
            "query_vector": vector,
            "k": request.candidate_k,
            "num_candidates": min(100, max(request.candidate_k, request.candidate_k * 2)),
        }
        if filters:
            knn["filter"] = {"bool": {"filter": filters}}
        response = self._client.search(
            index=self._index.index_name,
            knn=knn,
            size=request.candidate_k,
            source_includes=SOURCE_FIELDS,
            track_total_hits=False,
        )
        return _parse_hits(response, request.candidate_k)

    def search(self, request: KnowledgeSearchRequest) -> KnowledgeSearchResult:
        """Execute exactly the selected mode and return only validated references."""

        cleaned = clean_query_text(request.query)
        request = request.model_copy(update={"query": cleaned})
        try:
            self._index.validate_index()
            lexical = (
                self._lexical(request)
                if request.mode in {RetrievalMode.LEXICAL, RetrievalMode.HYBRID}
                else []
            )
            vector = (
                self._vector(request)
                if request.mode in {RetrievalMode.VECTOR, RetrievalMode.HYBRID}
                else []
            )
        except KnowledgeRetrievalError:
            raise
        except Exception as error:
            raise KnowledgeRetrievalError("bounded knowledge retrieval failed") from error
        if request.mode == RetrievalMode.HYBRID:
            references = rrf_fuse(lexical, vector, cleaned, request.top_k)
        else:
            selected = lexical if request.mode == RetrievalMode.LEXICAL else vector
            references = [
                _as_reference(
                    item,
                    cleaned,
                    lexical=item.score if request.mode == RetrievalMode.LEXICAL else None,
                    vector=item.score if request.mode == RetrievalMode.VECTOR else None,
                    fused=None,
                )
                for item in selected[: request.top_k]
            ]
        return KnowledgeSearchResult(query=cleaned, mode=request.mode, references=references)
