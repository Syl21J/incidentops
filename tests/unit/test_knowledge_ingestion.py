"""Unit coverage for idempotent, batched, and scoped knowledge ingestion."""

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from incidentops.knowledge.chunking import chunk_corpus
from incidentops.knowledge.corpus import load_knowledge_corpus
from incidentops.knowledge.embeddings import EMBEDDING_DIMENSION, EmbeddingError, EmbeddingProvider
from incidentops.knowledge.ingestion import KnowledgeIngestor
from incidentops.knowledge.models import IndexedChunk

PROJECT_DIR = Path(__file__).resolve().parents[2]


class RecordingProvider(EmbeddingProvider):
    """Small valid provider that records batch-only calls."""

    name = "recording-test"
    model_name = "recording-v1"

    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        self.batch_sizes.append(len(texts))
        return [[1.0] + [0.0] * (EMBEDDING_DIMENSION - 1) for _ in texts]


class MemoryIndex:
    """In-memory implementation of the narrow ingestion backend."""

    def __init__(self) -> None:
        self.created = False
        self.documents: dict[str, dict[str, Any]] = {}
        self.deleted_batches: list[list[str]] = []

    def exists(self) -> bool:
        return self.created

    def ensure_index(self) -> None:
        self.created = True

    def validate_index(self) -> None:
        if not self.created:
            raise ValueError("knowledge index does not exist")

    def fetch_existing_chunks(self) -> dict[str, IndexedChunk]:
        return {
            chunk_id: IndexedChunk(
                chunk_id=chunk_id,
                document_id=document["document_id"],
                indexing_hash=document["indexing_hash"],
            )
            for chunk_id, document in self.documents.items()
        }

    def upsert_chunks(self, documents: Sequence[Mapping[str, Any]]) -> None:
        for document in documents:
            self.documents[str(document["chunk_id"])] = dict(document)

    def delete_chunks(self, chunk_ids: Sequence[str]) -> None:
        self.deleted_batches.append(list(chunk_ids))
        for chunk_id in chunk_ids:
            self.documents.pop(chunk_id, None)


class InvalidDimensionProvider(EmbeddingProvider):
    """Provider used to verify enforcement outside concrete implementations."""

    name = "invalid-test"
    model_name = "invalid-v1"

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        return [[0.0] * 12 for _ in texts]


def _chunks():
    documents = load_knowledge_corpus(PROJECT_DIR / "knowledge")
    return documents, chunk_corpus(documents)


def test_ingestion_is_batched_and_idempotent() -> None:
    documents, chunks = _chunks()
    index = MemoryIndex()
    provider = RecordingProvider()
    ingestor = KnowledgeIngestor(index, provider)

    first = ingestor.ingest(chunks, len(documents), dry_run=False)
    second = ingestor.ingest(chunks, len(documents), dry_run=False)

    assert first.created == len(chunks)
    assert first.updated == 0
    assert first.unchanged == 0
    assert provider.batch_sizes == [32, 32, 32, 24]
    assert second.created == 0
    assert second.updated == 0
    assert second.unchanged == len(chunks)
    assert len(index.documents) == len(chunks)


def test_ingestion_removes_only_exact_stale_chunks_after_upsert() -> None:
    documents, chunks = _chunks()
    index = MemoryIndex()
    provider = RecordingProvider()
    ingestor = KnowledgeIngestor(index, provider)
    ingestor.ingest(chunks, len(documents), dry_run=False)

    retained = chunks[:-1]
    report = ingestor.ingest(retained, len(documents), dry_run=False)

    assert report.stale == 1
    assert report.deleted == 1
    assert index.deleted_batches[-1] == [chunks[-1].chunk_id]
    assert chunks[-1].chunk_id not in index.documents


def test_ingestion_updates_only_changed_chunk_payloads() -> None:
    documents, chunks = _chunks()
    index = MemoryIndex()
    provider = RecordingProvider()
    ingestor = KnowledgeIngestor(index, provider)
    ingestor.ingest(chunks, len(documents), dry_run=False)
    changed_content = chunks[0].content + "\n\nAdditional bounded check."
    changed = chunks[0].model_copy(
        update={
            "content": changed_content,
            "content_hash": hashlib.sha256(changed_content.encode()).hexdigest(),
        }
    )

    report = ingestor.ingest([changed, *chunks[1:]], len(documents), dry_run=False)

    assert report.created == 0
    assert report.updated == 1
    assert report.unchanged == len(chunks) - 1


def test_dry_run_never_creates_embeds_upserts_or_deletes() -> None:
    documents, chunks = _chunks()
    index = MemoryIndex()
    provider = RecordingProvider()

    report = KnowledgeIngestor(index, provider).ingest(chunks, len(documents), dry_run=True)

    assert report.dry_run is True
    assert report.created == len(chunks)
    assert index.created is False
    assert index.documents == {}
    assert index.deleted_batches == []
    assert provider.batch_sizes == []


def test_ingestion_rejects_invalid_vectors_and_empty_authoritative_corpus() -> None:
    documents, chunks = _chunks()
    index = MemoryIndex()

    with pytest.raises(ValueError, match="must not be empty"):
        KnowledgeIngestor(index, RecordingProvider()).ingest([], 0, dry_run=False)
    with pytest.raises(EmbeddingError, match="dimension"):
        KnowledgeIngestor(index, InvalidDimensionProvider()).ingest(
            chunks[:1],
            len(documents),
            dry_run=False,
        )

    assert index.documents == {}
    assert index.deleted_batches == []
