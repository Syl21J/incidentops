"""Deterministic, idempotent knowledge ingestion orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from incidentops.knowledge.embeddings import EmbeddingProvider, validate_embedding_vectors
from incidentops.knowledge.models import IndexedChunk, IngestionReport, KnowledgeChunk

EMBEDDING_BATCH_SIZE = 32


class KnowledgeIndexBackend(Protocol):
    """Narrow write surface required by deterministic ingestion."""

    def exists(self) -> bool: ...

    def ensure_index(self) -> None: ...

    def validate_index(self) -> None: ...

    def fetch_existing_chunks(self) -> dict[str, IndexedChunk]: ...

    def upsert_chunks(self, documents: Sequence[Mapping[str, Any]]) -> None: ...

    def delete_chunks(self, chunk_ids: Sequence[str]) -> None: ...


def indexing_hash(chunk: KnowledgeChunk, provider: EmbeddingProvider) -> str:
    """Hash every stored non-vector input plus provider identity canonically."""

    payload = {
        "chunk_id": chunk.chunk_id,
        "content": chunk.content,
        "content_hash": chunk.content_hash,
        "heading_path": chunk.heading_path,
        "headings": chunk.headings,
        "chunk_index": chunk.chunk_index,
        "metadata": chunk.metadata.model_dump(mode="json"),
        "source_path": chunk.source_path.as_posix(),
        "embedding_provider": provider.name,
        "embedding_model": provider.model_name,
        "embedding_dimension": provider.dimension,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _index_document(
    chunk: KnowledgeChunk,
    provider: EmbeddingProvider,
    vector: list[float],
    digest: str,
) -> dict[str, Any]:
    metadata = chunk.metadata.model_dump(mode="json")
    title = metadata.pop("title")
    document_id = metadata.pop("document_id")
    return {
        "document_id": document_id,
        "chunk_id": chunk.chunk_id,
        "title": title,
        "content": chunk.content,
        "headings": " > ".join(chunk.headings),
        "metadata": metadata,
        "source_path": chunk.source_path.as_posix(),
        "heading_path": chunk.heading_path,
        "chunk_index": chunk.chunk_index,
        "content_hash": chunk.content_hash,
        "indexing_hash": digest,
        "embedding_provider": provider.name,
        "embedding_model": provider.model_name,
        "embedding": vector,
    }


class KnowledgeIngestor:
    """Plan first, then batch embed, upsert, and remove only exact stale chunks."""

    def __init__(self, index: KnowledgeIndexBackend, provider: EmbeddingProvider) -> None:
        self.index = index
        self.provider = provider

    def ingest(
        self,
        chunks: list[KnowledgeChunk],
        document_count: int,
        dry_run: bool,
    ) -> IngestionReport:
        """Synchronize the authoritative validated corpus with the fixed index."""

        if not chunks or document_count < 1:
            raise ValueError("authoritative knowledge corpus must not be empty")
        index_exists = self.index.exists()
        if dry_run and index_exists:
            self.index.validate_index()
        elif not dry_run:
            self.index.ensure_index()
            index_exists = True
        existing = self.index.fetch_existing_chunks() if index_exists else {}
        digests = {chunk.chunk_id: indexing_hash(chunk, self.provider) for chunk in chunks}
        created = [chunk for chunk in chunks if chunk.chunk_id not in existing]
        updated = [
            chunk
            for chunk in chunks
            if chunk.chunk_id in existing
            and existing[chunk.chunk_id].indexing_hash != digests[chunk.chunk_id]
        ]
        unchanged = len(chunks) - len(created) - len(updated)
        current_ids = set(digests)
        stale = sorted(set(existing) - current_ids)
        if not dry_run:
            changed = created + updated
            indexed_documents: list[dict[str, Any]] = []
            for offset in range(0, len(changed), EMBEDDING_BATCH_SIZE):
                batch = changed[offset : offset + EMBEDDING_BATCH_SIZE]
                vectors = self.provider.embed_batch([chunk.content for chunk in batch])
                validate_embedding_vectors(vectors, len(batch))
                indexed_documents.extend(
                    _index_document(chunk, self.provider, vector, digests[chunk.chunk_id])
                    for chunk, vector in zip(batch, vectors, strict=True)
                )
            self.index.upsert_chunks(indexed_documents)
            self.index.delete_chunks(stale)
        return IngestionReport(
            dry_run=dry_run,
            documents=document_count,
            chunks=len(chunks),
            created=len(created),
            updated=len(updated),
            unchanged=unchanged,
            stale=len(stale),
            deleted=0 if dry_run else len(stale),
        )
