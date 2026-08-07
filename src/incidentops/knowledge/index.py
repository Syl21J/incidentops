"""Fixed and strictly validated Elasticsearch knowledge index operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from elasticsearch.helpers import bulk

from elasticsearch import Elasticsearch
from incidentops.knowledge.embeddings import EMBEDDING_DIMENSION
from incidentops.knowledge.models import IndexedChunk, IndexStatus

KNOWLEDGE_INDEX_NAME = "incidentops-knowledge-v1"
MAX_INDEXED_CHUNKS = 5_000
INDEX_SETTINGS: dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
}
INDEX_MAPPINGS: dict[str, Any] = {
    "dynamic": "strict",
    "properties": {
        "document_id": {"type": "keyword"},
        "chunk_id": {"type": "keyword"},
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
        },
        "content": {"type": "text"},
        "headings": {"type": "text"},
        "metadata": {
            "type": "object",
            "dynamic": "strict",
            "properties": {
                "schema_version": {"type": "integer"},
                "document_type": {"type": "keyword"},
                "services": {"type": "keyword"},
                "technologies": {"type": "keyword"},
                "incident_types": {"type": "keyword"},
                "status": {"type": "keyword"},
                "updated_at": {"type": "date", "format": "strict_date"},
            },
        },
        "source_path": {"type": "keyword"},
        "heading_path": {"type": "keyword"},
        "chunk_index": {"type": "integer"},
        "content_hash": {"type": "keyword"},
        "indexing_hash": {"type": "keyword"},
        "embedding_provider": {"type": "keyword"},
        "embedding_model": {"type": "keyword"},
        "embedding": {
            "type": "dense_vector",
            "dims": EMBEDDING_DIMENSION,
            "index": True,
            "similarity": "cosine",
        },
    },
}


class KnowledgeIndexError(RuntimeError):
    """Raised when the fixed knowledge index is unavailable or incompatible."""


def _response_body(response: Any) -> Any:
    return response.body if hasattr(response, "body") else response


def _require_mapping_value(
    actual: Mapping[str, Any],
    expected: Mapping[str, Any],
    path: str,
) -> None:
    for key, expected_value in expected.items():
        if key not in actual:
            if key == "type" and expected_value == "object" and "properties" in actual:
                # Elasticsearch omits the implicit object type from GET mapping responses.
                continue
            raise KnowledgeIndexError(f"knowledge index mapping is missing {path}.{key}")
        actual_value = actual[key]
        if isinstance(expected_value, Mapping):
            if not isinstance(actual_value, Mapping):
                raise KnowledgeIndexError(f"knowledge index mapping has invalid {path}.{key}")
            _require_mapping_value(actual_value, expected_value, f"{path}.{key}")
        elif actual_value != expected_value:
            raise KnowledgeIndexError(
                f"knowledge index mapping mismatch at {path}.{key}: "
                f"expected {expected_value!r}, got {actual_value!r}"
            )


def validate_knowledge_mapping(mapping: Mapping[str, Any]) -> None:
    """Reject any mapping that cannot safely store the v1 knowledge payload."""

    _require_mapping_value(mapping, INDEX_MAPPINGS, "mappings")


class ElasticsearchKnowledgeIndex:
    """Bounded Elasticsearch adapter that never deletes a complete index."""

    def __init__(self, client: Elasticsearch, index_name: str = KNOWLEDGE_INDEX_NAME) -> None:
        self.client = client
        self.index_name = index_name

    def exists(self) -> bool:
        """Return whether the exact configured index exists."""

        return bool(self.client.indices.exists(index=self.index_name))

    def _mapping(self) -> Mapping[str, Any]:
        response = _response_body(self.client.indices.get_mapping(index=self.index_name))
        try:
            mapping = response[self.index_name]["mappings"]
        except (KeyError, TypeError) as error:
            raise KnowledgeIndexError(
                "Elasticsearch returned an invalid knowledge mapping"
            ) from error
        if not isinstance(mapping, Mapping):
            raise KnowledgeIndexError("Elasticsearch returned an invalid knowledge mapping")
        return mapping

    def ensure_index(self) -> None:
        """Create the index idempotently or validate its complete fixed mapping."""

        if not self.exists():
            self.client.indices.create(
                index=self.index_name,
                settings=INDEX_SETTINGS,
                mappings=INDEX_MAPPINGS,
            )
        else:
            properties = self._mapping().get("properties", {})
            if isinstance(properties, Mapping) and "headings" not in properties:
                self.client.indices.put_mapping(
                    index=self.index_name,
                    properties={"headings": INDEX_MAPPINGS["properties"]["headings"]},
                )
        self.validate_index()

    def validate_index(self) -> None:
        """Validate an existing index without changing it."""

        validate_knowledge_mapping(self._mapping())

    def fetch_existing_chunks(self) -> dict[str, IndexedChunk]:
        """Fetch only bounded ingestion state, never arbitrary knowledge content."""

        if not self.exists():
            return {}
        response = _response_body(
            self.client.search(
                index=self.index_name,
                query={"match_all": {}},
                size=MAX_INDEXED_CHUNKS,
                sort=[{"chunk_id": {"order": "asc"}}],
                source_includes=["chunk_id", "document_id", "indexing_hash"],
                track_total_hits=True,
            )
        )
        try:
            total = response["hits"]["total"]["value"]
            hits = response["hits"]["hits"]
        except (KeyError, TypeError) as error:
            raise KnowledgeIndexError("Elasticsearch returned invalid ingestion state") from error
        if not isinstance(total, int) or total > MAX_INDEXED_CHUNKS:
            raise KnowledgeIndexError(
                f"knowledge index exceeds the safe limit of {MAX_INDEXED_CHUNKS} chunks"
            )
        try:
            chunks = [IndexedChunk.model_validate(hit["_source"]) for hit in hits]
        except (KeyError, TypeError, ValueError) as error:
            raise KnowledgeIndexError("knowledge index contains invalid ingestion state") from error
        return {chunk.chunk_id: chunk for chunk in chunks}

    def embedding_identity(self) -> tuple[str, str]:
        """Return the provider and model stored by authoritative ingestion."""

        response = _response_body(
            self.client.search(
                index=self.index_name,
                size=0,
                aggregations={
                    "providers": {"terms": {"field": "embedding_provider", "size": 2}},
                    "models": {"terms": {"field": "embedding_model", "size": 2}},
                },
            )
        )
        try:
            provider_buckets = response["aggregations"]["providers"]["buckets"]
            model_buckets = response["aggregations"]["models"]["buckets"]
            if len(provider_buckets) != 1 or len(model_buckets) != 1:
                raise ValueError("embedding identity must be unique")
            provider = provider_buckets[0]["key"]
            model = model_buckets[0]["key"]
        except (IndexError, KeyError, TypeError, ValueError) as error:
            raise KnowledgeIndexError("knowledge index contains no embedding identity") from error
        if not isinstance(provider, str) or not provider or not isinstance(model, str) or not model:
            raise KnowledgeIndexError("knowledge index contains an invalid embedding identity")
        return provider, model

    def upsert_chunks(self, documents: Sequence[Mapping[str, Any]]) -> None:
        """Bulk upsert exact chunk identifiers and fail on any partial error."""

        if not documents:
            return
        operations = [
            {
                "_op_type": "index",
                "_index": self.index_name,
                "_id": document["chunk_id"],
                "_source": dict(document),
            }
            for document in documents
        ]
        bulk(self.client, operations, refresh="wait_for", raise_on_error=True)

    def delete_chunks(self, chunk_ids: Sequence[str]) -> None:
        """Delete only exact stale chunk IDs after successful replacement upserts."""

        if not chunk_ids:
            return
        operations = [
            {"_op_type": "delete", "_index": self.index_name, "_id": chunk_id}
            for chunk_id in chunk_ids
        ]
        bulk(
            self.client,
            operations,
            refresh="wait_for",
            raise_on_error=True,
            ignore_status=(404,),
        )

    def status(self) -> IndexStatus:
        """Return bounded index compatibility and count information."""

        if not self.exists():
            return IndexStatus(
                index_name=self.index_name,
                exists=False,
                mapping_compatible=None,
                documents=0,
                chunks=0,
            )
        compatible = True
        try:
            validate_knowledge_mapping(self._mapping())
        except KnowledgeIndexError:
            compatible = False
        response = _response_body(
            self.client.search(
                index=self.index_name,
                size=0,
                track_total_hits=True,
                aggregations={"documents": {"cardinality": {"field": "document_id"}}},
            )
        )
        try:
            chunks = response["hits"]["total"]["value"]
            documents = response["aggregations"]["documents"]["value"]
        except (KeyError, TypeError) as error:
            raise KnowledgeIndexError("Elasticsearch returned invalid knowledge status") from error
        return IndexStatus(
            index_name=self.index_name,
            exists=True,
            mapping_compatible=compatible,
            documents=documents,
            chunks=chunks,
        )
