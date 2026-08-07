"""Unit coverage for explicit knowledge embedding providers."""

import math

import pytest

from incidentops.knowledge.embeddings import (
    EMBEDDING_DIMENSION,
    DeterministicEmbeddingProvider,
    EmbeddingError,
    SentenceTransformerEmbeddingProvider,
    create_embedding_provider,
)


def test_deterministic_provider_returns_stable_content_sensitive_batches() -> None:
    provider = DeterministicEmbeddingProvider()

    first = provider.embed_batch(["alpha", "beta"])
    second = provider.embed_batch(["alpha", "beta"])

    assert first == second
    assert first[0] != first[1]
    assert all(len(vector) == EMBEDDING_DIMENSION for vector in first)
    assert all(math.isclose(sum(value * value for value in vector), 1.0) for vector in first)


def test_embedding_provider_switching_is_explicit() -> None:
    deterministic = create_embedding_provider("deterministic-test", "ignored", "cpu")
    real = create_embedding_provider("sentence-transformers", "all-MiniLM-L6-v2", "cpu")

    assert isinstance(deterministic, DeterministicEmbeddingProvider)
    assert isinstance(real, SentenceTransformerEmbeddingProvider)
    with pytest.raises(EmbeddingError, match="unsupported"):
        create_embedding_provider("automatic", "model", "cpu")
    with pytest.raises(EmbeddingError, match="device"):
        create_embedding_provider("sentence-transformers", "model", "cuda")


def test_embedding_batches_must_not_be_empty() -> None:
    with pytest.raises(EmbeddingError, match="must not be empty"):
        DeterministicEmbeddingProvider().embed_batch([])
