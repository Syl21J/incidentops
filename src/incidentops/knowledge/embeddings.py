"""Explicit real and deterministic-test embedding providers."""

from __future__ import annotations

import hashlib
import math
from abc import ABC, abstractmethod
from collections.abc import Sequence
from importlib import import_module
from typing import Any

EMBEDDING_DIMENSION = 384


class EmbeddingError(RuntimeError):
    """Raised when an embedding provider cannot produce valid bounded vectors."""


class EmbeddingProvider(ABC):
    """Batch-only embedding interface used by knowledge ingestion."""

    name: str
    model_name: str
    dimension: int = EMBEDDING_DIMENSION

    @abstractmethod
    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed one non-empty bounded batch."""


def validate_embedding_vectors(vectors: list[list[float]], expected_count: int) -> None:
    """Enforce count, dimension, and finite-value guarantees at every provider boundary."""

    if len(vectors) != expected_count:
        raise EmbeddingError(
            f"embedding provider returned {len(vectors)} vectors for {expected_count} texts"
        )
    for vector in vectors:
        if len(vector) != EMBEDDING_DIMENSION:
            raise EmbeddingError(
                f"embedding dimension {len(vector)} does not match {EMBEDDING_DIMENSION}"
            )
        if not all(math.isfinite(value) for value in vector):
            raise EmbeddingError("embedding provider returned a non-finite value")


class DeterministicEmbeddingProvider(EmbeddingProvider):
    """Content-sensitive fixed-dimension vectors used only in explicit test mode."""

    name = "deterministic-test"
    model_name = "sha256-deterministic-v1"

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Generate stable normalized vectors without external model state."""

        if not texts:
            raise EmbeddingError("embedding batch must not be empty")
        vectors: list[list[float]] = []
        for text in texts:
            if not text:
                raise EmbeddingError("embedding text must not be empty")
            values = []
            for index in range(EMBEDDING_DIMENSION):
                digest = hashlib.sha256(f"{index}\0{text}".encode()).digest()
                integer = int.from_bytes(digest[:8], "big")
                values.append((integer / ((1 << 64) - 1)) * 2.0 - 1.0)
            norm = math.sqrt(sum(value * value for value in values))
            vectors.append([value / norm for value in values])
        validate_embedding_vectors(vectors, len(texts))
        return vectors


class SentenceTransformerEmbeddingProvider(EmbeddingProvider):
    """Lazy CPU-only sentence-transformers provider with no fallback behavior."""

    name = "sentence-transformers"

    def __init__(self, model_name: str, device: str) -> None:
        if device != "cpu":
            raise EmbeddingError("sentence-transformers embedding device must be 'cpu'")
        self.model_name = model_name
        self._model: Any | None = None

    def _load_model(self) -> Any:
        if self._model is not None:
            return self._model
        try:
            sentence_transformers = import_module("sentence_transformers")
            sentence_transformer = sentence_transformers.SentenceTransformer
        except ImportError as error:
            raise EmbeddingError("sentence-transformers is not installed") from error
        try:
            model = sentence_transformer(self.model_name, device="cpu")
        except Exception as error:
            raise EmbeddingError(f"could not load embedding model '{self.model_name}'") from error
        dimension = (
            model.get_embedding_dimension()
            if hasattr(model, "get_embedding_dimension")
            else model.get_sentence_embedding_dimension()
        )
        if dimension != EMBEDDING_DIMENSION:
            raise EmbeddingError(
                f"embedding model dimension {dimension} does not match {EMBEDDING_DIMENSION}"
            )
        self._model = model
        return model

    @staticmethod
    def _reject_truncation(model: Any, texts: Sequence[str]) -> None:
        try:
            encoded = model.tokenizer(
                list(texts),
                add_special_tokens=True,
                padding=False,
                truncation=False,
            )
            token_lists = encoded["input_ids"]
        except Exception as error:
            raise EmbeddingError("could not validate embedding input lengths") from error
        maximum = int(model.max_seq_length)
        if any(len(tokens) > maximum for tokens in token_lists):
            raise EmbeddingError(
                f"knowledge chunk exceeds the embedding model limit of {maximum} tokens"
            )

    def embed_batch(self, texts: Sequence[str]) -> list[list[float]]:
        """Embed a batch on CPU and reject dimension mismatch or silent truncation."""

        if not texts:
            raise EmbeddingError("embedding batch must not be empty")
        if any(not text for text in texts):
            raise EmbeddingError("embedding text must not be empty")
        model = self._load_model()
        self._reject_truncation(model, texts)
        try:
            encoded = model.encode(
                list(texts),
                batch_size=len(texts),
                show_progress_bar=False,
                convert_to_numpy=True,
                normalize_embeddings=True,
            )
            vectors = [[float(value) for value in vector] for vector in encoded]
        except Exception as error:
            raise EmbeddingError("sentence-transformers batch embedding failed") from error
        validate_embedding_vectors(vectors, len(texts))
        return vectors


def create_embedding_provider(
    provider_name: str,
    model_name: str,
    device: str,
) -> EmbeddingProvider:
    """Create only the explicitly configured provider."""

    if provider_name == "sentence-transformers":
        return SentenceTransformerEmbeddingProvider(model_name, device)
    if provider_name == "deterministic-test":
        return DeterministicEmbeddingProvider()
    raise EmbeddingError(f"unsupported embedding provider: {provider_name}")
