"""Unit coverage for fixed Elasticsearch knowledge index mapping behavior."""

from copy import deepcopy
from typing import cast
from unittest.mock import MagicMock

import pytest

from elasticsearch import Elasticsearch
from incidentops.knowledge.index import (
    INDEX_MAPPINGS,
    KNOWLEDGE_INDEX_NAME,
    ElasticsearchKnowledgeIndex,
    KnowledgeIndexError,
    validate_knowledge_mapping,
)


def test_fixed_mapping_accepts_only_expected_vector_dimension() -> None:
    validate_knowledge_mapping(deepcopy(INDEX_MAPPINGS))
    elasticsearch_response = deepcopy(INDEX_MAPPINGS)
    del elasticsearch_response["properties"]["metadata"]["type"]
    validate_knowledge_mapping(elasticsearch_response)

    incompatible = deepcopy(INDEX_MAPPINGS)
    incompatible["properties"]["embedding"]["dims"] = 768

    with pytest.raises(KnowledgeIndexError, match="dims"):
        validate_knowledge_mapping(incompatible)


def test_index_creation_is_idempotent_and_validates_existing_mapping() -> None:
    client = MagicMock()
    client.indices.exists.side_effect = [False, True]
    client.indices.get_mapping.return_value = {
        KNOWLEDGE_INDEX_NAME: {"mappings": deepcopy(INDEX_MAPPINGS)}
    }
    index = ElasticsearchKnowledgeIndex(cast(Elasticsearch, client))

    index.ensure_index()
    index.ensure_index()

    client.indices.create.assert_called_once_with(
        index=KNOWLEDGE_INDEX_NAME,
        settings={"number_of_shards": 1, "number_of_replicas": 0},
        mappings=INDEX_MAPPINGS,
    )


def test_existing_incompatible_mapping_is_rejected_without_deletion() -> None:
    client = MagicMock()
    client.indices.exists.return_value = True
    incompatible = deepcopy(INDEX_MAPPINGS)
    incompatible["properties"]["embedding"]["dims"] = 12
    client.indices.get_mapping.return_value = {KNOWLEDGE_INDEX_NAME: {"mappings": incompatible}}
    index = ElasticsearchKnowledgeIndex(cast(Elasticsearch, client))

    with pytest.raises(KnowledgeIndexError, match="dims"):
        index.ensure_index()

    client.indices.create.assert_not_called()
    client.indices.delete.assert_not_called()


def test_existing_v1_index_adds_heading_text_mapping_without_deletion() -> None:
    client = MagicMock()
    client.indices.exists.return_value = True
    legacy = deepcopy(INDEX_MAPPINGS)
    del legacy["properties"]["headings"]
    client.indices.get_mapping.side_effect = [
        {KNOWLEDGE_INDEX_NAME: {"mappings": legacy}},
        {KNOWLEDGE_INDEX_NAME: {"mappings": deepcopy(INDEX_MAPPINGS)}},
    ]
    index = ElasticsearchKnowledgeIndex(cast(Elasticsearch, client))

    index.ensure_index()

    client.indices.put_mapping.assert_called_once_with(
        index=KNOWLEDGE_INDEX_NAME,
        properties={"headings": {"type": "text"}},
    )
    client.indices.delete.assert_not_called()
