"""Unit coverage for document-level retrieval evaluation metrics."""

from pathlib import Path

from incidentops.knowledge.evaluation import evaluate_retrieval, load_retrieval_dataset
from incidentops.knowledge.models import (
    KnowledgeReference,
    KnowledgeScores,
    KnowledgeSearchResult,
    RetrievalMode,
)

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _reference(document_id: str, rank: int) -> KnowledgeReference:
    return KnowledgeReference(
        knowledge_reference_id=f"knowledge-{rank:024x}",
        document_id=document_id,
        chunk_id=f"{document_id}::purpose::{rank:04d}",
        title="Retrieved Document",
        snippet="Bounded operational context.",
        scores=KnowledgeScores(fused=1.0 / (60 + rank)),
    )


class EvaluationRetriever:
    def search(self, request):
        del request
        return KnowledgeSearchResult(
            query="consumer lag",
            mode=RetrievalMode.HYBRID,
            references=[
                _reference("irrelevant_doc", 1),
                _reference("relevant_doc", 2),
                _reference("relevant_doc", 3),
                _reference("excluded_doc", 4),
            ],
        )


def test_retrieval_dataset_has_ten_strict_cases() -> None:
    dataset = load_retrieval_dataset(PROJECT_DIR / "evaluation" / "retrieval_cases.yaml")

    assert dataset.schema_version == 1
    assert len(dataset.cases) == 10
    assert {case.case_id for case in dataset.cases} >= {
        "retrieval_malformed_event",
        "retrieval_consumer_rebalance",
    }


def test_recall_precision_mrr_and_exclusions_are_document_level(tmp_path: Path) -> None:
    dataset_path = tmp_path / "cases.yaml"
    dataset_path.write_text(
        """schema_version: 1
cases:
  - case_id: evaluation_case
    query: consumer lag
    relevant_document_ids: [relevant_doc, second_relevant_doc]
    excluded_document_ids: [excluded_doc]
    top_k: 4
""",
        encoding="utf-8",
    )
    dataset = load_retrieval_dataset(dataset_path)

    result = evaluate_retrieval(
        dataset,
        EvaluationRetriever(),
        mode=RetrievalMode.HYBRID,
    )

    assert result.recall_at_k == 0.5
    assert result.precision_at_k == 0.25
    assert result.mrr == 0.5
    assert result.excluded_document_count == 1
    assert result.cases[0].retrieved_document_ids == [
        "irrelevant_doc",
        "relevant_doc",
        "excluded_doc",
    ]
