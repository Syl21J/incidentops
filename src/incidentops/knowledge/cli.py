"""Command-line interface for controlled knowledge ingestion and retrieval."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from elasticsearch import Elasticsearch
from incidentops.config import Settings
from incidentops.knowledge.chunking import chunk_corpus
from incidentops.knowledge.corpus import load_knowledge_corpus
from incidentops.knowledge.embeddings import EmbeddingError, create_embedding_provider
from incidentops.knowledge.evaluation import (
    RetrievalEvaluationError,
    evaluate_retrieval,
    load_retrieval_dataset,
)
from incidentops.knowledge.index import ElasticsearchKnowledgeIndex, KnowledgeIndexError
from incidentops.knowledge.ingestion import KnowledgeIngestor
from incidentops.knowledge.models import (
    DocumentType,
    IncidentType,
    KnowledgeFilters,
    KnowledgeSearchRequest,
    KnowledgeService,
    RetrievalMode,
)
from incidentops.knowledge.retrieval import KnowledgeRetrievalError, KnowledgeSearchService


def build_parser() -> argparse.ArgumentParser:
    """Build the explicit knowledge management and retrieval CLI."""

    parser = argparse.ArgumentParser(
        prog="incidentops-knowledge",
        description="Validate, ingest, retrieve, and evaluate IncidentOps knowledge.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command, help_text in (
        ("validate", "Validate and chunk the corpus without external calls."),
        ("ingest", "Synchronize the authoritative corpus with Elasticsearch."),
    ):
        command_parser = subparsers.add_parser(command, help=help_text)
        command_parser.add_argument(
            "--knowledge-directory",
            type=Path,
            default=Path("knowledge"),
        )
        if command == "ingest":
            command_parser.add_argument(
                "--dry-run",
                action="store_true",
                help="Plan using read-only index state without embedding or writing.",
            )
    subparsers.add_parser("status", help="Report fixed index compatibility and bounded counts.")

    search = subparsers.add_parser("search", help="Run bounded knowledge retrieval.")
    search.add_argument("--query", required=True)
    search.add_argument("--mode", choices=[item.value for item in RetrievalMode], default="hybrid")
    search.add_argument("--top-k", type=int, default=5)
    search.add_argument("--candidate-k", type=int, default=40)
    search.add_argument(
        "--service",
        action="append",
        choices=[item.value for item in KnowledgeService],
        default=[],
    )
    search.add_argument(
        "--incident-type",
        action="append",
        choices=[item.value for item in IncidentType],
        default=[],
    )
    search.add_argument(
        "--document-type",
        action="append",
        choices=[item.value for item in DocumentType],
        default=[],
    )
    search.add_argument("--status", action="append", choices=["active"], default=[])

    evaluate = subparsers.add_parser("evaluate", help="Evaluate bounded retrieval cases.")
    evaluate.add_argument("--cases", type=Path, required=True)
    evaluate.add_argument(
        "--mode",
        choices=[item.value for item in RetrievalMode],
        default="hybrid",
    )
    evaluate.add_argument("--candidate-k", type=int, default=40)
    return parser


def _client(settings: Settings) -> Elasticsearch:
    return Elasticsearch(
        settings.elasticsearch_url,
        request_timeout=10,
        retry_on_timeout=True,
        max_retries=2,
    )


def _validate(directory: Path) -> tuple[int, int]:
    documents = load_knowledge_corpus(directory)
    chunks = chunk_corpus(documents)
    return len(documents), len(chunks)


def _search_service(client: Elasticsearch, settings: Settings) -> KnowledgeSearchService:
    return KnowledgeSearchService(
        client,
        create_embedding_provider(
            settings.embedding_provider,
            settings.embedding_model,
            settings.embedding_device,
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run one knowledge command with stable JSON output and exit codes."""

    args = build_parser().parse_args(argv)
    client: Elasticsearch | None = None
    try:
        if args.command == "validate":
            documents, chunks = _validate(args.knowledge_directory)
            print(f'{{"documents":{documents},"chunks":{chunks},"valid":true}}')
            return 0

        settings = Settings()
        client = _client(settings)
        index = ElasticsearchKnowledgeIndex(client)
        if args.command == "status":
            print(index.status().model_dump_json())
            return 0

        if args.command == "search":
            result = _search_service(client, settings).search(
                KnowledgeSearchRequest(
                    query=args.query,
                    mode=args.mode,
                    filters=KnowledgeFilters(
                        services=args.service,
                        incident_types=args.incident_type,
                        document_types=args.document_type,
                        statuses=args.status,
                    ),
                    top_k=args.top_k,
                    candidate_k=args.candidate_k,
                )
            )
            print(result.model_dump_json())
            return 0

        if args.command == "evaluate":
            dataset = load_retrieval_dataset(args.cases)
            result = evaluate_retrieval(
                dataset,
                _search_service(client, settings),
                mode=RetrievalMode(args.mode),
                candidate_k=args.candidate_k,
            )
            print(result.model_dump_json())
            return 0

        documents = load_knowledge_corpus(args.knowledge_directory)
        chunks = chunk_corpus(documents)
        provider = create_embedding_provider(
            settings.embedding_provider,
            settings.embedding_model,
            settings.embedding_device,
        )
        report = KnowledgeIngestor(index, provider).ingest(
            chunks,
            document_count=len(documents),
            dry_run=args.dry_run,
        )
        print(report.model_dump_json())
        return 0
    except (
        EmbeddingError,
        KnowledgeIndexError,
        KnowledgeRetrievalError,
        RetrievalEvaluationError,
        OSError,
        ValidationError,
        ValueError,
    ) as error:
        print(f"[ERROR] Knowledge command failed: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(
            f"[ERROR] Knowledge infrastructure failed with {type(error).__name__}.",
            file=sys.stderr,
        )
        return 1
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
