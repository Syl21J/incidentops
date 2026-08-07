"""Unit coverage for strict corpus validation and deterministic chunking."""

from pathlib import Path

import pytest

from incidentops.knowledge.chunking import MAX_CHUNK_CHARACTERS, chunk_corpus, chunk_document
from incidentops.knowledge.corpus import load_knowledge_corpus, load_knowledge_document

PROJECT_DIR = Path(__file__).resolve().parents[2]


def _markdown(document_id: str = "runbook_test_document", extra: str = "") -> str:
    return f"""---
schema_version: 1
document_id: {document_id}
document_type: runbook
title: Test Runbook
services: [order-consumer]
technologies: [python]
incident_types: [slow_processing]
status: active
updated_at: 2026-08-05
{extra}---
# Test Runbook

## Purpose
Explain the test procedure.

## Symptoms
- Processing is delayed.

## Likely Causes
- A bounded test delay is active.

## Checks
- Check processing duration.

## Safe Actions
- Remove only the confirmed delay.

## Escalation
Escalate when the delay is unexplained.
"""


def _write_runbook(directory: Path, name: str, content: str) -> Path:
    runbooks = directory / "runbooks"
    runbooks.mkdir(parents=True, exist_ok=True)
    path = runbooks / name
    path.write_text(content, encoding="utf-8")
    return path


def test_repository_corpus_is_strict_and_complete() -> None:
    documents = load_knowledge_corpus(PROJECT_DIR / "knowledge")

    assert len(documents) == 20
    assert len({document.metadata.document_id for document in documents}) == 20
    assert len(chunk_corpus(documents)) == 120


def test_metadata_rejects_missing_unknown_and_duplicate_fields(tmp_path: Path) -> None:
    missing = _write_runbook(
        tmp_path / "missing",
        "missing.md",
        _markdown().replace("status: active\n", ""),
    )
    with pytest.raises(ValueError, match="status"):
        load_knowledge_document(missing, tmp_path / "missing")

    unknown = _write_runbook(
        tmp_path / "unknown",
        "unknown.md",
        _markdown(extra="unexpected: true\n"),
    )
    with pytest.raises(ValueError, match="unexpected"):
        load_knowledge_document(unknown, tmp_path / "unknown")

    duplicate = _write_runbook(
        tmp_path / "duplicate",
        "duplicate.md",
        _markdown(extra="title: Repeated Title\n"),
    )
    with pytest.raises(ValueError, match="duplicate YAML key"):
        load_knowledge_document(duplicate, tmp_path / "duplicate")

    unknown_enum = _write_runbook(
        tmp_path / "unknown-enum",
        "unknown-enum.md",
        _markdown().replace("technologies: [python]", "technologies: [unknown]"),
    )
    with pytest.raises(ValueError, match="unknown"):
        load_knowledge_document(unknown_enum, tmp_path / "unknown-enum")


def test_corpus_rejects_duplicate_document_ids(tmp_path: Path) -> None:
    _write_runbook(tmp_path, "first.md", _markdown())
    _write_runbook(tmp_path, "second.md", _markdown())

    with pytest.raises(ValueError, match="duplicate document_id"):
        load_knowledge_corpus(tmp_path)


def test_document_type_must_match_directory(tmp_path: Path) -> None:
    path = _write_runbook(
        tmp_path,
        "wrong-type.md",
        _markdown().replace("document_type: runbook", "document_type: metric"),
    )

    with pytest.raises(ValueError, match="document_type must be 'runbook'"):
        load_knowledge_document(path, tmp_path)


def test_corpus_rejects_markdown_outside_category_directories(tmp_path: Path) -> None:
    _write_runbook(tmp_path, "valid.md", _markdown())
    (tmp_path / "unexpected.md").write_text(_markdown(), encoding="utf-8")

    with pytest.raises(ValueError, match="supported category"):
        load_knowledge_corpus(tmp_path)


def test_non_ascii_content_is_rejected(tmp_path: Path) -> None:
    path = _write_runbook(
        tmp_path,
        "not-english.md",
        _markdown().replace(
            "Explain the test procedure.",
            "Explain the caf\N{LATIN SMALL LETTER E WITH ACUTE} procedure.",
        ),
    )

    with pytest.raises(ValueError, match="ASCII English"):
        load_knowledge_document(path, tmp_path)


def test_chunking_is_bounded_deterministic_and_has_stable_ids(tmp_path: Path) -> None:
    long_checks = " ".join(f"bounded-check-{index}" for index in range(250))
    source = _markdown().replace("- Check processing duration.", long_checks)
    path = _write_runbook(tmp_path, "stable.md", source)
    document = load_knowledge_document(path, tmp_path)

    first = chunk_document(document)
    second = chunk_document(document)

    assert first == second
    assert all(chunk.content and len(chunk.content) <= MAX_CHUNK_CHARACTERS for chunk in first)
    assert len({chunk.chunk_id for chunk in first}) == len(first)
    assert all(chunk.chunk_id.startswith("runbook_test_document::test-runbook/") for chunk in first)
    assert [chunk.chunk_index for chunk in first if chunk.heading_path.endswith("checks")] == list(
        range(sum(chunk.heading_path.endswith("checks") for chunk in first))
    )
