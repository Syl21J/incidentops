"""Strict and deterministic loading of the controlled Markdown corpus."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from incidentops.knowledge.models import DocumentType, KnowledgeDocument, KnowledgeMetadata

MAX_CORPUS_DOCUMENTS = 100
MAX_DOCUMENT_BYTES = 64_000
DIRECTORY_TYPES = {
    "architecture": DocumentType.ARCHITECTURE,
    "runbooks": DocumentType.RUNBOOK,
    "metrics": DocumentType.METRIC,
    "log-events": DocumentType.LOG_EVENT,
    "incidents": DocumentType.INCIDENT,
}
REQUIRED_SECTIONS = (
    "Purpose",
    "Symptoms",
    "Likely Causes",
    "Checks",
    "Safe Actions",
    "Escalation",
)
FRONT_MATTER = re.compile(r"\A---\n(?P<metadata>.*?)\n---\n(?P<content>.*)\Z", re.DOTALL)
H2_HEADING = re.compile(r"^## (?P<title>[^#].*)$", re.MULTILINE)


class UniqueKeyLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ValueError(f"duplicate YAML key: {key}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _normalize_markdown(raw: str) -> str:
    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(line.rstrip() for line in normalized.split("\n")).strip() + "\n"


def _validate_structure(content: str, metadata: KnowledgeMetadata, source_path: Path) -> None:
    lines = content.splitlines()
    expected_title = f"# {metadata.title}"
    if not lines or lines[0] != expected_title:
        raise ValueError(f"{source_path}: content must start with '{expected_title}'")
    headings = tuple(match.group("title").strip() for match in H2_HEADING.finditer(content))
    if headings != REQUIRED_SECTIONS:
        raise ValueError(
            f"{source_path}: level-two headings must be exactly {', '.join(REQUIRED_SECTIONS)}"
        )
    matches = list(H2_HEADING.finditer(content))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
        if not content[match.end() : end].strip():
            raise ValueError(f"{source_path}: section '{match.group('title')}' must not be empty")


def load_knowledge_document(path: Path, knowledge_directory: Path) -> KnowledgeDocument:
    """Load and strictly validate one Markdown document."""

    relative_path = path.relative_to(knowledge_directory)
    if len(relative_path.parts) != 2 or relative_path.parts[0] not in DIRECTORY_TYPES:
        raise ValueError(f"{relative_path}: knowledge files must be in one supported category")
    if path.is_symlink():
        raise ValueError(f"{relative_path}: symbolic links are not accepted")
    raw_bytes = path.read_bytes()
    if len(raw_bytes) > MAX_DOCUMENT_BYTES:
        raise ValueError(f"{relative_path}: document exceeds {MAX_DOCUMENT_BYTES} bytes")
    try:
        raw = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as error:
        raise ValueError(f"{relative_path}: document must use UTF-8") from error
    normalized = _normalize_markdown(raw)
    match = FRONT_MATTER.fullmatch(normalized)
    if match is None:
        raise ValueError(f"{relative_path}: document must start with YAML front matter")
    try:
        payload = yaml.load(match.group("metadata"), Loader=UniqueKeyLoader)
    except yaml.YAMLError as error:
        raise ValueError(f"{relative_path}: invalid YAML front matter") from error
    if not isinstance(payload, Mapping):
        raise ValueError(f"{relative_path}: front matter must be a YAML mapping")
    try:
        metadata = KnowledgeMetadata.model_validate(payload)
    except ValidationError as error:
        raise ValueError(f"{relative_path}: invalid metadata: {error}") from error
    expected_type = DIRECTORY_TYPES[relative_path.parts[0]]
    if metadata.document_type != expected_type:
        raise ValueError(
            f"{relative_path}: document_type must be '{expected_type.value}' for this directory"
        )
    content = match.group("content").strip()
    _validate_structure(content, metadata, relative_path)
    return KnowledgeDocument(metadata=metadata, content=content, source_path=relative_path)


def load_knowledge_corpus(knowledge_directory: Path) -> list[KnowledgeDocument]:
    """Load a complete corpus in stable path order and reject duplicate identifiers."""

    if not knowledge_directory.is_dir():
        raise ValueError(f"knowledge directory does not exist: {knowledge_directory}")
    paths = sorted(
        knowledge_directory.rglob("*.md"),
        key=lambda item: item.relative_to(knowledge_directory).as_posix(),
    )
    if not paths:
        raise ValueError("knowledge corpus contains no Markdown documents")
    if len(paths) > MAX_CORPUS_DOCUMENTS:
        raise ValueError(f"knowledge corpus exceeds {MAX_CORPUS_DOCUMENTS} documents")
    documents = [load_knowledge_document(path, knowledge_directory) for path in paths]
    seen: dict[str, Path] = {}
    for document in documents:
        document_id = document.metadata.document_id
        if document_id in seen:
            raise ValueError(
                f"duplicate document_id '{document_id}' in {seen[document_id]} and "
                f"{document.source_path}"
            )
        seen[document_id] = document.source_path
    return documents
