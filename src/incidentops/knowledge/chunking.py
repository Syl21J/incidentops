"""Markdown-aware deterministic chunking with stable identifiers."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from incidentops.knowledge.models import KnowledgeChunk, KnowledgeDocument

MAX_CHUNK_CHARACTERS = 1_000
CHUNK_OVERLAP_CHARACTERS = 120
MAX_CORPUS_CHUNKS = 5_000
HEADING = re.compile(r"^(?P<marks>#{1,6}) (?P<title>.+)$")
SLUG_PART = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class MarkdownSection:
    """One heading and its body with a complete hierarchical path."""

    heading_path: tuple[str, ...]
    heading_lines: tuple[str, ...]
    body: str


def _slug(value: str) -> str:
    slug = SLUG_PART.sub("-", value.lower()).strip("-")
    return slug or "section"


def _sections(markdown: str) -> list[MarkdownSection]:
    hierarchy: list[tuple[str, str]] = []
    sections: list[MarkdownSection] = []
    current_path: tuple[str, ...] | None = None
    current_headings: tuple[str, ...] = ()
    body_lines: list[str] = []
    occurrences: dict[tuple[str, ...], int] = {}
    in_fence = False

    def finish() -> None:
        if current_path is not None and "\n".join(body_lines).strip():
            sections.append(
                MarkdownSection(
                    heading_path=current_path,
                    heading_lines=current_headings,
                    body="\n".join(body_lines).strip(),
                )
            )

    for line in markdown.splitlines():
        if line.startswith("```"):
            in_fence = not in_fence
        match = None if in_fence else HEADING.fullmatch(line)
        if match is None:
            body_lines.append(line)
            continue
        finish()
        body_lines = []
        level = len(match.group("marks"))
        hierarchy = hierarchy[: level - 1]
        hierarchy.append((line, _slug(match.group("title"))))
        base_path = tuple(item[1] for item in hierarchy)
        occurrence = occurrences.get(base_path, 0)
        occurrences[base_path] = occurrence + 1
        path = (
            base_path if occurrence == 0 else (*base_path[:-1], f"{base_path[-1]}-{occurrence + 1}")
        )
        current_path = path
        current_headings = tuple(item[0] for item in hierarchy)
    finish()
    return sections


def _find_cut(text: str, start: int, maximum: int) -> int:
    hard_end = min(start + maximum, len(text))
    if hard_end == len(text):
        return hard_end
    minimum = start + maximum // 2
    for separator in ("\n\n", "\n", " "):
        position = text.rfind(separator, minimum, hard_end + 1)
        if position >= minimum:
            return position + (len(separator) if separator != " " else 0)
    return hard_end


def _bounded_bodies(body: str, maximum: int) -> list[str]:
    if maximum <= CHUNK_OVERLAP_CHARACTERS:
        raise ValueError("heading path leaves no room for bounded chunk content")
    chunks: list[str] = []
    start = 0
    while start < len(body):
        end = _find_cut(body, start, maximum)
        piece = body[start:end].strip()
        if piece:
            chunks.append(piece)
        if end >= len(body):
            break
        next_start = max(0, end - CHUNK_OVERLAP_CHARACTERS)
        while next_start < end and not body[next_start].isspace():
            next_start += 1
        start = next_start
        while start < len(body) and body[start].isspace():
            start += 1
    return chunks


def chunk_document(document: KnowledgeDocument) -> list[KnowledgeChunk]:
    """Split one validated document without changing heading order or content."""

    chunks: list[KnowledgeChunk] = []
    for section in _sections(document.content):
        prefix = "\n\n".join(section.heading_lines) + "\n\n"
        maximum_body = MAX_CHUNK_CHARACTERS - len(prefix)
        heading_path = "/".join(section.heading_path)
        for chunk_index, body in enumerate(_bounded_bodies(section.body, maximum_body)):
            content = f"{prefix}{body}".strip()
            content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
            chunk_id = f"{document.metadata.document_id}::{heading_path}::{chunk_index:04d}"
            chunks.append(
                KnowledgeChunk(
                    chunk_id=chunk_id,
                    content=content,
                    metadata=document.metadata,
                    source_path=document.source_path,
                    headings=[heading.lstrip("#").strip() for heading in section.heading_lines],
                    heading_path=heading_path,
                    chunk_index=chunk_index,
                    content_hash=content_hash,
                )
            )
    if not chunks:
        raise ValueError(f"{document.source_path}: chunking produced no content")
    return chunks


def chunk_corpus(documents: list[KnowledgeDocument]) -> list[KnowledgeChunk]:
    """Chunk a stable document sequence and enforce a hard corpus bound."""

    chunks = [chunk for document in documents for chunk in chunk_document(document)]
    if len(chunks) > MAX_CORPUS_CHUNKS:
        raise ValueError(f"knowledge corpus exceeds {MAX_CORPUS_CHUNKS} chunks")
    identifiers = [chunk.chunk_id for chunk in chunks]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("chunking produced duplicate chunk identifiers")
    return chunks
