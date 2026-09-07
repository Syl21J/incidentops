"""Execute or validate the bounded IncidentOps multi-incident benchmark."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from incidentops.benchmark.models import MultiIncidentBenchmarkResult
from incidentops.benchmark.replay import (
    EvidenceComparisonResult,
    EvidenceReplayResult,
    compare_bundle,
    load_evidence_bundle,
    load_model_proposal,
    replay_bundle,
    rescore_proposal,
    write_replay_result,
)
from incidentops.benchmark.runner import run_benchmark, write_benchmark
from incidentops.config import Settings
from incidentops.scenarios import load_scenario_manifest
from incidentops.validation.checks import ValidationCheckError


def _table(headers: tuple[str, ...], rows: list[tuple[str, ...]]) -> list[str]:
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    separator = "-+-".join("-" * width for width in widths)
    return [
        " | ".join(value.ljust(widths[index]) for index, value in enumerate(headers)),
        separator,
        *(
            " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))
            for row in rows
        ),
    ]


def _flag(value: bool | None, *, true: str, false: str) -> str:
    return true if value is True else false if value is False else "n/a"


def _percentage(value: float | None) -> str:
    return f"{value:.0%}" if value is not None else "n/a"


def render_benchmark_summary(result: MultiIncidentBenchmarkResult) -> str:
    """Render compact case and aggregate tables for an interactive terminal."""

    case_rows: list[tuple[str, ...]] = []
    details: list[str] = []
    for aggregate in result.aggregates:
        for case in aggregate.cases:
            status = (
                case.investigation_status.value
                if case.investigation_status is not None
                else ("diagnosed" if case.root_cause_exact_match else "unknown")
            )
            case_rows.append(
                (
                    case.approach or case.knowledge_mode,
                    case.knowledge_mode,
                    case.scenario_id,
                    _flag(case.provider_available, true="yes", false="no"),
                    _flag(case.structured_response_valid, true="valid", false="invalid"),
                    case.proposed_root_cause.value if case.proposed_root_cause else "none",
                    case.diagnosed_root_cause.value,
                    status,
                    "yes" if case.root_cause_exact_match else "no",
                    str(case.model_calls),
                )
            )
            for issue in [*case.model_errors, *case.verification_issues]:
                details.append(f"- {case.knowledge_mode}/{case.scenario_id}: {issue}")
    aggregate_rows = [
        (
            aggregate.approach or aggregate.knowledge_mode,
            aggregate.knowledge_mode,
            _percentage(aggregate.provider_availability_rate),
            _percentage(aggregate.structured_response_valid_rate),
            _percentage(aggregate.proposal_root_cause_accuracy),
            _percentage(aggregate.verifier_acceptance_rate),
            f"{aggregate.root_cause_accuracy:.0%}",
            _percentage(aggregate.macro_citation_coverage),
        )
        for aggregate in result.aggregates
    ]
    lines = [
        "Benchmark cases",
        *_table(
            (
                "Approach",
                "Knowledge",
                "Scenario",
                "Provider",
                "Schema",
                "Proposed",
                "Accepted cause",
                "Status",
                "Accepted correct",
                "Calls",
            ),
            case_rows,
        ),
        "",
        "Aggregate results",
        *_table(
            (
                "Approach",
                "Knowledge",
                "Available",
                "Valid",
                "Proposal accuracy",
                "Accepted",
                "Accepted accuracy",
                "Citations",
            ),
            aggregate_rows,
        ),
    ]
    if details:
        lines.extend(["", "Rejections and model errors", *details])
    return "\n".join(lines) + "\n"


def render_replay_summary(result: EvidenceReplayResult) -> str:
    """Render the model proposal and deterministic decision as separate stages."""

    proposed = (
        result.proposal.hypotheses[0].cause_code.value
        if result.proposal.hypotheses
        else "not_available"
    )
    accepted = (
        result.report.primary_root_cause.cause_code.value
        if result.report.primary_root_cause is not None
        else "not_accepted"
    )
    lines = [
        "Evidence replay",
        f"Approach | {result.approach}",
        f"Bundle | {result.evidence_bundle_id}",
        "Provider available | "
        + _flag(result.proposal.provider_available, true="yes", false="no"),
        "Structured response | "
        + ("valid" if result.proposal.structured_response_valid else "invalid"),
        f"Proposed cause | {proposed}",
        f"Verifier status | {result.report.status.value}",
        f"Accepted cause | {accepted}",
        f"Model calls in this replay | {result.report.model_call_count}",
    ]
    if result.approach == "rescore":
        lines.append(f"Calls recorded in saved proposal | {result.proposal.model_call_count}")
    if result.report.verification_issues:
        lines.extend(
            ["Verification issues:", *(f"- {item}" for item in result.report.verification_issues)]
        )
    if result.proposal.model_errors:
        lines.extend(["Model errors:", *(f"- {item}" for item in result.proposal.model_errors)])
    return "\n".join(lines) + "\n"


def render_evidence_comparison_summary(result: EvidenceComparisonResult) -> str:
    """Render the three diagnostic approaches for one immutable evidence bundle."""

    rows = [
        (
            case.approach or "unknown",
            _flag(case.provider_available, true="yes", false="no"),
            _flag(case.structured_response_valid, true="valid", false="invalid"),
            case.proposed_root_cause.value if case.proposed_root_cause else "none",
            _percentage(case.citation_coverage),
            _flag(case.verifier_accepted, true="accepted", false="rejected"),
            "yes" if case.root_cause_exact_match else "no",
            str(case.model_calls),
        )
        for case in result.cases
    ]
    return "\n".join(
        [
            "Evidence comparison",
            f"Bundle: {result.evidence_bundle_id}",
            f"Scenario: {result.scenario_id}",
            "",
            *_table(
                (
                    "Approach",
                    "Provider",
                    "Schema",
                    "Proposed",
                    "Citations",
                    "Verifier",
                    "Accepted correct",
                    "Calls",
                ),
                rows,
            ),
        ]
    ) + "\n"


def _validate_result(result: MultiIncidentBenchmarkResult) -> None:
    """Enforce deterministic Stage Seven acceptance without hiding raw results."""

    for aggregate in result.aggregates:
        if aggregate.scenario_count != 4:
            raise ValidationCheckError("benchmark must contain all four scenarios")
        if aggregate.root_cause_accuracy != 1.0:
            raise ValidationCheckError("deterministic root-cause accuracy is below one")
        if aggregate.macro_evidence_recall != 1.0:
            raise ValidationCheckError("deterministic evidence recall is incomplete")
        if aggregate.macro_negative_evidence_recall != 1.0:
            raise ValidationCheckError("deterministic negative evidence recall is incomplete")
        if aggregate.unsupported_evidence_reference_count:
            raise ValidationCheckError("benchmark contains unsupported evidence references")
        if aggregate.unknown_knowledge_reference_count:
            raise ValidationCheckError("benchmark contains unknown knowledge references")
        if aggregate.forbidden_action_count:
            raise ValidationCheckError("benchmark contains forbidden actions")
        if aggregate.insufficient_evidence_rate:
            raise ValidationCheckError("deterministic benchmark returned insufficient evidence")
        if aggregate.knowledge_mode == "required" and aggregate.macro_knowledge_recall_at_k < 0.5:
            raise ValidationCheckError("required RAG knowledge recall is below one half")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--all", action="store_true", required=True)
    run.add_argument(
        "--model-provider",
        choices=("deterministic-test", "openai", "openai-compatible"),
        default="deterministic-test",
    )
    run.add_argument(
        "--knowledge-mode",
        choices=("disabled", "required", "compare"),
        default="compare",
    )
    run.add_argument("--output-file", type=Path)
    run.add_argument(
        "--output-format",
        choices=("json", "summary"),
        default="json",
    )
    validate = subparsers.add_parser("validate")
    validate.add_argument("result", type=Path)
    summarize = subparsers.add_parser("summarize")
    summarize.add_argument("result", type=Path)
    replay = subparsers.add_parser(
        "replay",
        help="Generate and verify a fresh proposal from a saved evidence bundle.",
    )
    replay.add_argument("evidence", type=Path)
    replay.add_argument(
        "--approach",
        choices=("rules", "llm", "llm-rag"),
        default="rules",
    )
    replay.add_argument("--output-file", type=Path)
    replay.add_argument("--output-format", choices=("json", "summary"), default="summary")
    rescore = subparsers.add_parser(
        "rescore",
        help="Re-run deterministic verification on a saved parsed proposal without a model call.",
    )
    rescore.add_argument("evidence", type=Path)
    rescore.add_argument("proposal", type=Path)
    rescore.add_argument("--output-file", type=Path)
    rescore.add_argument("--output-format", choices=("json", "summary"), default="summary")
    compare = subparsers.add_parser(
        "compare-evidence",
        help="Compare rules, LLM, and LLM with RAG on one saved evidence bundle.",
    )
    compare.add_argument("evidence", type=Path)
    compare.add_argument("--scenario-path", required=True, type=Path)
    compare.add_argument("--output-file", type=Path)
    compare.add_argument("--output-format", choices=("json", "summary"), default="summary")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "compare-evidence":
            comparison = compare_bundle(
                load_evidence_bundle(arguments.evidence),
                load_scenario_manifest(arguments.scenario_path),
                Settings(),
            )
            if arguments.output_file is not None:
                write_replay_result(arguments.output_file, comparison)
            if arguments.output_format == "summary":
                print(render_evidence_comparison_summary(comparison), end="")
            else:
                print(comparison.model_dump_json(indent=2))
            return 0
        if arguments.command in {"replay", "rescore"}:
            evidence = load_evidence_bundle(arguments.evidence)
            replay_result = (
                replay_bundle(evidence, Settings(), approach=arguments.approach)
                if arguments.command == "replay"
                else rescore_proposal(evidence, load_model_proposal(arguments.proposal))
            )
            if arguments.output_file is not None:
                write_replay_result(arguments.output_file, replay_result)
            if arguments.output_format == "summary":
                print(render_replay_summary(replay_result), end="")
            else:
                print(replay_result.model_dump_json(indent=2))
            return 0
        if arguments.command in {"validate", "summarize"}:
            result = MultiIncidentBenchmarkResult.model_validate_json(
                arguments.result.read_text(encoding="utf-8")
            )
            if arguments.command == "summarize":
                print(render_benchmark_summary(result), end="")
                return 0
            _validate_result(result)
            print("IncidentOps multi-incident benchmark validation succeeded.")
            return 0
        provider = (
            "openai-compatible"
            if arguments.model_provider in {"openai", "openai-compatible"}
            else "deterministic-test"
        )
        result = run_benchmark(
            Settings(),
            model_provider=provider,
            knowledge_mode=arguments.knowledge_mode,
        )
        if arguments.output_file is not None:
            write_benchmark(arguments.output_file, result)
        if arguments.output_format == "summary":
            print(render_benchmark_summary(result), end="")
        else:
            print(result.model_dump_json(indent=2))
        return 0
    except (OSError, RuntimeError, ValidationCheckError, ValidationError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
