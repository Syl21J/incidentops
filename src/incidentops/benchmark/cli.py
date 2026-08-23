"""Execute or validate the bounded IncidentOps multi-incident benchmark."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from incidentops.benchmark.models import MultiIncidentBenchmarkResult
from incidentops.benchmark.runner import run_benchmark, write_benchmark
from incidentops.config import Settings
from incidentops.validation.checks import ValidationCheckError


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
    validate = subparsers.add_parser("validate")
    validate.add_argument("result", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "validate":
            result = MultiIncidentBenchmarkResult.model_validate_json(
                arguments.result.read_text(encoding="utf-8")
            )
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
        print(result.model_dump_json(indent=2))
        return 0
    except (OSError, RuntimeError, ValidationCheckError, ValidationError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
