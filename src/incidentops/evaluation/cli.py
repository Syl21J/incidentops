"""Command-line interface for deterministic scenario evaluation."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from incidentops.evaluation.evaluator import evaluate_incident_report
from incidentops.evaluation.models import EvaluationRequest
from incidentops.investigation.models import IncidentReport
from incidentops.investigation.report import write_report_output
from incidentops.scenarios import load_scenario_manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="incidentops-evaluation",
        description="Evaluate a validated report against scenario ground truth.",
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--scenario", type=Path, required=True)
    parser.add_argument("--output-file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Evaluate one report and emit validated JSON."""

    args = _parser().parse_args(argv)
    try:
        request = EvaluationRequest(
            report_path=args.report,
            scenario_path=args.scenario,
        )
        report = IncidentReport.model_validate_json(request.report_path.read_text(encoding="utf-8"))
        manifest = load_scenario_manifest(request.scenario_path)
        payload = evaluate_incident_report(report, manifest).model_dump_json(indent=2) + "\n"
        if args.output_file is not None:
            write_report_output(args.output_file, payload)
            print(f"[INFO] Evaluation written to {args.output_file}", file=sys.stderr)
        else:
            print(payload, end="")
    except (OSError, ValidationError, ValueError) as error:
        print(f"[ERROR] Evaluation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
