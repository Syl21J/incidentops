"""Run one allow-listed IncidentOps scenario with scoped cleanup."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from incidentops.config import Settings
from incidentops.scenario_runner.runtime import ScenarioRunError, run_scenario, scenario_path
from incidentops.scenarios import load_scenario_manifest


def bounded_seconds(value: str) -> float:
    """Parse a non-negative scenario phase duration capped at two minutes."""

    parsed = float(value)
    if not 0 <= parsed <= 120:
        raise argparse.ArgumentTypeError("value must be between zero and 120 seconds")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the bounded scenario runner CLI."""

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Execute one versioned scenario.")
    run.add_argument("--scenario", required=True)
    run.add_argument("--output-metadata", type=Path)
    run.add_argument(
        "--start-delay-seconds",
        type=bounded_seconds,
        default=0.0,
        help="Expose a healthy idle metrics phase before producing incident traffic.",
    )
    run.add_argument(
        "--minimum-incident-seconds",
        type=bounded_seconds,
        default=0.0,
        help="Keep observing the bounded incident for at least this duration.",
    )
    run.add_argument(
        "--retain-evidence",
        action="store_true",
        help="Retain only run-scoped Elasticsearch evidence for a follow-up investigation.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Execute one scenario and print validated neutral metadata."""

    arguments = build_parser().parse_args(argv)
    try:
        manifest = load_scenario_manifest(scenario_path(arguments.scenario))
        metadata = run_scenario(
            manifest,
            Settings(),
            retain_evidence=arguments.retain_evidence,
            output_metadata=arguments.output_metadata,
            start_delay_seconds=arguments.start_delay_seconds,
            minimum_incident_seconds=arguments.minimum_incident_seconds,
        )
    except (OSError, ValueError, ValidationError, ScenarioRunError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    print(metadata.model_dump_json(indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
