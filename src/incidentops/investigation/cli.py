"""Command-line entry point for the bounded investigation workflow."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import ValidationError

from incidentops.config import Settings
from incidentops.investigation.graph import build_configured_investigation_graph
from incidentops.investigation.model import (
    ModelConfigurationError,
    create_model_provider,
    slow_consumer_scripted_responses,
)
from incidentops.investigation.models import (
    IncidentReport,
    IncidentRequest,
    IncidentStatus,
    ServiceName,
)
from incidentops.investigation.report import (
    persist_investigation_artifacts,
    render_report_markdown,
    write_report_output,
)
from incidentops.investigation.tools import InvestigationToolset


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="incidentops-investigation",
        description="Run the first bounded IncidentOps investigation workflow.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    investigate = subparsers.add_parser(
        "investigate",
        help="Investigate one bounded incident window.",
    )
    investigate.add_argument("--description", required=True)
    investigate.add_argument("--start-time", required=True)
    investigate.add_argument("--end-time", required=True)
    investigate.add_argument("--run-id")
    investigate.add_argument(
        "--affected-service",
        action="append",
        choices=[item.value for item in ServiceName],
        dest="affected_services",
    )
    investigate.add_argument(
        "--output-format",
        choices=("json", "markdown"),
        default="json",
    )
    investigate.add_argument("--output-file", type=Path)
    investigate.add_argument(
        "--model-provider",
        choices=("openai-compatible", "scripted-test"),
        help=("Override LLM_PROVIDER. scripted-test is deterministic and non-production."),
    )
    investigate.add_argument(
        "--persist-artifacts",
        action="store_true",
        help="Persist the validated JSON report and safe JSONL trace locally.",
    )
    return parser


def _settings_for_cli(provider_override: str | None) -> Settings:
    if provider_override is None:
        return Settings()
    return Settings(llm_provider=provider_override)  # type: ignore[arg-type]


def _build_request(args: argparse.Namespace) -> IncidentRequest:
    services = args.affected_services or [ServiceName.ORDER_CONSUMER.value]
    return IncidentRequest.model_validate(
        {
            "description": args.description,
            "start_time": args.start_time,
            "end_time": args.end_time,
            "run_id": args.run_id,
            "affected_services": services,
        }
    )


def _render(report: IncidentReport, output_format: str) -> str:
    if output_format == "markdown":
        return render_report_markdown(report)
    return report.model_dump_json(indent=2) + "\n"


def _run_investigation(args: argparse.Namespace) -> int:
    settings = _settings_for_cli(args.model_provider)
    request = _build_request(args)
    scripted_responses = (
        slow_consumer_scripted_responses() if settings.llm_provider == "scripted-test" else None
    )
    model_provider = create_model_provider(
        settings,
        scripted_responses=scripted_responses,
    )
    toolset = InvestigationToolset.from_settings(settings)
    print(
        f"[INFO] Starting bounded investigation with {settings.llm_provider}",
        file=sys.stderr,
    )
    try:
        graph = build_configured_investigation_graph(settings, model_provider, toolset)
        final_state = graph.invoke({"incident_request": request})
    finally:
        toolset.close()

    report = final_state.get("final_report")
    if report is None:
        raise RuntimeError("investigation graph completed without a final report")
    rendered = _render(report, args.output_format)
    if args.output_file is not None:
        write_report_output(args.output_file, rendered)
        print(f"[INFO] Report written to {args.output_file}", file=sys.stderr)
    else:
        print(rendered, end="")

    if args.persist_artifacts:
        paths = persist_investigation_artifacts(
            report,
            final_state.get("trace_events", []),
            settings.investigation_artifact_directory,
        )
        print(f"[INFO] JSON artifact: {paths.report_path}", file=sys.stderr)
        print(f"[INFO] JSONL trace: {paths.trace_path}", file=sys.stderr)

    print(f"[INFO] Investigation completed: {report.status.value}", file=sys.stderr)
    return 1 if report.status == IncidentStatus.PIPELINE_ERROR else 0


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments and return a stable process exit code."""

    parser = _parser()
    args = parser.parse_args(argv)
    try:
        return _run_investigation(args)
    except (ModelConfigurationError, ValidationError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(
            f"[ERROR] Investigation pipeline failed with {type(error).__name__}.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
