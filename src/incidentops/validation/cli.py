"""Command-line boundary for typed repository validation helpers."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from pydantic import AwareDatetime, TypeAdapter, ValidationError

from incidentops.metric_query import MetricQueryError
from incidentops.validation.checks import (
    ValidationCheckError,
    artifact_directory,
    collect_slow_consumer_metrics,
    delete_run_logs,
    error_log_counts,
    load_model,
    log_services_ready,
    log_total,
    prometheus_targets_ready,
    scenario_run_id,
    scenario_window,
    slow_consumer_metrics_ready,
    validate_agent_workflow,
    validate_elasticsearch_mappings,
    validate_live_model_configuration,
    validate_live_rag_workflow,
    validate_log_correlation,
    validate_rag_workflow,
    validate_retrieval_benchmark,
    validate_scenario,
    validate_service_aggregation,
    write_scenario_metadata,
)
from incidentops.validation.models import SlowConsumerMetrics

CommandHandler = Callable[[argparse.Namespace], int]


def _path(value: str) -> Path:
    return Path(value)


def _metadata_run_id(arguments: argparse.Namespace) -> int:
    print(scenario_run_id(arguments.metadata))
    return 0


def _scenario_window(arguments: argparse.Namespace) -> int:
    print(*scenario_window(arguments.metadata))
    return 0


def _artifact_directory(_arguments: argparse.Namespace) -> int:
    print(artifact_directory())
    return 0


def _delete_run_logs(arguments: argparse.Namespace) -> int:
    delete_run_logs(arguments.run_id)
    return 0


def _validate_retrieval(arguments: argparse.Namespace) -> int:
    validate_retrieval_benchmark(arguments.result)
    return 0


def _validate_agent(arguments: argparse.Namespace) -> int:
    lines = validate_agent_workflow(
        arguments.report,
        arguments.evaluation,
        arguments.metadata,
        arguments.artifact_directory,
    )
    print("\n" + "\n".join(lines))
    return 0


def _validate_rag(arguments: argparse.Namespace) -> int:
    lines = validate_rag_workflow(
        arguments.baseline_report,
        arguments.rag_report,
        arguments.baseline_evaluation,
        arguments.rag_evaluation,
    )
    print("\n" + "\n".join(lines))
    return 0


def _validate_live_rag(arguments: argparse.Namespace) -> int:
    lines = validate_live_rag_workflow(
        arguments.report,
        arguments.evaluation,
        arguments.artifact_directory,
    )
    print("\n" + "\n".join(lines))
    return 0


def _validate_live_model(_arguments: argparse.Namespace) -> int:
    model = validate_live_model_configuration()
    print(f"[OK]   Live model configuration is valid for model={model}")
    return 0


def _validate_scenario(arguments: argparse.Namespace) -> int:
    validate_scenario(arguments.scenario, arguments.expected_id)
    return 0


def _prometheus_targets_ready(_arguments: argparse.Namespace) -> int:
    return 0 if prometheus_targets_ready(sys.stdin.read()) else 1


def _collect_slow_metrics(arguments: argparse.Namespace) -> int:
    start = TypeAdapter(AwareDatetime).validate_python(arguments.start)
    metrics = collect_slow_consumer_metrics(start)
    print(metrics.model_dump_json())
    return 0


def _slow_metrics_ready(arguments: argparse.Namespace) -> int:
    metrics = load_model(arguments.metrics, SlowConsumerMetrics)
    ready = slow_consumer_metrics_ready(
        metrics,
        minimum_lag=arguments.minimum_lag,
        minimum_p95_seconds=arguments.minimum_p95_seconds,
    )
    return 0 if ready else 1


def _log_total(arguments: argparse.Namespace) -> int:
    print(log_total(arguments.result))
    return 0


def _error_log_counts(arguments: argparse.Namespace) -> int:
    print(*error_log_counts(arguments.result))
    return 0


def _log_services_ready(arguments: argparse.Namespace) -> int:
    return 0 if log_services_ready(arguments.result) else 1


def _validate_log_correlation(arguments: argparse.Namespace) -> int:
    validate_log_correlation(arguments.result)
    return 0


def _validate_service_aggregation(arguments: argparse.Namespace) -> int:
    producer_count, consumer_count = validate_service_aggregation(arguments.result)
    print(
        "[INFO] Indexed service counts: "
        f"order-producer={producer_count}, order-consumer={consumer_count}"
    )
    return 0


def _metric_values(arguments: argparse.Namespace) -> int:
    metrics = load_model(arguments.metrics, SlowConsumerMetrics)
    print(
        metrics.maximum_lag,
        metrics.p95_seconds,
        metrics.producer_rate,
        metrics.consumer_rate,
    )
    return 0


def _write_scenario_metadata(arguments: argparse.Namespace) -> int:
    write_scenario_metadata(
        arguments.output,
        arguments.metrics,
        run_id=arguments.run_id,
        topic=arguments.topic,
        consumer_group=arguments.consumer_group,
        start_time=arguments.start_time,
        end_time=arguments.end_time,
        slow_processing_log_count=arguments.slow_processing_log_count,
        database_error_count=arguments.database_error_count,
        kafka_error_count=arguments.kafka_error_count,
    )
    return 0


def _validate_elasticsearch_mappings(arguments: argparse.Namespace) -> int:
    validate_elasticsearch_mappings(
        arguments.template,
        arguments.installed_template,
        arguments.index_mappings,
    )
    return 0


def _command(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    name: str,
    handler: CommandHandler,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name)
    parser.set_defaults(handler=handler)
    return parser


def build_parser() -> argparse.ArgumentParser:
    """Build the internal validation command parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    metadata_run_id = _command(subparsers, "metadata-run-id", _metadata_run_id)
    metadata_run_id.add_argument("metadata", type=_path)

    window = _command(subparsers, "scenario-window", _scenario_window)
    window.add_argument("metadata", type=_path)

    _command(subparsers, "artifact-directory", _artifact_directory)

    cleanup = _command(subparsers, "delete-run-logs", _delete_run_logs)
    cleanup.add_argument("run_id")

    retrieval = _command(subparsers, "validate-retrieval-benchmark", _validate_retrieval)
    retrieval.add_argument("result", type=_path)

    agent = _command(subparsers, "validate-agent-workflow", _validate_agent)
    agent.add_argument("--report", type=_path, required=True)
    agent.add_argument("--evaluation", type=_path, required=True)
    agent.add_argument("--metadata", type=_path, required=True)
    agent.add_argument("--artifact-directory", type=_path, required=True)

    rag = _command(subparsers, "validate-rag-workflow", _validate_rag)
    rag.add_argument("--baseline-report", type=_path, required=True)
    rag.add_argument("--rag-report", type=_path, required=True)
    rag.add_argument("--baseline-evaluation", type=_path, required=True)
    rag.add_argument("--rag-evaluation", type=_path, required=True)

    live_rag = _command(subparsers, "validate-live-rag-workflow", _validate_live_rag)
    live_rag.add_argument("--report", type=_path, required=True)
    live_rag.add_argument("--evaluation", type=_path, required=True)
    live_rag.add_argument("--artifact-directory", type=_path, required=True)

    _command(subparsers, "validate-live-model", _validate_live_model)

    scenario = _command(subparsers, "validate-scenario", _validate_scenario)
    scenario.add_argument("scenario", type=_path)
    scenario.add_argument("expected_id")

    _command(subparsers, "prometheus-targets-ready", _prometheus_targets_ready)

    collect = _command(subparsers, "collect-slow-consumer-metrics", _collect_slow_metrics)
    collect.add_argument("--start", required=True)

    metrics_ready = _command(subparsers, "slow-consumer-metrics-ready", _slow_metrics_ready)
    metrics_ready.add_argument("metrics", type=_path)
    metrics_ready.add_argument("minimum_lag", type=float)
    metrics_ready.add_argument("minimum_p95_seconds", type=float)

    total = _command(subparsers, "log-total", _log_total)
    total.add_argument("result", type=_path)

    errors = _command(subparsers, "error-log-counts", _error_log_counts)
    errors.add_argument("result", type=_path)

    services = _command(subparsers, "log-services-ready", _log_services_ready)
    services.add_argument("result", type=_path)

    correlation = _command(subparsers, "validate-log-correlation", _validate_log_correlation)
    correlation.add_argument("result", type=_path)

    aggregation = _command(
        subparsers,
        "validate-service-aggregation",
        _validate_service_aggregation,
    )
    aggregation.add_argument("result", type=_path)

    metric_values = _command(subparsers, "metric-values", _metric_values)
    metric_values.add_argument("metrics", type=_path)

    metadata = _command(subparsers, "write-scenario-metadata", _write_scenario_metadata)
    metadata.add_argument("--output", type=_path, required=True)
    metadata.add_argument("--metrics", type=_path, required=True)
    metadata.add_argument("--run-id", required=True)
    metadata.add_argument("--topic", required=True)
    metadata.add_argument("--consumer-group", required=True)
    metadata.add_argument("--start-time", required=True)
    metadata.add_argument("--end-time", required=True)
    metadata.add_argument("--slow-processing-log-count", type=int, required=True)
    metadata.add_argument("--database-error-count", type=int, required=True)
    metadata.add_argument("--kafka-error-count", type=int, required=True)

    mappings = _command(
        subparsers,
        "validate-elasticsearch-mappings",
        _validate_elasticsearch_mappings,
    )
    mappings.add_argument("--template", type=_path, required=True)
    mappings.add_argument("--installed-template", type=_path, required=True)
    mappings.add_argument("--index-mappings", type=_path, required=True)
    return parser


def run(arguments: argparse.Namespace) -> int:
    """Run one parsed command and render safe validation failures."""

    handler: CommandHandler = arguments.handler
    try:
        return handler(arguments)
    except (
        MetricQueryError,
        OSError,
        ValidationCheckError,
        ValidationError,
        ValueError,
    ) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 2


def main(arguments: Sequence[str] | None = None) -> None:
    """Run the internal validation CLI."""

    sys.exit(run(build_parser().parse_args(arguments)))


if __name__ == "__main__":
    main()
