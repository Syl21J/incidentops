"""Deterministic checks shared by the repository's shell entry points."""

from __future__ import annotations

import json
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, TypeAdapter

from elasticsearch import Elasticsearch
from incidentops.config import Settings
from incidentops.investigation.models import (
    EvaluationResult,
    EvidenceSource,
    Identifier,
    IncidentReport,
    IncidentStatus,
    InvestigationTraceEvent,
    InvestigationTraceEventType,
    LogEvidence,
    MetricEvidence,
    RootCauseCode,
    RootCauseHypothesis,
)
from incidentops.knowledge.evaluation import RetrievalEvaluationResult
from incidentops.log_search import INDEX_PATTERN, LogCountResult, LogSearchResult
from incidentops.metric_query import (
    PrometheusClient,
    compare_production_and_processing_rates,
    get_consumer_lag_summary,
    get_processing_latency_summary,
)
from incidentops.scenarios import load_scenario_manifest
from incidentops.validation.models import (
    ScenarioMetadata,
    SlowConsumerMetrics,
    SlowConsumerObservations,
)

DATABASE_ERROR_EVENTS = frozenset({"database_connection_failed", "database_write_failed"})
KAFKA_ERROR_EVENTS = frozenset(
    {"consumer_error", "producer_error", "delivery_failed", "topic_error"}
)
APPLICATION_SERVICES = frozenset({"order-producer", "order-consumer"})
PROMETHEUS_JOBS = frozenset({"incidentops-producer", "incidentops-consumer"})
CLEANUP_RUN_ID_PATTERN = re.compile(r"^(?:slow-consumer|log-pipeline-check)-\d+-\d+$")


class ValidationCheckError(RuntimeError):
    """Report a failed deterministic repository check."""


class _PrometheusTarget(BaseModel):
    model_config = ConfigDict(extra="ignore")

    health: str
    labels: dict[str, str]


class _PrometheusTargetData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    activeTargets: list[_PrometheusTarget]


class _PrometheusTargetResponse(BaseModel):
    model_config = ConfigDict(extra="ignore")

    data: _PrometheusTargetData


def load_model[ModelT: BaseModel](path: Path, model: type[ModelT]) -> ModelT:
    """Load and validate one UTF-8 JSON file as a Pydantic model."""

    return model.model_validate_json(path.read_text(encoding="utf-8"))


def load_json_object(path: Path) -> dict[str, Any]:
    """Load one JSON object while rejecting non-object roots."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValidationCheckError(f"expected a JSON object in {path}")
    return payload


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationCheckError(message)


def scenario_window(path: Path) -> tuple[str, str, str]:
    """Return the validated run identifier and exact UTC scenario window."""

    metadata = load_model(path, ScenarioMetadata)
    return (
        metadata.run_id,
        metadata.start_time.isoformat(),
        metadata.end_time.isoformat(),
    )


def scenario_run_id(path: Path) -> str:
    """Recover a validated run identifier from retained scenario metadata."""

    return load_model(path, ScenarioMetadata).run_id


def delete_run_logs(run_id: str, *, elasticsearch_url: str | None = None) -> None:
    """Delete only Elasticsearch log documents carrying one exact validated run identifier."""

    validated_run_id = TypeAdapter(Identifier).validate_python(run_id)
    if CLEANUP_RUN_ID_PATTERN.fullmatch(validated_run_id) is None:
        raise ValidationCheckError("run_id is not an isolated validation-run identifier")
    client = Elasticsearch(
        elasticsearch_url or Settings().elasticsearch_url,
        request_timeout=10,
    )
    try:
        client.delete_by_query(
            index=INDEX_PATTERN,
            query={"term": {"run_id": validated_run_id}},
            allow_no_indices=True,
            conflicts="proceed",
            ignore_unavailable=True,
            refresh=True,
        )
    finally:
        client.close()


def validate_retrieval_benchmark(path: Path) -> RetrievalEvaluationResult:
    """Enforce the deterministic acceptance thresholds for hybrid retrieval."""

    result = load_model(path, RetrievalEvaluationResult)
    _require(result.case_count == 10, "retrieval benchmark must contain ten cases")
    _require(result.recall_at_k >= 0.8, "retrieval recall@k is below 0.8")
    _require(result.precision_at_k >= 0.3, "retrieval precision@k is below 0.3")
    _require(result.mrr >= 0.8, "retrieval MRR is below 0.8")
    _require(result.excluded_document_count == 0, "retrieval returned an excluded document")
    return result


def _require_slow_consumer_diagnosis(report: IncidentReport) -> RootCauseHypothesis:
    _require(report.status == IncidentStatus.DIAGNOSED, "report status is not diagnosed")
    primary = report.primary_root_cause
    if primary is None:
        raise ValidationCheckError("report has no primary root cause")
    _require(
        primary.cause_code == RootCauseCode.SLOW_CONSUMER_PROCESSING,
        "report primary root cause is not slow consumer processing",
    )
    return primary


def _require_complete_evaluation(evaluation: EvaluationResult) -> None:
    _require(evaluation.root_cause_exact_match, "root cause does not match the scenario")
    _require(evaluation.root_cause_rank == 1, "expected root cause is not ranked first")
    _require(evaluation.expected_metric_evidence_recall == 1.0, "metric evidence is incomplete")
    _require(evaluation.expected_log_evidence_recall == 1.0, "log evidence is incomplete")
    _require(evaluation.negative_evidence_recall == 1.0, "negative evidence is incomplete")
    _require(
        evaluation.unsupported_evidence_reference_count == 0,
        "evaluation found unsupported evidence references",
    )
    _require(evaluation.forbidden_action_count == 0, "evaluation found a forbidden action")
    _require(evaluation.tool_call_count <= 10, "workflow exceeded the tool-call limit")
    _require(
        evaluation.investigation_attempt_count <= 2,
        "workflow exceeded the investigation-attempt limit",
    )


def _artifact_paths(report: IncidentReport, artifact_directory: Path) -> tuple[Path, Path]:
    return (
        artifact_directory / f"{report.investigation_id}.report.json",
        artifact_directory / f"{report.investigation_id}.trace.jsonl",
    )


def validate_agent_workflow(
    report_path: Path,
    evaluation_path: Path,
    metadata_path: Path,
    artifact_directory: Path,
) -> list[str]:
    """Validate the scripted agent workflow and its persisted artifacts."""

    report = load_model(report_path, IncidentReport)
    evaluation = load_model(evaluation_path, EvaluationResult)
    metadata = load_model(metadata_path, ScenarioMetadata)
    primary = _require_slow_consumer_diagnosis(report)
    _require_complete_evaluation(evaluation)
    _require(report.tool_call_count == 6, "scripted workflow did not make exactly six tool calls")

    metric_evidence = [
        item
        for item in report.supporting_evidence
        if isinstance(item, MetricEvidence) and item.source == EvidenceSource.PROMETHEUS
    ]
    log_evidence = [
        item
        for item in report.supporting_evidence
        if isinstance(item, LogEvidence) and item.source == EvidenceSource.ELASTICSEARCH
    ]
    _require(len(metric_evidence) == 3, "report does not contain three Prometheus summaries")
    _require(bool(log_evidence), "report contains no positive Elasticsearch evidence")
    _require(len(report.negative_evidence) == 2, "report does not contain two negative checks")

    report_artifact, trace_artifact = _artifact_paths(report, artifact_directory)
    _require(report_artifact.is_file(), f"persisted report is missing: {report_artifact}")
    _require(trace_artifact.is_file(), f"persisted trace is missing: {trace_artifact}")
    trace_events = [
        InvestigationTraceEvent.model_validate_json(line)
        for line in trace_artifact.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    event_types = {item.event_type for item in trace_events}
    required_events = {
        InvestigationTraceEventType.TOOL_CALLED,
        InvestigationTraceEventType.TOOL_COMPLETED,
        InvestigationTraceEventType.VERIFICATION_COMPLETED,
        InvestigationTraceEventType.INVESTIGATION_COMPLETED,
    }
    _require(required_events <= event_types, "persisted trace is missing required lifecycle events")

    observations = metadata.observations
    return [
        "IncidentOps agent workflow validation succeeded.",
        f"Root cause: {primary.cause_code.value}",
        f"Diagnosis status: {report.status.value}",
        "Metric evidence found: 3/3",
        f"Log evidence found: {len(log_evidence)}/1",
        f"Negative evidence found: {len(report.negative_evidence)}/2",
        f"Tool calls: {evaluation.tool_call_count}",
        f"Investigation attempts: {evaluation.investigation_attempt_count}",
        f"Unsupported references: {evaluation.unsupported_evidence_reference_count}",
        f"Forbidden actions: {evaluation.forbidden_action_count}",
        f"Workflow duration: {evaluation.workflow_duration_seconds:.3f} seconds",
        f"Real maximum lag: {observations.maximum_lag}",
        f"Real P95 processing duration: {observations.p95_seconds} seconds",
        f"Real slow-processing logs: {observations.slow_processing_log_count}",
        f"Persisted report: {report_artifact}",
        f"Persisted trace: {trace_artifact}",
        f"Persisted evaluation: {evaluation_path}",
    ]


def validate_rag_workflow(
    baseline_report_path: Path,
    rag_report_path: Path,
    baseline_evaluation_path: Path,
    rag_evaluation_path: Path,
) -> list[str]:
    """Compare baseline and deterministic RAG runs over the same evidence."""

    baseline = load_model(baseline_report_path, IncidentReport)
    rag = load_model(rag_report_path, IncidentReport)
    baseline_evaluation = load_model(baseline_evaluation_path, EvaluationResult)
    rag_evaluation = load_model(rag_evaluation_path, EvaluationResult)
    baseline_primary = _require_slow_consumer_diagnosis(baseline)
    rag_primary = _require_slow_consumer_diagnosis(rag)
    _require(baseline.tool_call_count == rag.tool_call_count == 6, "tool-call counts differ")
    _require(
        baseline.supporting_evidence == rag.supporting_evidence,
        "baseline and RAG supporting evidence differ",
    )
    _require(
        baseline.negative_evidence == rag.negative_evidence,
        "baseline and RAG negative evidence differ",
    )
    _require(not baseline.knowledge_references, "baseline unexpectedly contains knowledge")
    _require(baseline.knowledge_retrieval_count == 0, "baseline performed knowledge retrieval")
    _require(bool(rag.knowledge_references), "RAG report contains no knowledge references")
    _require(rag.knowledge_retrieval_count == 1, "RAG workflow did not retrieve exactly once")
    _require(baseline_evaluation.root_cause_exact_match, "baseline root cause is incorrect")
    _require(rag_evaluation.root_cause_exact_match, "RAG root cause is incorrect")
    _require(baseline_evaluation.forbidden_action_count == 0, "baseline has a forbidden action")
    _require(rag_evaluation.forbidden_action_count == 0, "RAG run has a forbidden action")
    _require(
        rag_evaluation.unsupported_knowledge_reference_count == 0,
        "RAG evaluation found an unsupported knowledge reference",
    )
    return [
        "IncidentOps RAG workflow validation succeeded.",
        f"Baseline root cause: {baseline_primary.cause_code.value}",
        f"RAG root cause: {rag_primary.cause_code.value}",
        f"Live tool calls in both runs: {rag.tool_call_count}",
        f"Validated knowledge references: {len(rag.knowledge_references)}",
    ]


def validate_live_rag_workflow(
    report_path: Path,
    evaluation_path: Path,
    artifact_directory: Path,
) -> list[str]:
    """Validate a live-model RAG report without relaxing local limits."""

    report = load_model(report_path, IncidentReport)
    evaluation = load_model(evaluation_path, EvaluationResult)
    primary = _require_slow_consumer_diagnosis(report)
    _require_complete_evaluation(evaluation)
    _require(report.knowledge_retrieval_count in {1, 2}, "knowledge retrieval count is invalid")
    _require(bool(report.knowledge_references), "live RAG report contains no knowledge references")
    _require(
        evaluation.unsupported_knowledge_reference_count == 0,
        "live evaluation found an unsupported knowledge reference",
    )
    _require(report.model_call_count <= 4, "live workflow exceeded the model-call limit")
    report_artifact, trace_artifact = _artifact_paths(report, artifact_directory)
    _require(report_artifact.is_file(), f"persisted report is missing: {report_artifact}")
    _require(trace_artifact.is_file(), f"persisted trace is missing: {trace_artifact}")
    return [
        "IncidentOps live-model RAG validation succeeded.",
        f"Root cause: {primary.cause_code.value}",
        f"Model calls: {report.model_call_count}/4",
        f"Tool calls: {report.tool_call_count}/10",
        f"Investigation attempts: {report.investigation_attempts}/2",
        f"Knowledge references: {len(report.knowledge_references)}",
        f"Persisted report: {report_artifact}",
        f"Persisted trace: {trace_artifact}",
        f"Persisted evaluation: {evaluation_path}",
    ]


def validate_live_model_configuration() -> str:
    """Construct the live provider to validate configuration without making a model call."""

    from incidentops.investigation.model import (
        ModelConfigurationError,
        OpenAICompatibleModelProvider,
    )

    settings = Settings(llm_provider="openai-compatible")
    try:
        OpenAICompatibleModelProvider(settings)
    except ModelConfigurationError as error:
        raise ValidationCheckError(str(error)) from error
    if settings.llm_model is None:
        raise ValidationCheckError("live model is missing after provider validation")
    return settings.llm_model


def validate_scenario(path: Path, expected_id: str) -> None:
    """Validate a scenario manifest and its expected stable identifier."""

    manifest = load_scenario_manifest(path)
    _require(manifest.id == expected_id, f"unexpected scenario manifest ID: {manifest.id}")


def prometheus_targets_ready(payload: str) -> bool:
    """Return whether both application scrape targets are healthy."""

    response = _PrometheusTargetResponse.model_validate_json(payload)
    healthy = {
        target.labels.get("job") for target in response.data.activeTargets if target.health == "up"
    }
    return PROMETHEUS_JOBS <= healthy


def collect_slow_consumer_metrics(start: datetime) -> SlowConsumerMetrics:
    """Collect only the fixed, bounded summaries used by the slow-consumer check."""

    end = datetime.now(UTC)
    client = PrometheusClient(Settings().prometheus_url)
    lag = get_consumer_lag_summary(client, start=start, end=end, step_seconds=2)
    latency = get_processing_latency_summary(
        client,
        percentile=0.95,
        start=start,
        end=end,
        step_seconds=2,
    )
    rates = compare_production_and_processing_rates(
        client,
        start=start,
        end=end,
        step_seconds=2,
    )
    return SlowConsumerMetrics(
        maximum_lag=lag.maximum,
        lag_start=lag.start_value,
        lag_end=lag.end_value,
        lag_trend=lag.trend,
        lag_samples=lag.sample_count,
        p95_seconds=latency.duration_seconds,
        latency_samples=latency.sample_count,
        producer_rate=rates.producer_rate,
        consumer_rate=rates.consumer_rate,
        consumer_is_slower=rates.consumer_is_slower,
    )


def slow_consumer_metrics_ready(
    metrics: SlowConsumerMetrics,
    *,
    minimum_lag: float,
    minimum_p95_seconds: float,
) -> bool:
    """Apply the scenario's explicit metric acceptance thresholds."""

    return (
        metrics.maximum_lag >= minimum_lag
        and metrics.lag_end > metrics.lag_start
        and metrics.lag_trend == "increasing"
        and metrics.lag_samples >= 4
        and metrics.p95_seconds >= minimum_p95_seconds
        and metrics.latency_samples >= 2
        and metrics.consumer_is_slower
        and metrics.producer_rate > metrics.consumer_rate
    )


def log_total(path: Path) -> int:
    """Return the validated total from one bounded log-search result."""

    return load_model(path, LogSearchResult).total


def error_log_counts(path: Path) -> tuple[int, int]:
    """Count fixed database and Kafka error categories in a bounded search result."""

    result = load_model(path, LogSearchResult)
    database = sum(item.event_type in DATABASE_ERROR_EVENTS for item in result.logs)
    kafka = sum(item.event_type in KAFKA_ERROR_EVENTS for item in result.logs)
    return database, kafka


def log_services_ready(path: Path) -> bool:
    """Return whether a bounded search contains both application services."""

    result = load_model(path, LogSearchResult)
    return APPLICATION_SERVICES <= {item.service for item in result.logs}


def validate_log_correlation(path: Path) -> None:
    """Require at least one indexed correlation identifier."""

    result = load_model(path, LogSearchResult)
    _require(
        any(item.event_id or item.order_id for item in result.logs),
        "no indexed log contains an event_id or order_id",
    )


def validate_service_aggregation(path: Path) -> tuple[int, int]:
    """Require producer and consumer buckets and return their counts."""

    result = load_model(path, LogCountResult)
    _require(result.group_by == "service", "aggregation is not grouped by service")
    counts = {bucket.key: bucket.count for bucket in result.buckets}
    _require(APPLICATION_SERVICES <= counts.keys(), "service aggregation is incomplete")
    return counts["order-producer"], counts["order-consumer"]


def write_scenario_metadata(
    output_path: Path,
    metrics_path: Path,
    *,
    run_id: str,
    topic: str,
    consumer_group: str,
    start_time: str,
    end_time: str,
    slow_processing_log_count: int,
    database_error_count: int,
    kafka_error_count: int,
) -> None:
    """Validate and atomically persist metadata for one retained scenario run."""

    metrics = load_model(metrics_path, SlowConsumerMetrics)
    metadata = ScenarioMetadata(
        run_id=run_id,
        topic=topic,
        consumer_group=consumer_group,
        start_time=TypeAdapter(AwareDatetime).validate_python(start_time),
        end_time=TypeAdapter(AwareDatetime).validate_python(end_time),
        observations=SlowConsumerObservations(
            **metrics.model_dump(),
            slow_processing_log_count=slow_processing_log_count,
            database_error_count=database_error_count,
            kafka_error_count=kafka_error_count,
        ),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        delete=False,
    ) as handle:
        json.dump(metadata.model_dump(mode="json"), handle, indent=2, sort_keys=True)
        handle.write("\n")
        temporary_path = Path(handle.name)
    temporary_path.replace(output_path)


def validate_elasticsearch_mappings(
    template_path: Path,
    installed_template_path: Path,
    index_mappings_path: Path,
) -> None:
    """Compare installed and existing field types with the versioned index template."""

    expected_template = load_json_object(template_path)
    template_response = load_json_object(installed_template_path)
    index_mappings = load_json_object(index_mappings_path)
    try:
        expected_properties = expected_template["template"]["mappings"]["properties"]
        installed_properties = template_response["index_templates"][0]["index_template"][
            "template"
        ]["mappings"]["properties"]
    except (KeyError, IndexError, TypeError) as error:
        raise ValidationCheckError(
            "Elasticsearch template response has an invalid shape"
        ) from error
    expected_types = {
        field: definition["type"] for field, definition in expected_properties.items()
    }
    installed_types = {
        field: definition["type"] for field, definition in installed_properties.items()
    }
    _require(
        installed_types == expected_types,
        "installed index template mapping does not match the versioned mapping",
    )
    for index_name, index_definition in index_mappings.items():
        try:
            properties = index_definition["mappings"].get("properties", {})
            actual_types = {
                field: properties.get(field, {}).get("type") for field in expected_types
            }
        except (AttributeError, KeyError, TypeError) as error:
            raise ValidationCheckError(f"index {index_name} has invalid mappings") from error
        mismatches = {
            field: (expected_type, actual_types[field])
            for field, expected_type in expected_types.items()
            if actual_types[field] != expected_type
        }
        _require(
            not mismatches,
            f"existing index {index_name} has incompatible mappings: {mismatches}",
        )


def artifact_directory() -> Path:
    """Return the configured investigation artifact directory."""

    return Settings().investigation_artifact_directory
