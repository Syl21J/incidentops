"""Sequential multi-incident execution with same-window RAG comparison."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from elasticsearch import Elasticsearch
from incidentops.benchmark.aggregation import aggregate_cases, compare_aggregates
from incidentops.benchmark.models import (
    BenchmarkCaseResult,
    KnowledgeMode,
    MultiIncidentBenchmarkResult,
)
from incidentops.config import Settings
from incidentops.evaluation.evaluator import evaluate_incident_report
from incidentops.investigation.graph import build_configured_investigation_graph
from incidentops.investigation.model import create_model_provider
from incidentops.investigation.models import (
    IncidentReport,
    IncidentRequest,
    RootCauseCode,
    ServiceName,
)
from incidentops.investigation.report import persist_investigation_artifacts
from incidentops.investigation.tools import InvestigationToolset
from incidentops.knowledge.embeddings import create_embedding_provider
from incidentops.knowledge.retrieval import KnowledgeSearchService
from incidentops.scenario_runner.runtime import run_scenario, scenario_path
from incidentops.scenarios import ScenarioManifest, load_scenario_manifest
from incidentops.validation.checks import delete_run_logs_and_verify
from incidentops.validation.models import ScenarioMetadata

NEUTRAL_INCIDENT_DESCRIPTION = (
    "Orders appear to be falling behind. Investigate the most likely cause."
)
SCENARIO_IDS = (
    "slow_consumer_v1",
    "database_latency_v1",
    "traffic_spike_v1",
    "malformed_events_v1",
)
ModelProviderName = Literal["deterministic-test", "openai-compatible"]


def _settings_for_run(
    base: Settings,
    *,
    provider: ModelProviderName,
    knowledge_mode: KnowledgeMode,
) -> Settings:
    return base.model_copy(
        update={
            "llm_provider": provider,
            "knowledge_enabled": knowledge_mode == "required",
            "knowledge_required": knowledge_mode == "required",
        }
    )


def build_benchmark_incident_request(metadata: ScenarioMetadata) -> IncidentRequest:
    """Reduce retained run metadata to the four graph-visible operational inputs."""

    return IncidentRequest(
        description=NEUTRAL_INCIDENT_DESCRIPTION,
        start_time=metadata.start_time,
        end_time=metadata.end_time,
        run_id=metadata.run_id,
        affected_services=[ServiceName(metadata.affected_service)],
    )


def investigate_observations(
    metadata: ScenarioMetadata,
    settings: Settings,
) -> IncidentReport:
    """Run LangGraph with neutral metadata only; no manifest enters this boundary."""

    model_provider = create_model_provider(settings)
    toolset = InvestigationToolset.from_settings(settings)
    knowledge_service: KnowledgeSearchService | None = None
    try:
        if settings.knowledge_enabled:
            knowledge_service = KnowledgeSearchService(
                Elasticsearch(
                    settings.elasticsearch_url,
                    request_timeout=10,
                    retry_on_timeout=True,
                    max_retries=2,
                ),
                create_embedding_provider(
                    settings.embedding_provider,
                    settings.embedding_model,
                    settings.embedding_device,
                ),
                owns_client=True,
            )
        graph = build_configured_investigation_graph(
            settings,
            model_provider,
            toolset,
            knowledge_retriever=knowledge_service,
        )
        state = graph.invoke({"incident_request": build_benchmark_incident_request(metadata)})
    finally:
        if knowledge_service is not None:
            knowledge_service.close()
        toolset.close()
    report = state.get("final_report")
    if report is None:
        raise RuntimeError("benchmark investigation completed without a report")
    persist_investigation_artifacts(
        report,
        state.get("trace_events", []),
        settings.investigation_artifact_directory,
    )
    return report


def _knowledge_recall(report: IncidentReport, manifest: ScenarioManifest) -> float:
    expected = set(manifest.expected_knowledge_documents)
    if not expected:
        return 1.0
    retrieved = {item.document_id for item in report.knowledge_references}
    return len(expected & retrieved) / len(expected)


def _case_result(
    report: IncidentReport,
    manifest: ScenarioManifest,
    mode: KnowledgeMode,
) -> BenchmarkCaseResult:
    evaluation = evaluate_incident_report(report, manifest)
    diagnosed = (
        report.primary_root_cause.cause_code
        if report.primary_root_cause is not None
        else RootCauseCode.INSUFFICIENT_EVIDENCE
    )
    positive_expectation_count = len(manifest.expected_metrics) + len(manifest.expected_logs)
    evidence_recall = (
        evaluation.expected_metric_evidence_recall * len(manifest.expected_metrics)
        + evaluation.expected_log_evidence_recall * len(manifest.expected_logs)
    ) / positive_expectation_count
    return BenchmarkCaseResult(
        scenario_id=manifest.id,
        knowledge_mode=mode,
        expected_root_cause=RootCauseCode(manifest.root_cause.code),
        diagnosed_root_cause=diagnosed,
        root_cause_exact_match=evaluation.root_cause_exact_match,
        root_cause_rank=evaluation.root_cause_rank,
        evidence_recall=evidence_recall,
        negative_evidence_recall=evaluation.negative_evidence_recall,
        knowledge_recall_at_k=_knowledge_recall(report, manifest),
        unsupported_evidence_reference_count=(evaluation.unsupported_evidence_reference_count),
        unknown_knowledge_reference_count=(evaluation.unsupported_knowledge_reference_count),
        forbidden_action_count=evaluation.forbidden_action_count,
        insufficient_evidence=diagnosed == RootCauseCode.INSUFFICIENT_EVIDENCE,
        tool_calls=report.tool_call_count,
        model_calls=report.model_call_count,
        workflow_duration_seconds=evaluation.workflow_duration_seconds,
        retrieved_document_ids=sorted({item.document_id for item in report.knowledge_references}),
    )


def _selected_modes(mode: Literal["disabled", "required", "compare"]) -> tuple[KnowledgeMode, ...]:
    if mode == "compare":
        return ("disabled", "required")
    return (mode,)


def run_benchmark(
    settings: Settings,
    *,
    model_provider: ModelProviderName = "deterministic-test",
    knowledge_mode: Literal["disabled", "required", "compare"] = "compare",
) -> MultiIncidentBenchmarkResult:
    """Inject each incident once and investigate its retained window in selected modes."""

    modes = _selected_modes(knowledge_mode)
    results: dict[KnowledgeMode, list[BenchmarkCaseResult]] = {mode: [] for mode in modes}
    for scenario_id in SCENARIO_IDS:
        manifest = load_scenario_manifest(scenario_path(scenario_id))
        metadata = run_scenario(manifest, settings, retain_evidence=True)
        try:
            for mode in modes:
                run_settings = _settings_for_run(
                    settings,
                    provider=model_provider,
                    knowledge_mode=mode,
                )
                report = investigate_observations(metadata, run_settings)
                results[mode].append(_case_result(report, manifest, mode))
        finally:
            delete_run_logs_and_verify(
                metadata.run_id,
                elasticsearch_url=settings.elasticsearch_url,
            )
    aggregates = [aggregate_cases(results[mode]) for mode in modes]
    comparison = compare_aggregates(aggregates[0], aggregates[1]) if len(aggregates) == 2 else None
    return MultiIncidentBenchmarkResult.now(
        model_provider=model_provider,
        aggregates=aggregates,
        rag_comparison=comparison,
    )


def write_benchmark(path: Path, result: MultiIncidentBenchmarkResult) -> None:
    """Write the aggregate benchmark atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(result.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
