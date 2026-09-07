"""Sequential multi-incident execution with same-window RAG comparison."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast

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
    EvidenceAvailability,
    EvidenceBundle,
    IncidentReport,
    IncidentRequest,
    ModelProposalArtifact,
    RootCauseCode,
    ServiceName,
)
from incidentops.investigation.report import (
    build_evidence_bundle,
    build_model_proposal_artifact,
    persist_investigation_artifacts,
)
from incidentops.investigation.state import InvestigationState
from incidentops.investigation.tools import InvestigationToolset
from incidentops.knowledge.embeddings import EmbeddingProvider, create_embedding_provider
from incidentops.knowledge.retrieval import KnowledgeSearchService
from incidentops.scenario_runner.runtime import run_scenario, scenario_path
from incidentops.scenarios import ScenarioManifest, load_scenario_manifest
from incidentops.validation.checks import delete_run_logs_and_verify, wait_for_run_logs_to_settle
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


@dataclass(frozen=True, slots=True)
class InvestigationExecution:
    """Report plus the pre-verification inputs needed for honest benchmark metrics."""

    report: IncidentReport
    evidence_bundle: EvidenceBundle
    model_proposal: ModelProposalArtifact


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


def execute_observations(
    metadata: ScenarioMetadata,
    settings: Settings,
    *,
    embedding_provider: EmbeddingProvider | None = None,
) -> InvestigationExecution:
    """Run LangGraph with neutral metadata only; no manifest enters this boundary."""

    model_provider = create_model_provider(settings)
    toolset = InvestigationToolset.from_settings(settings)
    knowledge_service: KnowledgeSearchService | None = None
    try:
        if settings.knowledge_enabled:
            if embedding_provider is None:
                embedding_provider = create_embedding_provider(
                    settings.embedding_provider,
                    settings.embedding_model,
                    settings.embedding_device,
                )
            knowledge_service = KnowledgeSearchService(
                Elasticsearch(
                    settings.elasticsearch_url,
                    request_timeout=10,
                    retry_on_timeout=True,
                    max_retries=2,
                ),
                embedding_provider,
                owns_client=True,
            )
        graph = build_configured_investigation_graph(
            settings,
            model_provider,
            toolset,
            knowledge_retriever=knowledge_service,
        )
        state = cast(
            InvestigationState,
            graph.invoke({"incident_request": build_benchmark_incident_request(metadata)}),
        )
    finally:
        if knowledge_service is not None:
            knowledge_service.close()
        toolset.close()
    report = state.get("final_report")
    if report is None:
        raise RuntimeError("benchmark investigation completed without a report")
    if settings.llm_provider == "deterministic-test":
        state["model_call_count"] = 0
        report = report.model_copy(update={"model_call_count": 0})
    evidence_bundle = build_evidence_bundle(state)
    model_proposal = build_model_proposal_artifact(
        state,
        evidence_bundle,
        model_provider=settings.llm_provider,
        knowledge_mode="required" if settings.knowledge_enabled else "disabled",
    )
    persist_investigation_artifacts(
        report,
        state.get("trace_events", []),
        settings.investigation_artifact_directory,
        evidence_bundle=evidence_bundle,
        model_proposal=model_proposal,
    )
    return InvestigationExecution(
        report=report,
        evidence_bundle=evidence_bundle,
        model_proposal=model_proposal,
    )


def investigate_observations(
    metadata: ScenarioMetadata,
    settings: Settings,
    *,
    embedding_provider: EmbeddingProvider | None = None,
) -> IncidentReport:
    """Compatibility wrapper returning only the final verified report."""

    return execute_observations(
        metadata,
        settings,
        embedding_provider=embedding_provider,
    ).report


def _knowledge_recall(report: IncidentReport, manifest: ScenarioManifest) -> float:
    expected = set(manifest.expected_knowledge_documents)
    if not expected:
        return 1.0
    retrieved = {item.document_id for item in report.knowledge_references}
    return len(expected & retrieved) / len(expected)


def build_case_result(
    execution: InvestigationExecution,
    manifest: ScenarioManifest,
    mode: KnowledgeMode,
) -> BenchmarkCaseResult:
    report = execution.report
    proposal = execution.model_proposal
    bundle = execution.evidence_bundle
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
    proposed = proposal.hypotheses[0].cause_code if proposal.hypotheses else None
    all_evidence = [
        *bundle.metric_evidence,
        *bundle.log_evidence,
        *bundle.negative_evidence,
    ]
    all_evidence_ids = {item.evidence_id for item in all_evidence}
    available_evidence_ids = {
        item.evidence_id
        for item in all_evidence
        if item.availability == EvidenceAvailability.AVAILABLE
    }
    proposal_references = {
        reference
        for hypothesis in proposal.hypotheses
        for reference in (
            *hypothesis.supporting_evidence_ids,
            *hypothesis.contradicting_evidence_ids,
        )
    }
    primary_references = (
        {
            *proposal.hypotheses[0].supporting_evidence_ids,
            *proposal.hypotheses[0].contradicting_evidence_ids,
        }
        if proposal.hypotheses
        else set()
    )
    citation_coverage = (
        len(primary_references & available_evidence_ids) / len(available_evidence_ids)
        if available_evidence_ids
        else 1.0
    )
    approach = (
        "rules"
        if proposal.model_provider == "deterministic-test"
        else ("llm-rag" if mode == "required" else "llm")
    )
    return BenchmarkCaseResult(
        scenario_id=manifest.id,
        knowledge_mode=mode,
        approach=approach,
        expected_root_cause=RootCauseCode(manifest.root_cause.code),
        diagnosed_root_cause=diagnosed,
        proposed_root_cause=proposed,
        model_invoked=proposal.model_invoked,
        provider_available=proposal.provider_available,
        structured_response_valid=proposal.structured_response_valid,
        proposal_exact_match=(
            proposed == RootCauseCode(manifest.root_cause.code) if proposed is not None else None
        ),
        verifier_accepted=report.status.value == "diagnosed",
        citation_coverage=citation_coverage,
        proposal_unsupported_evidence_reference_count=len(proposal_references - all_evidence_ids),
        investigation_status=report.status,
        verification_issues=report.verification_issues,
        model_errors=[
            item
            for item in report.limitations
            if item.split(":", maxsplit=1)[0]
            in {
                "quota_exceeded",
                "timeout",
                "request_rejected",
                "invalid_structured_response",
                "request_failed",
                "call_limit",
            }
        ][:4],
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
        model_calls=proposal.model_call_count,
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
    embedding_provider = (
        create_embedding_provider(
            settings.embedding_provider,
            settings.embedding_model,
            settings.embedding_device,
        )
        if "required" in modes
        else None
    )
    results: dict[KnowledgeMode, list[BenchmarkCaseResult]] = {mode: [] for mode in modes}
    for scenario_id in SCENARIO_IDS:
        manifest = load_scenario_manifest(scenario_path(scenario_id))
        metadata = run_scenario(manifest, settings, retain_evidence=True)
        try:
            wait_for_run_logs_to_settle(
                metadata.run_id,
                elasticsearch_url=settings.elasticsearch_url,
            )
            for mode in modes:
                run_settings = _settings_for_run(
                    settings,
                    provider=model_provider,
                    knowledge_mode=mode,
                )
                execution = execute_observations(
                    metadata,
                    run_settings,
                    embedding_provider=embedding_provider if mode == "required" else None,
                )
                results[mode].append(build_case_result(execution, manifest, mode))
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
