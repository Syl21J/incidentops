"""Unit coverage for benchmark aggregation and embedding model lifetime."""

import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from incidentops.benchmark import runner
from incidentops.benchmark.aggregation import aggregate_cases, compare_aggregates
from incidentops.benchmark.cli import render_benchmark_summary
from incidentops.benchmark.models import BenchmarkCaseResult, MultiIncidentBenchmarkResult
from incidentops.config import Settings
from incidentops.investigation.models import IncidentRequest, IncidentStatus, RootCauseCode
from incidentops.investigation.report import assemble_incident_report
from incidentops.knowledge.embeddings import SentenceTransformerEmbeddingProvider
from incidentops.knowledge.retrieval import KnowledgeSearchService


def _case(
    scenario_id: str,
    expected: RootCauseCode,
    diagnosed: RootCauseCode,
    *,
    mode: str = "disabled",
    status: IncidentStatus | None = None,
    verification_issues: list[str] | None = None,
    model_errors: list[str] | None = None,
) -> BenchmarkCaseResult:
    return BenchmarkCaseResult.model_validate(
        {
            "scenario_id": scenario_id,
            "knowledge_mode": mode,
            "expected_root_cause": expected,
            "diagnosed_root_cause": diagnosed,
            "investigation_status": status,
            "verification_issues": verification_issues or [],
            "model_errors": model_errors or [],
            "root_cause_exact_match": expected == diagnosed,
            "root_cause_rank": 1 if expected == diagnosed else None,
            "evidence_recall": 1.0,
            "negative_evidence_recall": 1.0,
            "knowledge_recall_at_k": 0.0 if mode == "disabled" else 1.0,
            "unsupported_evidence_reference_count": 0,
            "unknown_knowledge_reference_count": 0,
            "forbidden_action_count": 0,
            "insufficient_evidence": diagnosed == RootCauseCode.INSUFFICIENT_EVIDENCE,
            "tool_calls": 6,
            "model_calls": 2,
            "workflow_duration_seconds": 1.0,
        }
    )


def test_aggregation_reports_macro_metrics_and_true_by_predicted_confusion() -> None:
    cases = [
        _case(
            "slow_consumer_v1",
            RootCauseCode.SLOW_CONSUMER_PROCESSING,
            RootCauseCode.SLOW_CONSUMER_PROCESSING,
        ),
        _case(
            "database_latency_v1",
            RootCauseCode.DATABASE_LATENCY,
            RootCauseCode.TRAFFIC_SPIKE,
        ),
    ]

    result = aggregate_cases(cases)

    assert result.root_cause_accuracy == 0.5
    assert result.mean_root_cause_rank == 2.5
    assert result.confusion_matrix == {
        "slow_consumer_processing": {"slow_consumer_processing": 1},
        "database_latency": {"traffic_spike": 1},
    }


def test_rag_comparison_reports_required_minus_disabled_deltas() -> None:
    disabled = aggregate_cases(
        [
            _case(
                "slow_consumer_v1",
                RootCauseCode.SLOW_CONSUMER_PROCESSING,
                RootCauseCode.SLOW_CONSUMER_PROCESSING,
            )
        ]
    )
    required = aggregate_cases(
        [
            _case(
                "slow_consumer_v1",
                RootCauseCode.SLOW_CONSUMER_PROCESSING,
                RootCauseCode.SLOW_CONSUMER_PROCESSING,
                mode="required",
            )
        ]
    )

    comparison = compare_aggregates(disabled, required)

    assert comparison.root_cause_accuracy_delta == 0
    assert comparison.macro_knowledge_recall_at_k_delta == 1


def test_aggregation_separates_provider_contract_proposal_and_verifier_rates() -> None:
    accepted = _case(
        "slow_consumer_v1",
        RootCauseCode.SLOW_CONSUMER_PROCESSING,
        RootCauseCode.SLOW_CONSUMER_PROCESSING,
    ).model_copy(
        update={
            "approach": "llm",
            "model_invoked": True,
            "provider_available": True,
            "structured_response_valid": True,
            "proposed_root_cause": RootCauseCode.SLOW_CONSUMER_PROCESSING,
            "proposal_exact_match": True,
            "verifier_accepted": True,
            "citation_coverage": 0.75,
            "proposal_unsupported_evidence_reference_count": 1,
        }
    )
    unavailable = _case(
        "database_latency_v1",
        RootCauseCode.DATABASE_LATENCY,
        RootCauseCode.INSUFFICIENT_EVIDENCE,
    ).model_copy(
        update={
            "approach": "llm",
            "model_invoked": True,
            "provider_available": False,
            "structured_response_valid": False,
            "verifier_accepted": False,
            "citation_coverage": 0.0,
            "proposal_unsupported_evidence_reference_count": 0,
        }
    )

    result = aggregate_cases([accepted, unavailable])

    assert result.provider_availability_rate == 0.5
    assert result.structured_response_valid_rate == 1.0
    assert result.proposal_root_cause_accuracy == 1.0
    assert result.verifier_acceptance_rate == 1.0
    assert result.macro_citation_coverage == 0.75
    assert result.proposal_unsupported_evidence_reference_count == 1


def test_rules_aggregate_keeps_quality_metrics_without_provider_metrics() -> None:
    rules_case = _case(
        "slow_consumer_v1",
        RootCauseCode.SLOW_CONSUMER_PROCESSING,
        RootCauseCode.SLOW_CONSUMER_PROCESSING,
    ).model_copy(
        update={
            "approach": "rules",
            "model_invoked": False,
            "provider_available": None,
            "structured_response_valid": True,
            "proposed_root_cause": RootCauseCode.SLOW_CONSUMER_PROCESSING,
            "proposal_exact_match": True,
            "verifier_accepted": True,
            "citation_coverage": 1.0,
            "proposal_unsupported_evidence_reference_count": 0,
            "model_calls": 0,
        }
    )

    result = aggregate_cases([rules_case])

    assert result.provider_availability_rate is None
    assert result.structured_response_valid_rate is None
    assert result.proposal_root_cause_accuracy == 1.0
    assert result.verifier_acceptance_rate == 1.0
    assert result.macro_citation_coverage == 1.0


def test_terminal_summary_shows_case_outcomes_rejections_and_model_errors() -> None:
    aggregate = aggregate_cases(
        [
            _case(
                "database_latency_v1",
                RootCauseCode.DATABASE_LATENCY,
                RootCauseCode.INSUFFICIENT_EVIDENCE,
                status=IncidentStatus.PIPELINE_ERROR,
                verification_issues=["database latency evidence was incompatible"],
                model_errors=["timeout: live structured model request timed out"],
            )
        ]
    )
    result = MultiIncidentBenchmarkResult.now(
        model_provider="openai-compatible",
        aggregates=[aggregate],
        rag_comparison=None,
    )

    summary = render_benchmark_summary(result)

    assert "Benchmark cases" in summary
    assert "database_latency_v1" in summary
    assert "pipeline_error" in summary
    assert "insufficient_evidence" in summary
    assert "Aggregate results" in summary
    assert "timeout: live structured model request timed out" in summary
    assert "database latency evidence was incompatible" in summary


def test_benchmark_loads_embeddings_once_per_run_with_separate_retrieval_services(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start = datetime(2026, 8, 1, 13, 0, tzinfo=UTC)
    metadata = SimpleNamespace(
        start_time=start,
        end_time=start + timedelta(minutes=5),
        run_id="incident-run-embedding-test",
        affected_service="order-consumer",
    )
    model = Mock()
    model.get_embedding_dimension.return_value = 384
    load_weights = Mock(return_value=model)
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        SimpleNamespace(SentenceTransformer=load_weights),
    )
    monkeypatch.setattr(runner, "run_scenario", Mock(return_value=metadata))
    monkeypatch.setattr(runner, "delete_run_logs_and_verify", Mock())
    wait_for_logs = Mock(return_value=1)
    monkeypatch.setattr(runner, "wait_for_run_logs_to_settle", wait_for_logs)
    monkeypatch.setattr(runner, "create_model_provider", Mock())
    monkeypatch.setattr(runner.InvestigationToolset, "from_settings", Mock())
    monkeypatch.setattr(runner, "Elasticsearch", Mock(side_effect=lambda *a, **kw: Mock()))
    monkeypatch.setattr(runner, "persist_investigation_artifacts", Mock())
    services: list[KnowledgeSearchService] = []

    def build_graph(
        settings: Settings,
        model_provider: object,
        toolset: object,
        *,
        knowledge_retriever: KnowledgeSearchService | None,
    ) -> Mock:
        if knowledge_retriever is not None:
            services.append(knowledge_retriever)
            provider = knowledge_retriever._embedding_provider
            assert isinstance(provider, SentenceTransformerEmbeddingProvider)
            provider._load_model()
        report = assemble_incident_report({})
        return Mock(
            invoke=Mock(
                return_value={
                    "investigation_id": "investigation-embedding-test",
                    "incident_request": IncidentRequest(
                        description="Orders are delayed.",
                        start_time=start,
                        end_time=start + timedelta(minutes=5),
                        run_id="incident-run-embedding-test",
                    ),
                    "start_time": start,
                    "end_time": start + timedelta(minutes=5),
                    "run_id": "incident-run-embedding-test",
                    "completed_tasks": [],
                    "metric_evidence": [],
                    "log_evidence": [],
                    "negative_evidence": [],
                    "knowledge_references": [],
                    "model_call_count": 0,
                    "tool_call_count": 0,
                    "investigation_attempts": 1,
                    "errors": [],
                    "final_report": report,
                }
            )
        )

    monkeypatch.setattr(runner, "build_configured_investigation_graph", build_graph)
    settings = Settings(embedding_provider="sentence-transformers")

    runner.run_benchmark(settings, knowledge_mode="disabled")
    assert not services
    load_weights.assert_not_called()

    for expected_loads in (1, 2):
        runner.run_benchmark(settings, knowledge_mode="compare")
        assert load_weights.call_count == expected_loads

    assert len(services) == 8
    assert len({id(service) for service in services}) == 8
    assert len({id(service._embedding_provider) for service in services}) == 2
    assert wait_for_logs.call_count == 12
    for service in services:
        assert isinstance(service._client, Mock)
        service._client.close.assert_called_once()
