"""Unit coverage for benchmark macro aggregation and confusion matrices."""

from incidentops.benchmark.aggregation import aggregate_cases, compare_aggregates
from incidentops.benchmark.models import BenchmarkCaseResult
from incidentops.investigation.models import RootCauseCode


def _case(
    scenario_id: str,
    expected: RootCauseCode,
    diagnosed: RootCauseCode,
    *,
    mode: str = "disabled",
) -> BenchmarkCaseResult:
    return BenchmarkCaseResult.model_validate(
        {
            "scenario_id": scenario_id,
            "knowledge_mode": mode,
            "expected_root_cause": expected,
            "diagnosed_root_cause": diagnosed,
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
