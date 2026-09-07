"""Deterministic benchmark metrics and confusion-matrix aggregation."""

from __future__ import annotations

from collections.abc import Sequence

from incidentops.benchmark.models import (
    BenchmarkAggregate,
    BenchmarkCaseResult,
    RagComparison,
)


def _mean(values: Sequence[float | int]) -> float:
    if not values:
        raise ValueError("cannot aggregate an empty benchmark")
    return sum(values) / len(values)


def _observed_rate(numerator: int, denominator: int) -> float | None:
    """Return no rate when the measured stage was never reached."""

    return numerator / denominator if denominator else None


def aggregate_cases(cases: list[BenchmarkCaseResult]) -> BenchmarkAggregate:
    """Aggregate one knowledge mode without weighting scenarios by expectation count."""

    if not cases:
        raise ValueError("benchmark cases must not be empty")
    modes = {item.knowledge_mode for item in cases}
    if len(modes) != 1:
        raise ValueError("benchmark aggregation requires one knowledge mode")
    approaches = {item.approach for item in cases}
    if len(approaches) != 1:
        raise ValueError("benchmark aggregation requires one diagnostic approach")
    confusion: dict[str, dict[str, int]] = {}
    for item in cases:
        expected = item.expected_root_cause.value
        diagnosed = item.diagnosed_root_cause.value
        confusion.setdefault(expected, {})[diagnosed] = (
            confusion.setdefault(expected, {}).get(diagnosed, 0) + 1
        )
    invoked = [item for item in cases if item.model_invoked is True]
    available = [item for item in invoked if item.provider_available is True]
    is_rules_reference = cases[0].approach == "rules"
    structured = [
        item
        for item in (cases if is_rules_reference else available)
        if item.structured_response_valid is True
    ]
    return BenchmarkAggregate(
        knowledge_mode=cases[0].knowledge_mode,
        approach=cases[0].approach,
        scenario_count=len(cases),
        provider_availability_rate=(
            None if is_rules_reference else _observed_rate(len(available), len(invoked))
        ),
        structured_response_valid_rate=(
            None
            if is_rules_reference
            else _observed_rate(len(structured), len(available))
        ),
        proposal_root_cause_accuracy=_observed_rate(
            sum(item.proposal_exact_match is True for item in structured),
            len(structured),
        ),
        verifier_acceptance_rate=_observed_rate(
            sum(item.verifier_accepted is True for item in structured),
            len(structured),
        ),
        macro_citation_coverage=(
            _mean([item.citation_coverage or 0.0 for item in structured])
            if structured
            else None
        ),
        proposal_unsupported_evidence_reference_count=sum(
            item.proposal_unsupported_evidence_reference_count or 0 for item in cases
        ),
        root_cause_accuracy=_mean([int(item.root_cause_exact_match) for item in cases]),
        mean_root_cause_rank=_mean(
            [item.root_cause_rank if item.root_cause_rank is not None else 4 for item in cases]
        ),
        macro_evidence_recall=_mean([item.evidence_recall for item in cases]),
        macro_negative_evidence_recall=_mean([item.negative_evidence_recall for item in cases]),
        macro_knowledge_recall_at_k=_mean([item.knowledge_recall_at_k for item in cases]),
        unsupported_evidence_reference_count=sum(
            item.unsupported_evidence_reference_count for item in cases
        ),
        unknown_knowledge_reference_count=sum(
            item.unknown_knowledge_reference_count for item in cases
        ),
        forbidden_action_count=sum(item.forbidden_action_count for item in cases),
        insufficient_evidence_rate=_mean([int(item.insufficient_evidence) for item in cases]),
        average_tool_calls=_mean([item.tool_calls for item in cases]),
        average_model_calls=_mean([item.model_calls for item in cases]),
        average_workflow_duration_seconds=_mean([item.workflow_duration_seconds for item in cases]),
        confusion_matrix=confusion,
        cases=cases,
    )


def compare_aggregates(
    disabled: BenchmarkAggregate,
    required: BenchmarkAggregate,
) -> RagComparison:
    """Return honest required-RAG minus no-RAG deltas without significance claims."""

    if disabled.knowledge_mode != "disabled" or required.knowledge_mode != "required":
        raise ValueError("RAG comparison requires disabled and required aggregates")
    return RagComparison(
        root_cause_accuracy_delta=(required.root_cause_accuracy - disabled.root_cause_accuracy),
        macro_evidence_recall_delta=(
            required.macro_evidence_recall - disabled.macro_evidence_recall
        ),
        macro_knowledge_recall_at_k_delta=(
            required.macro_knowledge_recall_at_k - disabled.macro_knowledge_recall_at_k
        ),
        unsupported_evidence_reference_delta=(
            required.unsupported_evidence_reference_count
            - disabled.unsupported_evidence_reference_count
        ),
        forbidden_action_delta=(required.forbidden_action_count - disabled.forbidden_action_count),
        average_tool_calls_delta=required.average_tool_calls - disabled.average_tool_calls,
        average_model_calls_delta=required.average_model_calls - disabled.average_model_calls,
        average_duration_seconds_delta=(
            required.average_workflow_duration_seconds - disabled.average_workflow_duration_seconds
        ),
    )
