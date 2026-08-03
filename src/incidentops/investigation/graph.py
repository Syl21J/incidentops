"""LangGraph construction for the bounded single-workflow investigation."""

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from incidentops.config import Settings
from incidentops.investigation.model import StructuredModelProvider
from incidentops.investigation.nodes import (
    InvestigationNodes,
    route_after_plan,
    route_after_validation,
)
from incidentops.investigation.state import InvestigationState
from incidentops.investigation.tools import InvestigationToolset


def build_investigation_graph(
    nodes: InvestigationNodes,
) -> CompiledStateGraph[InvestigationState, None, InvestigationState, InvestigationState]:
    """Compile a fan-out/fan-in graph with one statically bounded recheck loop."""

    builder = StateGraph(InvestigationState)
    builder.add_node("validate_incident", nodes.validate_incident)
    builder.add_node("plan_investigation", nodes.plan_investigation)
    builder.add_node("collect_metrics", nodes.collect_metrics)
    builder.add_node("collect_logs", nodes.collect_logs)
    builder.add_node("generate_hypotheses", nodes.generate_hypotheses)
    builder.add_node("verify_hypotheses", nodes.verify_hypotheses)
    builder.add_node("targeted_recheck", nodes.targeted_recheck)
    builder.add_node("generate_report", nodes.generate_report)

    builder.add_edge(START, "validate_incident")
    builder.add_conditional_edges(
        "validate_incident",
        route_after_validation,
        ["plan_investigation", "generate_report"],
    )
    builder.add_conditional_edges(
        "plan_investigation",
        route_after_plan,
        ["collect_metrics", "collect_logs", "generate_report"],
    )
    builder.add_edge("collect_metrics", "generate_hypotheses")
    builder.add_edge("collect_logs", "generate_hypotheses")
    builder.add_edge("generate_hypotheses", "verify_hypotheses")
    builder.add_conditional_edges(
        "verify_hypotheses",
        nodes.route_after_verification,
        ["targeted_recheck", "generate_report"],
    )
    builder.add_edge("targeted_recheck", "generate_hypotheses")
    builder.add_edge("generate_report", END)
    return builder.compile()


def build_configured_investigation_graph(
    settings: Settings,
    model_provider: StructuredModelProvider,
    toolset: InvestigationToolset,
) -> CompiledStateGraph[InvestigationState, None, InvestigationState, InvestigationState]:
    """Apply validated environment limits while keeping providers replaceable in tests."""

    return build_investigation_graph(
        InvestigationNodes(
            model_provider,
            toolset,
            max_time_range_hours=settings.investigation_max_time_range_hours,
            max_tool_calls=settings.investigation_max_tool_calls,
            max_attempts=settings.investigation_max_attempts,
        )
    )
