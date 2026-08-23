"""Deterministic conversion of live observations into bounded knowledge queries."""

from __future__ import annotations

from typing import Protocol

from incidentops.investigation.models import (
    LogEvidenceType,
    MetricEvidenceType,
    NegativeEvidenceType,
)
from incidentops.investigation.state import InvestigationState
from incidentops.knowledge.models import (
    KnowledgeFilters,
    KnowledgeSearchRequest,
    KnowledgeSearchResult,
    KnowledgeService,
    RetrievalMode,
)
from incidentops.knowledge.retrieval import clean_query_text


class KnowledgeRetriever(Protocol):
    """Narrow retrieval surface injected into the graph."""

    def search(self, request: KnowledgeSearchRequest) -> KnowledgeSearchResult:
        """Return validated bounded knowledge references."""

        ...


def build_incident_knowledge_request(
    state: InvestigationState,
    *,
    mode: RetrievalMode,
    top_k: int,
    candidate_k: int,
) -> KnowledgeSearchRequest:
    """Use categorical live summaries only, excluding descriptions and raw log timelines."""

    terms: list[str] = []
    for item in state.get("metric_evidence", []):
        if item.metric_type == MetricEvidenceType.CONSUMER_LAG:
            terms.extend(["kafka", "consumer", "lag"])
            trend = item.raw_value_summary.get("trend")
            if trend in {"increasing", "stable", "decreasing"}:
                terms.append(str(trend))
        elif item.metric_type == MetricEvidenceType.PROCESSING_LATENCY:
            if item.raw_value_summary.get("processing_state") == "elevated":
                terms.extend(["order", "slow", "processing", "latency"])
            else:
                terms.extend(["normal", "order", "processing", "latency"])
            if item.raw_value_summary.get("database_state") == "elevated":
                terms.extend(["postgres", "database", "operation", "latency"])
            else:
                terms.extend(["normal", "database", "latency"])
        elif item.metric_type == MetricEvidenceType.PRODUCER_CONSUMER_RATES:
            terms.extend(["producer", "consumer", "rate"])
            if item.raw_value_summary.get("producer_surge") is True:
                terms.extend(["traffic", "spike", "surge", "backlog"])
            elif item.raw_value_summary.get("processing_errors_present") is True:
                terms.extend(["malformed", "invalid", "event", "validation"])
            elif item.raw_value_summary.get("consumer_is_slower") is True:
                terms.append("imbalance")
    for item in state.get("log_evidence", []):
        if item.log_type == LogEvidenceType.SLOW_PROCESSING:
            if item.raw_value_summary.get("slow_processing_count", 0):
                terms.append("slow processing")
            if item.raw_value_summary.get("database_operation_slow_count", 0):
                terms.append("database operation slow")
            if item.raw_value_summary.get("invalid_event_count", 0):
                terms.append("invalid malformed event")
        else:
            mapping = {
                LogEvidenceType.DATABASE_ERRORS: "database errors",
                LogEvidenceType.KAFKA_ERRORS: "kafka broker errors",
            }
            terms.append(mapping[item.log_type])
    for item in state.get("negative_evidence", []):
        mapping = {
            NegativeEvidenceType.NO_DATABASE_ERRORS: "no database errors",
            NegativeEvidenceType.NO_KAFKA_BROKER_ERRORS: "no kafka broker errors",
        }
        terms.append(mapping[item.negative_type])
    services = []
    for service in state.get("affected_services", []):
        services.append(KnowledgeService(service.value))
        terms.append(service.value.replace("-", " "))
    if not terms:
        terms.extend(["operational", "incident"])
    query = clean_query_text(" ".join(dict.fromkeys(terms)))
    return KnowledgeSearchRequest(
        query=query,
        mode=mode,
        filters=KnowledgeFilters(services=services, statuses=["active"]),
        top_k=top_k,
        candidate_k=candidate_k,
    )
