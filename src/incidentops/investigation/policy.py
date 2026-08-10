"""Validated execution policy for the bounded investigation workflow."""

from dataclasses import dataclass

from incidentops.knowledge.models import RetrievalMode


@dataclass(frozen=True, slots=True)
class InvestigationPolicy:
    """Group workflow limits and optional knowledge-retrieval behavior."""

    max_time_range_hours: int = 6
    max_tool_calls: int = 10
    max_attempts: int = 2
    knowledge_enabled: bool = False
    knowledge_required: bool = False
    knowledge_mode: RetrievalMode = RetrievalMode.HYBRID
    knowledge_top_k: int = 5
    knowledge_candidate_k: int = 40

    def __post_init__(self) -> None:
        """Reject incoherent limits before graph construction."""

        if not 1 <= self.max_time_range_hours <= 6:
            raise ValueError("max_time_range_hours must be between one and six")
        if not 6 <= self.max_tool_calls <= 10:
            raise ValueError("max_tool_calls must be between six and ten")
        if not 1 <= self.max_attempts <= 2:
            raise ValueError("max_attempts must be between one and two")
        if self.knowledge_required and not self.knowledge_enabled:
            raise ValueError("required knowledge retrieval must be enabled")
        if not 1 <= self.knowledge_top_k <= 10:
            raise ValueError("knowledge_top_k must be between one and ten")
        if not self.knowledge_top_k <= self.knowledge_candidate_k <= 100:
            raise ValueError("knowledge_candidate_k must be between top_k and one hundred")
