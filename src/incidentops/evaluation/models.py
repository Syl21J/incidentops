"""Validated inputs for scenario-driven investigation evaluation."""

from pathlib import Path

from incidentops.investigation.models import EvaluationResult, StrictModel


class EvaluationRequest(StrictModel):
    """Paths accepted by the evaluation command-line boundary."""

    report_path: Path
    scenario_path: Path


__all__ = ["EvaluationRequest", "EvaluationResult"]
