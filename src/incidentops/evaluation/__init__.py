"""Scenario-driven evaluation for bounded investigation reports."""

from incidentops.evaluation.evaluator import evaluate_incident_report
from incidentops.investigation.models import EvaluationResult

__all__ = ["EvaluationResult", "evaluate_incident_report"]
