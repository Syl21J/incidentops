"""Add bounded scenario-window annotations to the local Grafana dashboard."""

from __future__ import annotations

import argparse
import base64
import json
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from pydantic import ValidationError

from incidentops.config import Settings
from incidentops.validation.models import ScenarioMetadata

DASHBOARD_UID = "incidentops-pipeline"
DASHBOARD_SLUG = "incidentops-order-pipeline"
ANNOTATION_TAG = "incidentops-scenario"
MAX_RESPONSE_BYTES = 65_536


class GrafanaAnnotationError(RuntimeError):
    """Report a safe Grafana annotation failure without exposing credentials."""


@dataclass(frozen=True)
class ScenarioAnnotation:
    """The created Grafana annotation and the corresponding dashboard URL."""

    annotation_id: int
    dashboard_url: str


def _grafana_root(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise GrafanaAnnotationError(
            "GRAFANA_URL must be an HTTP(S) origin without credentials, a path, or a query"
        )
    return value.rstrip("/")


def _read_metadata(path: Path) -> ScenarioMetadata:
    return ScenarioMetadata.model_validate_json(path.read_text(encoding="utf-8"))


def add_scenario_annotation(
    metadata: ScenarioMetadata,
    settings: Settings,
) -> ScenarioAnnotation:
    """Create one dashboard-scoped annotation for an already completed scenario."""

    root = _grafana_root(settings.grafana_url)
    started_ms = int(metadata.start_time.timestamp() * 1_000)
    ended_ms = int(metadata.end_time.timestamp() * 1_000)
    payload = {
        "dashboardUID": DASHBOARD_UID,
        "time": started_ms,
        "timeEnd": ended_ms,
        "tags": [ANNOTATION_TAG, metadata.scenario_id],
        "text": f"Scenario {metadata.scenario_id} ({metadata.run_id})",
    }
    credentials = (
        f"{settings.grafana_admin_user}:"
        f"{settings.grafana_admin_password.get_secret_value()}"
    ).encode()
    authorization = base64.b64encode(credentials).decode("ascii")
    request = Request(  # noqa: S310 - the origin is validated above
        f"{root}/api/annotations",
        data=json.dumps(payload, separators=(",", ":")).encode(),
        headers={
            "Accept": "application/json",
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(  # noqa: S310 - the origin is validated above
            request,
            timeout=settings.grafana_timeout_seconds,
        ) as response:
            raw_response = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as error:
        if error.code in {401, 403}:
            detail = "check GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD"
        else:
            detail = f"HTTP {error.code}"
        raise GrafanaAnnotationError(f"Grafana rejected the annotation: {detail}") from error
    except (URLError, OSError, TimeoutError) as error:
        raise GrafanaAnnotationError(f"Grafana is unavailable at {root}") from error
    if len(raw_response) > MAX_RESPONSE_BYTES:
        raise GrafanaAnnotationError("Grafana returned an unexpectedly large response")
    try:
        response_payload = json.loads(raw_response)
        annotation_id = response_payload["id"]
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise GrafanaAnnotationError("Grafana returned an invalid annotation response") from error
    if not isinstance(annotation_id, int) or annotation_id <= 0:
        raise GrafanaAnnotationError("Grafana returned an invalid annotation identifier")

    query = urlencode({"from": started_ms - 30_000, "to": ended_ms + 15_000})
    dashboard_url = f"{root}/d/{DASHBOARD_UID}/{DASHBOARD_SLUG}?{query}"
    return ScenarioAnnotation(annotation_id=annotation_id, dashboard_url=dashboard_url)


def build_parser() -> argparse.ArgumentParser:
    """Build the small Grafana helper CLI."""

    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    annotation = subparsers.add_parser(
        "annotate-scenario",
        help="Annotate one validated scenario metadata window.",
    )
    annotation.add_argument("--metadata", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Create a scenario annotation and print its exact dashboard window."""

    arguments = build_parser().parse_args(argv)
    try:
        metadata = _read_metadata(arguments.metadata)
        annotation = add_scenario_annotation(metadata, Settings())
    except (GrafanaAnnotationError, OSError, ValidationError, ValueError) as error:
        print(f"[ERROR] {error}", file=sys.stderr)
        return 1
    print(f"[OK]   Grafana annotation created with id={annotation.annotation_id}")
    print(f"Dashboard window: {annotation.dashboard_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
