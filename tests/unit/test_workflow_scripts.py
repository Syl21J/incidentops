"""Exercise launcher safety and orchestration without Docker, telemetry, or model calls."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"

# Stand-ins record only command arguments and non-secret workflow settings. Production Python
# validation is covered separately; these checks exercise the actual Bash control flow.
FAKE_COMMAND = r"""
import json
import os
import sys
from pathlib import Path

command = Path(sys.argv[0]).name
args = sys.argv[1:]
if command == "uv" and args[:1] == ["run"]:
    args = args[1:]
    if args[:2] == ["python", "-m"]:
        args = args[2:]
    command, *args = args

record = {"command": command, "args": args, "cwd": os.getcwd()}
for key in ("KNOWLEDGE_ENABLED", "KNOWLEDGE_REQUIRED", "EMBEDDING_PROVIDER",
            "KNOWLEDGE_RETRIEVAL_MODE", "GRAFANA_URL"):
    record[key] = os.environ.get(key)
with Path(os.environ["SCRIPT_TEST_LOG"]).open("a", encoding="utf-8") as output:
    output.write(json.dumps(record) + "\n")

failure = os.environ.get("SCRIPT_TEST_FAILURE")
if failure and failure in [command, *args]:
    sys.exit(17)
if (os.environ.get("SCRIPT_TEST_PIPELINE_ERROR") == "true"
        and command == "incidentops.investigation.cli" and args[0] == "investigate"):
    report = Path(args[args.index("--output-file") + 1])
    report.write_text("{}", encoding="utf-8")
    sys.exit(1)

if command == "incidentops-scenario":
    metadata = Path(args[args.index("--output-metadata") + 1])
    metadata.write_text('{"run_id": "run-owned-by-test"}', encoding="utf-8")
elif command == "docker" and args == ["compose", "port", "grafana", "3000"]:
    print("127.0.0.1:3300")
elif command == "incidentops.grafana":
    print("[OK]   Grafana annotation created with id=123")
    print("Dashboard window: http://127.0.0.1:3300/d/incidentops-pipeline/window")
elif command == "incidentops.investigation.cli" and args[0] == "investigate":
    report = Path(args[args.index("--output-file") + 1])
    report.write_text("{}", encoding="utf-8")
elif command == "incidentops.validation.cli":
    if args[0] == "scenario-window":
        print("run-owned-by-test 2026-09-03T12:00:00Z 2026-09-03T12:01:00Z")
    elif args[0] == "metadata-run-id":
        print(json.loads(Path(args[1]).read_text(encoding="utf-8"))["run_id"])
    elif args[0] == "artifact-directory":
        print(Path.cwd() / "artifacts" / "investigations")
"""


@pytest.fixture
def launcher_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Copy launchers into an isolated project and replace every external operation."""

    project = tmp_path / "project with spaces"
    script_directory = project / "scripts"
    script_directory.mkdir(parents=True)
    for filename in ("run-benchmark.sh", "check-investigation.sh", "run-observability-demo.sh"):
        shutil.copy2(SCRIPTS / filename, script_directory / filename)
    shutil.copytree(SCRIPTS / "lib", script_directory / "lib")
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for command in ("uv", "docker", "curl", "wslview"):
        path = binaries / command
        path.write_text(f"#!{sys.executable}\n{FAKE_COMMAND}", encoding="utf-8")
        path.chmod(0o755)
    for filename in (
        "check-infrastructure.sh",
        "initialize-database.sh",
        "initialize-elasticsearch.sh",
    ):
        path = script_directory / filename
        path.write_text(f"#!{sys.executable}\n{FAKE_COMMAND}", encoding="utf-8")
        path.chmod(0o755)

    monkeypatch.setenv("PATH", f"{binaries}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("SCRIPT_TEST_LOG", str(project / "commands.jsonl"))
    monkeypatch.delenv("SCRIPT_TEST_FAILURE", raising=False)
    monkeypatch.delenv("SCRIPT_TEST_PIPELINE_ERROR", raising=False)
    monkeypatch.delenv("BENCHMARK_OUTPUT", raising=False)
    # Even a live/RAG developer environment must not change the deterministic modes.
    monkeypatch.setenv("LLM_PROVIDER", "openai-compatible")
    monkeypatch.setenv("KNOWLEDGE_ENABLED", "true")
    monkeypatch.setenv("KNOWLEDGE_REQUIRED", "true")
    return project


def launch(project: Path, script: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(project / "scripts" / script), *arguments],
        cwd=project.parent,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def calls(project: Path, command: str | None = None) -> list[dict]:
    log_path = project / "commands.jsonl"
    records = (
        [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
        if log_path.exists()
        else []
    )
    return [item for item in records if command is None or item["command"] == command]


def test_live_benchmark_prepares_services_and_preserves_output_path(launcher_project: Path) -> None:
    result = launch(
        launcher_project, "run-benchmark.sh", "--live", "--output-file", "reports/live result.json"
    )
    assert result.returncode == 0, result.stderr
    records = calls(launcher_project)
    steps = [(item["command"], item["args"]) for item in records]
    preflight = steps.index(("incidentops.validation.cli", ["validate-live-model"]))
    config = steps.index(("docker", ["compose", "config", "--quiet"]))
    start = steps.index(("docker", ["compose", "up", "-d"]))
    health = steps.index(("check-infrastructure.sh", []))
    database = steps.index(("initialize-database.sh", []))
    elasticsearch = steps.index(("initialize-elasticsearch.sh", []))
    ingest = steps.index(
        ("incidentops-knowledge", ["ingest", "--knowledge-directory", "knowledge"])
    )
    assert preflight < config < start < health < database < elasticsearch < ingest
    benchmark = calls(launcher_project, "incidentops-benchmark")
    assert len(benchmark) == 1  # No deterministic acceptance gate for live measurements.
    assert benchmark[0]["args"] == [
        "run",
        "--all",
        "--model-provider",
        "openai-compatible",
        "--knowledge-mode",
        "compare",
        "--output-format",
        "summary",
        "--output-file",
        "reports/live result.json",
    ]
    assert records.index(benchmark[0]) > ingest
    assert benchmark[0]["EMBEDDING_PROVIDER"] == "sentence-transformers"
    assert benchmark[0]["KNOWLEDGE_RETRIEVAL_MODE"] == "hybrid"
    assert all(item["cwd"] == str(launcher_project) for item in records)
    assert "Benchmark result: reports/live result.json" in result.stdout
    assert (
        "http://127.0.0.1:3300/d/incidentops-pipeline/incidentops-order-pipeline" in result.stdout
    )


def test_default_benchmark_is_deterministic_and_keeps_acceptance_gate(
    launcher_project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BENCHMARK_OUTPUT", "reports/deterministic.json")
    monkeypatch.setenv("SCRIPT_TEST_FAILURE", "validate")
    result = launch(launcher_project, "run-benchmark.sh")
    assert result.returncode == 17
    benchmark = calls(launcher_project, "incidentops-benchmark")
    assert len(benchmark) == 2
    assert benchmark[0]["args"][3] == "deterministic-test"
    assert benchmark[1]["args"] == ["validate", "reports/deterministic.json"]
    assert not any("validate-live-model" in item["args"] for item in calls(launcher_project))


@pytest.mark.parametrize(
    "failure",
    [
        "--frozen",
        "validate-live-model",
        "config",
        "check-infrastructure.sh",
        "initialize-database.sh",
        "initialize-elasticsearch.sh",
        "ingest",
    ],
)
def test_benchmark_setup_failures_prevent_incident_and_model_execution(
    launcher_project: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setenv("SCRIPT_TEST_FAILURE", failure)
    result = launch(launcher_project, "run-benchmark.sh", "--live")
    assert result.returncode == 17
    assert not calls(launcher_project, "incidentops-benchmark")
    if failure in {"--frozen", "validate-live-model", "config"}:
        assert not any("up" in item["args"] for item in calls(launcher_project, "docker"))


def test_observability_demo_runs_one_scenario_and_annotates_its_window(
    launcher_project: Path,
) -> None:
    result = launch(
        launcher_project,
        "run-observability-demo.sh",
        "--scenario",
        "database_latency_v1",
        "--no-open",
    )

    assert result.returncode == 0, result.stderr
    scenarios = calls(launcher_project, "incidentops-scenario")
    assert len(scenarios) == 1
    assert scenarios[0]["args"][:8] == [
        "run",
        "--scenario",
        "database_latency_v1",
        "--start-delay-seconds",
        "6",
        "--minimum-incident-seconds",
        "18",
        "--output-metadata",
    ]
    annotations = calls(launcher_project, "incidentops.grafana")
    assert len(annotations) == 1
    assert annotations[0]["args"][:2] == ["annotate-scenario", "--metadata"]
    assert annotations[0]["GRAFANA_URL"] == "http://127.0.0.1:3300"
    assert not calls(launcher_project, "wslview")
    assert "Database latency" in result.stdout
    assert "database P95 rise together" in result.stdout
    assert "Normal phase: 6 seconds, then at least 18 seconds" in result.stdout
    assert "No language model was called" in result.stdout


def test_observability_demo_runs_all_scenarios_in_stable_order(
    launcher_project: Path,
) -> None:
    result = launch(
        launcher_project,
        "run-observability-demo.sh",
        "--all",
        "--no-open",
        "--no-pause",
    )

    assert result.returncode == 0, result.stderr
    scenarios = calls(launcher_project, "incidentops-scenario")
    assert [item["args"][2] for item in scenarios] == [
        "slow_consumer_v1",
        "database_latency_v1",
        "traffic_spike_v1",
        "malformed_events_v1",
    ]
    assert len(calls(launcher_project, "incidentops.grafana")) == 4


def test_observability_demo_random_mode_stays_inside_allowlist(
    launcher_project: Path,
) -> None:
    result = launch(launcher_project, "run-observability-demo.sh", "--random", "--no-open")

    assert result.returncode == 0, result.stderr
    scenarios = calls(launcher_project, "incidentops-scenario")
    assert len(scenarios) == 1
    assert scenarios[0]["args"][2] in {
        "slow_consumer_v1",
        "database_latency_v1",
        "traffic_spike_v1",
        "malformed_events_v1",
    }


def test_observability_demo_can_open_the_live_dashboard(
    launcher_project: Path,
) -> None:
    result = launch(
        launcher_project,
        "run-observability-demo.sh",
        "malformed_events_v1",
        "--open",
    )

    assert result.returncode == 0, result.stderr
    browser = calls(launcher_project, "wslview")
    assert len(browser) == 1
    assert browser[0]["args"] == [
        "http://127.0.0.1:3300/d/incidentops-pipeline/incidentops-order-pipeline"
        "?from=now-5m&to=now&timezone=browser&refresh=5s"
    ]


def test_failed_demo_scenario_is_not_annotated_and_removes_private_metadata(
    launcher_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRIPT_TEST_FAILURE", "incidentops-scenario")
    result = launch(
        launcher_project,
        "run-observability-demo.sh",
        "traffic_spike_v1",
        "--no-open",
    )

    assert result.returncode != 0
    assert not calls(launcher_project, "incidentops.grafana")
    metadata = Path(calls(launcher_project, "incidentops-scenario")[0]["args"][-1])
    assert not metadata.parent.exists()


@pytest.mark.parametrize(
    ("script", "arguments", "exit_code"),
    [
        ("run-benchmark.sh", ["--help"], 0),
        ("run-benchmark.sh", ["--typo"], 2),
        ("run-benchmark.sh", ["--output-file"], 2),
        ("run-benchmark.sh", ["--output-file", "--live"], 2),
        ("check-investigation.sh", ["--help"], 0),
        ("check-investigation.sh", ["typo"], 2),
        ("check-investigation.sh", ["live", "typo"], 2),
        ("run-observability-demo.sh", ["--help"], 0),
        ("run-observability-demo.sh", [], 2),
        ("run-observability-demo.sh", ["--scenario"], 2),
        ("run-observability-demo.sh", ["--scenario", "unknown_v1"], 2),
        ("run-observability-demo.sh", ["--random", "--all"], 2),
    ],
)
def test_help_and_invalid_arguments_have_no_external_effects(
    launcher_project: Path, script: str, arguments: list[str], exit_code: int
) -> None:
    result = launch(launcher_project, script, *arguments)
    assert result.returncode == exit_code
    assert not calls(launcher_project)


@pytest.mark.parametrize("mode", ["baseline", "rag", "live"])
def test_investigation_modes_use_one_window_and_clean_only_the_owned_run(
    launcher_project: Path, mode: str
) -> None:
    result = launch(launcher_project, "check-investigation.sh", mode)
    assert result.returncode == 0, result.stderr
    scenarios = calls(launcher_project, "incidentops-scenario")
    assert len(scenarios) == 1
    assert "--retain-evidence" in scenarios[0]["args"]
    investigation_cli_calls = calls(launcher_project, "incidentops.investigation.cli")
    investigations = [
        item
        for item in investigation_cli_calls
        if item["args"][0] == "investigate"
    ]
    assert len(investigations) == (2 if mode == "rag" else 1)
    for index, item in enumerate(investigations):
        args = item["args"]
        assert args[args.index("--start-time") + 1] == "2026-09-03T12:00:00Z"
        assert args[args.index("--end-time") + 1] == "2026-09-03T12:01:00Z"
        assert args[args.index("--run-id") + 1] == "run-owned-by-test"
        assert "--scenario" not in args
        assert "slow_consumer_v1" not in args
        assert args[args.index("--model-provider") + 1] == (
            "openai-compatible" if mode == "live" else "scripted-test"
        )
        assert "--persist-artifacts" in args
        knowledge_enabled = "true" if mode == "live" or index == 1 else "false"
        assert item["KNOWLEDGE_ENABLED"] == item["KNOWLEDGE_REQUIRED"] == knowledge_enabled
    summaries = [item for item in investigation_cli_calls if item["args"][0] == "summarize"]
    assert len(summaries) == 1
    validation = calls(launcher_project, "incidentops.validation.cli")
    expected_validator = {
        "baseline": "validate-agent-workflow",
        "rag": "validate-rag-workflow",
        "live": "validate-live-rag-workflow",
    }[mode]
    assert any(item["args"][0] == expected_validator for item in validation)
    assert any("validate-retrieval-benchmark" in item["args"] for item in validation) == (
        mode == "rag"
    )
    assert any("validate-live-model" in item["args"] for item in validation) == (mode == "live")
    assert validation[-1]["args"] == ["delete-run-logs", "run-owned-by-test"]
    assert not Path(scenarios[0]["args"][-1]).parent.exists()


@pytest.mark.parametrize(
    "failure",
    [
        "wait-run-logs-stable",
        "scenario-window",
        "incidentops.investigation.cli",
        "validate-rag-workflow",
        "delete-run-logs",
    ],
)
def test_failed_investigation_still_attempts_scoped_cleanup_and_returns_failure(
    launcher_project: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    monkeypatch.setenv("SCRIPT_TEST_FAILURE", failure)
    result = launch(launcher_project, "check-investigation.sh", "rag")
    assert result.returncode != 0
    validation = calls(launcher_project, "incidentops.validation.cli")
    assert validation[-1]["args"] == ["delete-run-logs", "run-owned-by-test"]
    metadata = calls(launcher_project, "incidentops-scenario")[0]["args"][-1]
    assert not Path(metadata).parent.exists()


def test_pipeline_error_report_is_summarized_before_the_launcher_fails(
    launcher_project: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SCRIPT_TEST_PIPELINE_ERROR", "true")

    result = launch(launcher_project, "check-investigation.sh", "live")

    assert result.returncode == 1
    investigation_calls = calls(launcher_project, "incidentops.investigation.cli")
    assert [item["args"][0] for item in investigation_calls] == ["investigate", "summarize"]
    validation = calls(launcher_project, "incidentops.validation.cli")
    assert validation[-1]["args"] == ["delete-run-logs", "run-owned-by-test"]
