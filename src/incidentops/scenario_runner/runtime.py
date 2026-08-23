"""Shared process lifecycle, evidence polling, and scoped cleanup for scenarios."""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import IO, Literal

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from incidentops.config import Settings
from incidentops.database import connect_database
from incidentops.investigation.models import (
    EvidenceAvailability,
    InvestigationTaskType,
    LogEvidence,
    MetricEvidence,
    NegativeEvidence,
)
from incidentops.investigation.tools import InvestigationToolInput, InvestigationToolset
from incidentops.scenarios import (
    DatabaseLatencyExecution,
    MalformedEventsExecution,
    ScenarioManifest,
    SlowConsumerExecution,
    TelemetryBehavior,
    TelemetrySignal,
    TrafficSpikeExecution,
)
from incidentops.validation.checks import delete_run_logs_and_verify
from incidentops.validation.models import ScenarioMetadata, SlowConsumerObservations

PROJECT_DIRECTORY = Path(__file__).resolve().parents[3]
SCENARIO_DIRECTORY = PROJECT_DIRECTORY / "scenarios"
SCENARIO_FILE_NAMES = {
    "slow_consumer_v1": "slow_consumer.yaml",
    "database_latency_v1": "database_latency.yaml",
    "traffic_spike_v1": "traffic_spike.yaml",
    "malformed_events_v1": "malformed_events.yaml",
}
DELETE_SCENARIO_ROWS_SQL = "DELETE FROM processed_orders WHERE customer_id LIKE %s"


class ScenarioRunError(RuntimeError):
    """Report a bounded scenario orchestration or acceptance failure."""


@dataclass
class ManagedProcess:
    """One child process and its isolated captured output."""

    name: str
    process: subprocess.Popen[bytes]
    output_path: Path
    output_stream: IO[bytes]

    def output(self) -> str:
        """Return current bounded diagnostic output."""

        self.output_stream.flush()
        return self.output_path.read_text(encoding="utf-8", errors="replace")[-20_000:]

    def stop(self) -> None:
        """Stop one owned process with a fixed graceful timeout."""

        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired as error:
                raise ScenarioRunError(
                    f"{self.name} did not stop within fifteen seconds"
                ) from error

    def close(self) -> None:
        """Close the owned output stream after process termination."""

        self.output_stream.close()


def scenario_path(scenario_id: str) -> Path:
    """Resolve only a closed scenario identifier to its tracked manifest."""

    try:
        return SCENARIO_DIRECTORY / SCENARIO_FILE_NAMES[scenario_id]
    except KeyError as error:
        raise ValueError(f"unsupported scenario: {scenario_id}") from error


def _resource_prefix(manifest: ScenarioManifest) -> str:
    return manifest.execution.kind.value.replace("_", "-")


def _start_process(
    name: str, arguments: list[str], output_path: Path, env: dict[str, str]
) -> ManagedProcess:
    output_stream = output_path.open("wb")
    try:
        process = subprocess.Popen(
            arguments, stdout=output_stream, stderr=subprocess.STDOUT, env=env
        )
    except Exception:
        output_stream.close()
        raise
    return ManagedProcess(name, process, output_path, output_stream)


def _wait_for_output(process: ManagedProcess, text: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while text not in process.output():
        return_code = process.process.poll()
        if return_code is not None:
            raise ScenarioRunError(
                f"{process.name} exited with {return_code} before {text!r}:\n{process.output()}"
            )
        if time.monotonic() >= deadline:
            raise ScenarioRunError(f"timed out waiting for {text!r} from {process.name}")
        time.sleep(0.5)


def _producer_arguments(
    manifest: ScenarioManifest,
    settings: Settings,
    run_id: str,
    topic: str,
) -> list[str]:
    execution = manifest.execution
    arguments = [
        sys.executable,
        "-m",
        "incidentops.producer",
        "--run-id",
        run_id,
        "--topic",
        topic,
        "--seed",
        str(execution.seed),
        "--metrics-port",
        str(settings.producer_metrics_port),
        "--metrics-grace-seconds",
        "8",
    ]
    if isinstance(execution, TrafficSpikeExecution):
        return [
            *arguments,
            "--count",
            "0",
            "--baseline-count",
            str(execution.baseline_event_count),
            "--baseline-rate",
            str(execution.baseline_rate),
            "--burst-count",
            str(execution.burst_event_count),
            "--burst-rate",
            str(execution.burst_rate),
        ]
    if isinstance(execution, MalformedEventsExecution):
        return [
            *arguments,
            "--count",
            str(execution.valid_event_count),
            "--malformed-count",
            str(execution.malformed_event_count),
            "--rate",
            str(execution.producer_rate),
        ]
    return [
        *arguments,
        "--count",
        str(execution.event_count),
        "--rate",
        str(execution.producer_rate),
    ]


def _consumer_arguments(
    manifest: ScenarioManifest,
    settings: Settings,
    run_id: str,
    topic: str,
    group: str,
) -> list[str]:
    execution = manifest.execution
    processing_delay = (
        execution.processing_delay_ms if isinstance(execution, SlowConsumerExecution) else 0
    )
    database_delay = (
        execution.database_delay_ms if isinstance(execution, DatabaseLatencyExecution) else 0
    )
    arguments = [
        sys.executable,
        "-m",
        "incidentops.consumer",
        "--run-id",
        run_id,
        "--topic",
        topic,
        "--group",
        group,
        "--processing-delay-ms",
        str(processing_delay),
        "--database-delay-ms",
        str(database_delay),
        "--slow-processing-threshold-ms",
        str(execution.slow_processing_threshold_ms),
        "--slow-database-threshold-ms",
        str(execution.database_slow_threshold_ms),
        "--lag-update-interval-seconds",
        "1",
        "--metrics-port",
        str(settings.consumer_metrics_port),
    ]
    if isinstance(execution, MalformedEventsExecution):
        arguments.extend(
            [
                "--max-messages",
                str(execution.valid_event_count + execution.malformed_event_count),
                "--idle-timeout",
                str(execution.timeout_seconds),
            ]
        )
    return arguments


def _metric_evidence(
    toolset: InvestigationToolset,
    task: InvestigationTaskType,
    tool_input: InvestigationToolInput,
) -> MetricEvidence:
    result = toolset.execute(task, tool_input)
    if (
        not isinstance(result, MetricEvidence)
        or result.availability != EvidenceAvailability.AVAILABLE
    ):
        raise ScenarioRunError(f"{task.value} did not return available metric evidence")
    return result


def _log_count(result: LogEvidence | NegativeEvidence) -> int:
    return result.matching_log_count if result.availability == EvidenceAvailability.AVAILABLE else 0


def collect_observations(
    toolset: InvestigationToolset,
    *,
    start_time: datetime,
    end_time: datetime,
    run_id: str,
) -> SlowConsumerObservations:
    """Collect the same six bounded evidence profiles used by LangGraph."""

    tool_input = InvestigationToolInput(
        start_time=start_time,
        end_time=end_time,
        run_id=run_id,
    )
    lag = _metric_evidence(toolset, InvestigationTaskType.CHECK_CONSUMER_LAG, tool_input)
    latency = _metric_evidence(
        toolset,
        InvestigationTaskType.CHECK_PROCESSING_LATENCY,
        tool_input,
    )
    rates = _metric_evidence(
        toolset,
        InvestigationTaskType.COMPARE_PRODUCER_CONSUMER_RATES,
        tool_input,
    )
    application_logs = toolset.execute(
        InvestigationTaskType.FIND_SLOW_PROCESSING_LOGS,
        tool_input,
    )
    database_errors = toolset.execute(InvestigationTaskType.FIND_DATABASE_ERRORS, tool_input)
    kafka_errors = toolset.execute(InvestigationTaskType.FIND_KAFKA_ERRORS, tool_input)
    if not isinstance(application_logs, LogEvidence):
        raise ScenarioRunError("application signal query returned an invalid evidence type")

    if not isinstance(database_errors, LogEvidence | NegativeEvidence) or not isinstance(
        kafka_errors, LogEvidence | NegativeEvidence
    ):
        raise ScenarioRunError("dependency error query returned an invalid evidence type")
    return SlowConsumerObservations.model_validate(
        {
            "maximum_lag": lag.raw_value_summary["maximum"],
            "lag_start": lag.raw_value_summary["start_value"],
            "lag_end": lag.raw_value_summary["end_value"],
            "lag_trend": lag.raw_value_summary["trend"],
            "lag_samples": lag.raw_value_summary["sample_count"],
            "p95_seconds": latency.raw_value_summary["duration_seconds"],
            "latency_samples": latency.raw_value_summary["sample_count"],
            "database_p95_seconds": latency.raw_value_summary["database_duration_seconds"],
            "database_latency_samples": latency.raw_value_summary["database_sample_count"],
            "processing_state": latency.raw_value_summary["processing_state"],
            "database_state": latency.raw_value_summary["database_state"],
            "producer_rate": rates.raw_value_summary["producer_windowed_rate_per_second"],
            "consumer_rate": rates.raw_value_summary["consumer_windowed_rate_per_second"],
            "consumer_is_slower": rates.raw_value_summary["consumer_is_slower"],
            "producer_baseline_rate": rates.raw_value_summary["producer_baseline_rate_per_second"],
            "producer_recent_rate": rates.raw_value_summary["producer_recent_rate_per_second"],
            "producer_rate_change_ratio": rates.raw_value_summary["producer_rate_change_ratio"],
            "producer_surge": rates.raw_value_summary["producer_surge"],
            "processing_error_rate": rates.raw_value_summary["processing_error_rate_per_second"],
            "processing_errors_present": rates.raw_value_summary["processing_errors_present"],
            "valid_processing_present": rates.raw_value_summary["valid_processing_present"],
            "slow_processing_log_count": application_logs.raw_value_summary[
                "slow_processing_count"
            ],
            "database_operation_slow_log_count": application_logs.raw_value_summary[
                "database_operation_slow_count"
            ],
            "invalid_event_log_count": application_logs.raw_value_summary["invalid_event_count"],
            "database_error_count": _log_count(database_errors),
            "kafka_error_count": _log_count(kafka_errors),
        }
    )


def observations_match_manifest(
    manifest: ScenarioManifest,
    observations: SlowConsumerObservations,
) -> bool:
    """Apply ground truth only in the harness acceptance boundary."""

    values = {
        TelemetrySignal.CONSUMER_LAG: observations.lag_trend,
        TelemetrySignal.PROCESSING_LATENCY: observations.processing_state,
        TelemetrySignal.DATABASE_LATENCY: observations.database_state,
        TelemetrySignal.PRODUCER_RATE_CHANGE: (
            TelemetryBehavior.SURGING.value
            if observations.producer_surge
            else TelemetryBehavior.STABLE.value
        ),
        TelemetrySignal.PROCESSING_ERRORS: (
            TelemetryBehavior.PRESENT.value
            if observations.processing_errors_present
            else TelemetryBehavior.ABSENT.value
        ),
        TelemetrySignal.VALID_PROCESSING: (
            TelemetryBehavior.PRESENT.value
            if observations.valid_processing_present
            else TelemetryBehavior.ABSENT.value
        ),
    }
    if any(values[item.signal] != item.behavior.value for item in manifest.expected_metrics):
        return False
    minimum_lag = getattr(manifest.execution, "minimum_lag", None)
    if minimum_lag is not None and observations.maximum_lag < minimum_lag:
        return False
    log_counts = {
        "slow_processing": observations.slow_processing_log_count,
        "database_operation_slow": observations.database_operation_slow_log_count,
        "invalid_event_skipped": observations.invalid_event_log_count,
    }
    if any(log_counts[item.event_type] <= 0 for item in manifest.expected_logs):
        return False
    negative_counts = {
        "no_database_errors": observations.database_error_count,
        "no_kafka_broker_errors": observations.kafka_error_count,
    }
    return all(negative_counts[item] == 0 for item in manifest.negative_evidence)


class _ScenarioResources:
    """Own only resources derived from one validated scenario token."""

    def __init__(
        self,
        manifest: ScenarioManifest,
        settings: Settings,
        *,
        retain_evidence: bool,
    ) -> None:
        token = f"{time.time_ns()}-{os.getpid()}"
        prefix = _resource_prefix(manifest)
        # Keep the graph-visible identifier neutral; topic and group names never
        # cross the investigation boundary.
        self.run_id = f"incident-run-{token}"
        self.topic = f"orders.{prefix}.{token}"
        self.group = f"{prefix}-{token}"
        self.log_directory = PROJECT_DIRECTORY / "logs" / self.run_id
        self.temp_directory = Path(tempfile.mkdtemp(prefix=f"incidentops-{prefix}."))
        self.manifest = manifest
        self.settings = settings
        self.retain_evidence = retain_evidence
        self.processes: list[ManagedProcess] = []
        self.admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.topic_created = False
        self.group_created = False
        self.evidence_ready = False

    def __enter__(self) -> _ScenarioResources:
        try:
            future = self.admin.create_topics(
                [NewTopic(self.topic, num_partitions=1, replication_factor=1)],
                operation_timeout=10,
            )[self.topic]
            future.result(timeout=15)
            self.topic_created = True
            self.log_directory.mkdir(parents=True, exist_ok=False)
            return self
        except Exception:
            if self.topic_created:
                try:
                    self.admin.delete_topics([self.topic], operation_timeout=10)[self.topic].result(
                        timeout=15
                    )
                except KafkaException:
                    pass
            if self.log_directory.is_dir():
                shutil.rmtree(self.log_directory)
            shutil.rmtree(self.temp_directory)
            raise

    def start(self, name: str, arguments: list[str]) -> ManagedProcess:
        env = os.environ.copy()
        env["LOG_DIRECTORY"] = str(self.log_directory)
        process = _start_process(
            name,
            arguments,
            self.temp_directory / f"{name}.jsonl",
            env,
        )
        self.processes.append(process)
        return process

    def _delete_database_rows(self) -> None:
        connection = connect_database(self.settings)
        try:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(DELETE_SCENARIO_ROWS_SQL, (f"{self.run_id}-%",))
        finally:
            connection.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        cleanup_errors: list[str] = []
        for process in reversed(self.processes):
            try:
                process.stop()
            except ScenarioRunError as error:
                cleanup_errors.append(str(error))
            finally:
                process.close()
        if self.group_created:
            try:
                self.admin.delete_consumer_groups([self.group], request_timeout=10)[
                    self.group
                ].result(timeout=15)
            except KafkaException as error:
                cleanup_errors.append(f"consumer group cleanup failed: {type(error).__name__}")
        try:
            self._delete_database_rows()
        except Exception as error:
            cleanup_errors.append(f"database cleanup failed: {type(error).__name__}")
        if self.topic_created:
            try:
                self.admin.delete_topics([self.topic], operation_timeout=10)[self.topic].result(
                    timeout=15
                )
            except KafkaException as error:
                cleanup_errors.append(f"topic cleanup failed: {type(error).__name__}")
        if self.log_directory.is_dir():
            try:
                shutil.rmtree(self.log_directory)
            except OSError as error:
                cleanup_errors.append(f"JSONL cleanup failed: {type(error).__name__}")
        preserve_evidence = (
            self.retain_evidence
            and self.evidence_ready
            and exc_value is None
            and not cleanup_errors
        )
        if not preserve_evidence:
            try:
                delete_run_logs_and_verify(
                    self.run_id,
                    elasticsearch_url=self.settings.elasticsearch_url,
                )
            except Exception as error:
                cleanup_errors.append(f"log cleanup failed: {type(error).__name__}")
        try:
            shutil.rmtree(self.temp_directory)
        except OSError as error:
            cleanup_errors.append(f"temporary file cleanup failed: {type(error).__name__}")
        if cleanup_errors and exc_value is None:
            raise ScenarioRunError("; ".join(cleanup_errors))
        return False


def write_metadata(path: Path, metadata: ScenarioMetadata) -> None:
    """Persist validated metadata atomically without any ground-truth fields."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(metadata.model_dump_json(indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_scenario(
    manifest: ScenarioManifest,
    settings: Settings,
    *,
    retain_evidence: bool = False,
    output_metadata: Path | None = None,
) -> ScenarioMetadata:
    """Execute one bounded scenario and return only neutral run metadata and observations."""

    with _ScenarioResources(manifest, settings, retain_evidence=retain_evidence) as resources:
        started_at = datetime.now(UTC)
        consumer = resources.start(
            "consumer",
            _consumer_arguments(
                manifest,
                settings,
                resources.run_id,
                resources.topic,
                resources.group,
            ),
        )
        resources.group_created = True
        _wait_for_output(consumer, '"event_type":"partitions_assigned"', 30)
        producer = resources.start(
            "producer",
            _producer_arguments(manifest, settings, resources.run_id, resources.topic),
        )
        _wait_for_output(
            producer,
            '"event_type":"production_summary"',
            manifest.execution.timeout_seconds,
        )

        toolset = InvestigationToolset.from_settings(settings)
        try:
            deadline = time.monotonic() + manifest.execution.timeout_seconds
            last_error: Exception | None = None
            last_candidate: SlowConsumerObservations | None = None
            observations: SlowConsumerObservations | None = None
            while time.monotonic() < deadline:
                try:
                    candidate = collect_observations(
                        toolset,
                        start_time=started_at,
                        end_time=datetime.now(UTC),
                        run_id=resources.run_id,
                    )
                    last_candidate = candidate
                    if observations_match_manifest(manifest, candidate):
                        observations = candidate
                        break
                except Exception as error:
                    last_error = error
                if consumer.process.poll() not in {None, 0}:
                    raise ScenarioRunError(f"consumer exited unexpectedly:\n{consumer.output()}")
                time.sleep(2)
            if observations is None:
                if last_candidate is not None:
                    detail = f"; last observations: {last_candidate.model_dump_json()}"
                elif last_error is not None:
                    detail = f"; last collection error: {last_error}"
                else:
                    detail = ""
                raise ScenarioRunError(
                    f"{manifest.id} telemetry did not reach its bounded signature{detail}"
                )
        finally:
            toolset.close()

        ended_at = datetime.now(UTC)
        metadata = ScenarioMetadata(
            scenario_id=manifest.id,
            # The investigation boundary receives operational metadata only.  All
            # executable scenarios currently exercise the same consumer service;
            # do not derive this field from the ground-truth root-cause object.
            affected_service="order-consumer",
            run_id=resources.run_id,
            topic=resources.topic,
            consumer_group=resources.group,
            start_time=started_at,
            end_time=ended_at,
            observations=observations,
        )
        if output_metadata is not None:
            write_metadata(output_metadata, metadata)
        resources.evidence_ready = True
        return metadata
