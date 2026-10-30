"""Record task, worker, and queue telemetry as OpenTelemetry metrics."""

import importlib
import importlib.util
import logging
import os
import typing

if typing.TYPE_CHECKING:
    from opentelemetry.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import MetricExporter

logger = logging.getLogger(__name__)

DEFAULT_EXPORT_INTERVAL_MS = 15_000
"""Fallback in milliseconds; the SDK default of 60 seconds is too slow for autoscaling."""

TASK_DURATION_BUCKETS_S = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)
"""Histogram bucket boundaries in seconds; the SDK default targets milliseconds."""


class Metrics:
    """Discard every recording call, so call sites never check whether export is on."""

    def record_task_started(self, *, queue_name: str, task_module_path: str) -> None:
        """Count every attempt that begins execution, including retries."""

    def record_task_finished(
        self,
        *,
        queue_name: str,
        task_module_path: str,
        status: str,
        duration_s: float,
    ) -> None:
        """Count every attempt that reaches an outcome and observe how long it ran."""

    def record_busy_threads(self, *, busy_threads: int, thread_count: int) -> None:
        """Report how many execution slots are occupied, the signal that sizes the pool."""

    def record_queue_tasks(self, *, queue_name: str, status: str, count: int) -> None:
        """Report how many entries sit in one segment at the moment of sampling."""

    def record_queue_backlog(self, *, queue_name: str, count: int) -> None:
        """Report the combined pending and active depth used as the autoscaling signal."""

    def record_queue_ingress(self, *, queue_name: str) -> None:
        """Count each addition observed on the broker stream."""

    def record_queue_egress(self, *, queue_name: str) -> None:
        """Count each removal observed on the broker stream, acknowledged or returned."""

    def shutdown(self) -> None:
        """Flush pending samples and stop exporting."""


class OpenTelemetryMetrics(Metrics):
    """Emit every call through the supplied SDK provider."""

    meter_provider: MeterProvider

    def __init__(self, meter_provider: MeterProvider) -> None:
        self.meter_provider = meter_provider
        meter = meter_provider.get_meter("threadmill")
        self.task_started = meter.create_counter(
            "threadmill.task.started",
            unit="{task}",
            description="Task attempts started.",
        )
        self.task_finished = meter.create_counter(
            "threadmill.task.finished",
            unit="{task}",
            description="Task attempts finished.",
        )
        self.task_duration = meter.create_histogram(
            "threadmill.task.duration",
            unit="s",
            description="Task execution duration.",
            explicit_bucket_boundaries_advisory=TASK_DURATION_BUCKETS_S,
        )
        self.busy_threads = meter.create_gauge(
            "threadmill.worker.busy_threads",
            unit="{thread}",
            description="Busy worker threads in this process.",
        )
        self.queue_tasks = meter.create_gauge(
            "threadmill.queue.tasks",
            unit="{task}",
            description="Point-in-time task count per queue segment.",
        )
        self.queue_backlog = meter.create_gauge(
            "threadmill.queue.backlog",
            unit="{task}",
            description="Ready and running tasks per queue.",
        )
        self.queue_ingress = meter.create_counter(
            "threadmill.queue.ingress",
            unit="{task}",
            description="Tasks enqueued.",
        )
        self.queue_egress = meter.create_counter(
            "threadmill.queue.egress",
            unit="{task}",
            description="Tasks acknowledged or requeued.",
        )

    def record_task_started(self, *, queue_name: str, task_module_path: str) -> None:
        self.task_started.add(
            1,
            {
                "threadmill.queue.name": queue_name,
                "threadmill.task.module_path": task_module_path,
            },
        )

    def record_task_finished(
        self,
        *,
        queue_name: str,
        task_module_path: str,
        status: str,
        duration_s: float,
    ) -> None:
        attributes = {
            "threadmill.queue.name": queue_name,
            "threadmill.task.module_path": task_module_path,
            "threadmill.task.status": status,
        }
        self.task_finished.add(1, attributes)
        self.task_duration.record(duration_s, attributes)

    def record_busy_threads(self, *, busy_threads: int, thread_count: int) -> None:
        self.busy_threads.set(
            busy_threads, {"threadmill.worker.thread_count": thread_count}
        )

    def record_queue_tasks(self, *, queue_name: str, status: str, count: int) -> None:
        self.queue_tasks.set(
            count,
            {
                "threadmill.queue.name": queue_name,
                "threadmill.queue.segment": status,
            },
        )

    def record_queue_backlog(self, *, queue_name: str, count: int) -> None:
        self.queue_backlog.set(count, {"threadmill.queue.name": queue_name})

    def record_queue_ingress(self, *, queue_name: str) -> None:
        self.queue_ingress.add(1, {"threadmill.queue.name": queue_name})

    def record_queue_egress(self, *, queue_name: str) -> None:
        self.queue_egress.add(1, {"threadmill.queue.name": queue_name})

    def shutdown(self) -> None:
        self.meter_provider.shutdown()


def is_metrics_sdk_available() -> bool:
    """Report whether the optional dependency can be imported."""
    try:
        return importlib.util.find_spec("opentelemetry.sdk.metrics") is not None
    except ModuleNotFoundError:
        return False


def is_metrics_export_enabled() -> bool:
    """Report whether configuration asks for telemetry output."""
    if os.environ.get("OTEL_SDK_DISABLED", "").lower() == "true":
        return False
    return os.environ.get("OTEL_METRICS_EXPORTER", "otlp").lower() != "none"


def create_metric_exporter() -> MetricExporter:
    """
    Build the output sink named by `OTEL_METRICS_EXPORTER` and its protocol.

    Raise `ValueError` when the name or protocol is unknown.
    """
    match os.environ.get("OTEL_METRICS_EXPORTER", "otlp").lower():
        case "console":
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

            return ConsoleMetricExporter()
        case "otlp":
            protocol = (
                os.environ.get("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
                or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
                or "http/protobuf"
            )
            exporter_modules = {
                "grpc": "opentelemetry.exporter.otlp.proto.grpc.metric_exporter",
                "http/protobuf": "opentelemetry.exporter.otlp.proto.http.metric_exporter",
            }
            try:
                exporter_module = exporter_modules[protocol]
            except KeyError:
                message = f"Unsupported OTLP protocol: {protocol!r}"
                raise ValueError(message) from None
            return importlib.import_module(exporter_module).OTLPMetricExporter()
        case exporter:
            message = f"Unsupported OTEL_METRICS_EXPORTER: {exporter!r}"
            raise ValueError(message)


def read_export_interval_ms() -> int:
    """
    Return the publishing period in milliseconds.

    Use 15000 for unset, invalid, or non-positive values.
    """
    try:
        interval_ms = int(
            os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", DEFAULT_EXPORT_INTERVAL_MS)
        )
    except ValueError:
        return DEFAULT_EXPORT_INTERVAL_MS
    return interval_ms if interval_ms > 0 else DEFAULT_EXPORT_INTERVAL_MS


def configure_metrics(*, backend_alias: str | None = None) -> Metrics:
    """
    Build the live recorder for this process.

    Fall back to the no-op recorder when output is disabled or the SDK is
    missing, and tag the resource with `backend_alias` when given.
    """
    if not is_metrics_export_enabled():
        return Metrics()
    if not is_metrics_sdk_available():
        logger.warning(
            "Metrics export is enabled, but the OpenTelemetry SDK is missing. "
            "Install threadmill[opentelemetry]."
        )
        return Metrics()

    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource

    reader = PeriodicExportingMetricReader(
        create_metric_exporter(), export_interval_millis=read_export_interval_ms()
    )
    resource = Resource.create(
        {
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "threadmill"),
            "process.pid": os.getpid(),
        }
        | ({"threadmill.backend.alias": backend_alias} if backend_alias else {})
    )
    return OpenTelemetryMetrics(
        MeterProvider(metric_readers=[reader], resource=resource)
    )
