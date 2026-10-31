import asyncio
import dataclasses
import datetime
import os

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource

from .backends.base import TelemetryDirection, ThreadmillTaskBackend


class OpenTelemetryMetrics:
    meter_provider: MeterProvider

    def __init__(self, meter_provider: MeterProvider) -> None:
        self.meter_provider = meter_provider
        meter = meter_provider.get_meter("threadmill")
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


class QueueMetricsExporter:
    def __init__(
        self,
        *,
        backend: ThreadmillTaskBackend,
        metrics: OpenTelemetryMetrics,
        interval: datetime.timedelta,
        reconnect_delay: datetime.timedelta,
    ) -> None:
        self.backend = backend
        self.metrics = metrics
        self.interval = interval
        self.reconnect_delay = reconnect_delay

    async def sample_once(self) -> None:
        telemetry = await self.backend.queue_stats()
        for queue_name, stats in telemetry.queues.items():
            if queue_name in self.backend.queues:
                for status, count in dataclasses.asdict(stats.counts).items():
                    self.metrics.record_queue_tasks(
                        queue_name=queue_name, status=status, count=count
                    )
                self.metrics.record_queue_backlog(
                    queue_name=queue_name,
                    count=stats.counts.ready + stats.counts.running,
                )

    async def sample_periodically(self) -> None:
        while True:
            await self.sample_once()
            await asyncio.sleep(self.interval.total_seconds())

    async def consume_events(self) -> None:
        recorders = {
            TelemetryDirection.INGRESS: self.metrics.record_queue_ingress,
            TelemetryDirection.EGRESS: self.metrics.record_queue_egress,
        }
        while True:
            async for event in self.backend.worker_telemetry():
                if event.queue_name in self.backend.queues:
                    recorders[event.direction](queue_name=event.queue_name)
            await asyncio.sleep(self.reconnect_delay.total_seconds())

    async def run(self) -> None:
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self.sample_periodically())
            task_group.create_task(self.consume_events())


def create_metric_exporter() -> MetricExporter:
    match os.environ.get("OTEL_METRICS_EXPORTER", "otlp").lower():
        case "console":
            exporter = ConsoleMetricExporter()
        case "otlp":
            exporter = OTLPMetricExporter()
        case value:
            message = f"Unsupported OTEL_METRICS_EXPORTER: {value!r}"
            raise ValueError(message)
    return exporter


def configure_metrics(
    export_interval: datetime.timedelta,
    *,
    backend_alias: str | None = None,
) -> OpenTelemetryMetrics:
    reader = PeriodicExportingMetricReader(
        create_metric_exporter(),
        export_interval_millis=int(export_interval.total_seconds() * 1000),
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
