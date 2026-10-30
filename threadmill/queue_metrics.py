"""Export queue state and pub/sub telemetry as OpenTelemetry metrics."""

import asyncio
import dataclasses
import logging

from .backends.base import TelemetryDirection, ThreadmillTaskBackend
from .metrics import Metrics

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SAMPLE_INTERVAL_S = 15.0
STREAM_RECONNECT_DELAY_S = 1.0


class QueueMetricsExporter:
    """Feed sampled backend state and ingress/egress task events into a recorder."""

    def __init__(
        self,
        *,
        backend: ThreadmillTaskBackend,
        metrics: Metrics,
        interval: float = DEFAULT_QUEUE_SAMPLE_INTERVAL_S,
    ) -> None:
        self.backend = backend
        self.metrics = metrics
        self.interval = interval

    @staticmethod
    def require_telemetry_support(backend: ThreadmillTaskBackend) -> None:
        """Raise when the backend leaves queue state or worker telemetry unimplemented."""
        for name in ("queue_stats", "worker_telemetry"):
            if getattr(type(backend), name) is getattr(ThreadmillTaskBackend, name):
                message = f"Backend {backend.alias!r} does not implement {name}."
                raise NotImplementedError(message)

    async def sample_once(self) -> None:
        """Record the current segment counts and backlog of every configured queue."""
        telemetry = await self.backend.queue_stats()
        for queue_name, stats in telemetry.queues.items():
            if queue_name not in self.backend.queues:
                continue
            for status, count in dataclasses.asdict(stats.counts).items():
                self.metrics.record_queue_tasks(
                    queue_name=queue_name, status=status, count=count
                )
            self.metrics.record_queue_backlog(
                queue_name=queue_name,
                count=stats.counts.ready + stats.counts.running,
            )

    async def sample_periodically(self) -> None:
        """Sample backend state at a fixed interval until cancelled."""
        while True:
            try:
                await self.sample_once()
            except Exception as exception:
                if isinstance(exception, (NotImplementedError, TypeError)):
                    raise
                logger.exception("Failed to sample queue state.")
            await asyncio.sleep(self.interval)

    async def consume_events(self) -> None:
        """Count tasks entering and leaving the backend stream until cancelled."""
        recorders = {
            TelemetryDirection.INGRESS: self.metrics.record_queue_ingress,
            TelemetryDirection.EGRESS: self.metrics.record_queue_egress,
        }
        while True:
            try:
                async for event in self.backend.worker_telemetry():
                    if event.queue_name in self.backend.queues:
                        recorders[event.direction](queue_name=event.queue_name)
            except Exception as exception:
                if isinstance(exception, (NotImplementedError, TypeError)):
                    raise
                logger.exception("Worker telemetry stream failed; reconnecting.")
            # todo: constant reconnect delay, add exponential backoff if flapping matters
            await asyncio.sleep(STREAM_RECONNECT_DELAY_S)

    async def run(self) -> None:
        """Start sampling and event consumption concurrently; one failure cancels the other."""
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(self.sample_periodically())
            task_group.create_task(self.consume_events())
