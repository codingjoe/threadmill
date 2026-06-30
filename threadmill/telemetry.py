"""Worker-pool telemetry sampling via psutil and Redis pub/sub.

The :class:`TelemetrySampler` runs inside each worker process. It periodically
samples the process and node CPU/memory plus the rolling task throughput, then
publishes a :class:`~threadmill.backends.base.WorkerTelemetry` snapshot through
the backend's :meth:`publish_worker_telemetry` hook. ``psutil`` is an optional
dependency; when it is missing the sampler is a silent no-op so the worker
command still functions.
"""

from __future__ import annotations

import collections
import datetime
import logging
import socket
import threading
import time
import typing

from .backends.base import (
    NodeTelemetry,
    WorkerProcessTelemetry,
    WorkerTelemetry,
)

logger = logging.getLogger(__name__)

try:
    import psutil
except ImportError:  # pragma: no cover - exercised via the guarded path
    psutil = None  # type: ignore[assignment]


SAMPLE_INTERVAL_SECONDS = 2.0
"""Default seconds between worker telemetry samples."""


class TelemetrySampler:
    """Periodically sample and publish worker-pool telemetry.

    The sampler is driven by a daemon thread started in
    :meth:`start` and stopped in :meth:`stop`. Sampling only runs when
    ``psutil`` is importable; without it the sampler publishes nothing and the
    worker keeps working.
    """

    def __init__(
        self,
        backend,
        *,
        worker_process,
        interval_seconds: float = SAMPLE_INTERVAL_SECONDS,
        clock: typing.Callable[[], float] | None = None,
    ) -> None:
        self.backend = backend
        self.worker_process = worker_process
        self.interval_seconds = interval_seconds
        self._clock = clock or time.monotonic
        self._stop_requested = threading.Event()
        self._thread: threading.Thread | None = None
        self._task_count_previous = 0
        self._sampled_at_previous: float | None = None

    @property
    def is_available(self) -> bool:
        """Whether psutil is importable and sampling is active."""
        return psutil is not None

    def start(self) -> None:
        """Start the background sampling thread, if psutil is available."""
        if not self.is_available or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="worker-telemetry-sampler", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        """Request the sampling thread to stop and wait for it to exit."""
        self._stop_requested.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_seconds + 1)
            self._thread = None

    def _run(self) -> None:
        # Prime the CPU percent counters so the first sample is meaningful.
        psutil.cpu_percent(interval=None)
        psutil.Process().cpu_percent(interval=None)
        while not self._stop_requested.wait(self.interval_seconds):
            try:
                self._publish_one_sample()
            except Exception:  # noqa: BLE001
                logger.exception("Failed to sample worker telemetry")

    def _publish_one_sample(self) -> None:
        snapshot = self.sample()
        self.backend.publish_worker_telemetry(snapshot)

    def sample(self) -> WorkerTelemetry:
        """Build and return one :class:`WorkerTelemetry` snapshot."""
        now = datetime.datetime.now(tz=datetime.UTC)
        now_monotonic = self._clock()

        process = psutil.Process()
        worker_name = self.worker_process.name
        queues = tuple(self.worker_process.queues)
        thread_count = self.worker_process.thread_count
        task_count = self.worker_process.task_count

        tasks_per_minute = self._tasks_per_minute(task_count, now_monotonic)
        worker = WorkerProcessTelemetry(
            name=worker_name,
            pid=self.worker_process.pid or process.pid,
            queues=queues,
            thread_count=thread_count,
            task_count=task_count,
            tasks_per_minute=tasks_per_minute,
            cpu_percent=process.cpu_percent(interval=None),
            memory_bytes=process.memory_info().rss,
            sampled_at=now,
        )

        virtual_memory = psutil.virtual_memory()
        node = NodeTelemetry(
            hostname=socket.gethostname(),
            queues=queues,
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_percent=virtual_memory.percent,
            memory_bytes=virtual_memory.total,
            tasks_per_minute=tasks_per_minute,
            workers={worker_name: worker},
            sampled_at=now,
        )

        queue_index: dict[str, list[str]] = collections.defaultdict(list)
        for queue_name in queues:
            queue_index[queue_name].append(node.hostname)

        return WorkerTelemetry(
            nodes={node.hostname: node},
            queues={
                queue_name: tuple(hostnames)
                for queue_name, hostnames in queue_index.items()
            },
            sampled_at=now,
        )

    def _tasks_per_minute(self, task_count: int, now_monotonic: float) -> float:
        """Compute tasks/minute from the delta since the previous sample."""
        if self._sampled_at_previous is None:
            self._task_count_previous = task_count
            self._sampled_at_previous = now_monotonic
            return 0.0
        elapsed_seconds = now_monotonic - self._sampled_at_previous
        if elapsed_seconds <= 0:
            return 0.0
        delta = task_count - self._task_count_previous
        self._task_count_previous = task_count
        self._sampled_at_previous = now_monotonic
        return delta / elapsed_seconds * 60.0
