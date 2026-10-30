"""Tests for the queue metrics exporter."""

import asyncio
import datetime
import logging

import pytest

from tests.testapp.backends import StubTaskBackend, TelemetryTaskBackend
from tests.testapp.metrics import RecordingMetrics
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
    TelemetryDirection,
    TelemetryEvent,
    ThreadmillTaskBackend,
)
from threadmill.queue_metrics import QueueMetricsExporter


def _stats(
    *,
    ready: int = 0,
    running: int = 0,
    deferred: int = 0,
    successful: int = 0,
    failed: int = 0,
) -> QueueStats:
    """Build queue stats with explicit segment counts."""
    return QueueStats(
        counts=QueueCounts(
            ready=ready,
            running=running,
            deferred=deferred,
            successful=successful,
            failed=failed,
        ),
        rates=QueueRates(interval=datetime.timedelta(seconds=60), ingress=0, egress=0),
    )


def _backend(*, queues=("default",)) -> TelemetryTaskBackend:
    """Build a scripted telemetry backend with the given configured queues."""
    return TelemetryTaskBackend("telemetry", {"QUEUES": list(queues)})


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    """Yield until the predicate holds or the timeout expires."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            message = "condition was not met"
            raise AssertionError(message)
        await asyncio.sleep(0)


async def _cancel(task: asyncio.Task) -> None:
    """Cancel a background loop and wait for its cancellation."""
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


class QueueStatsOnlyBackend(ThreadmillTaskBackend):
    """Backend double implementing queue stats but not the event stream."""

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    async def queue_stats(
        self, *, interval=datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        return BackendTelemetry(queues={})


class TestRequireTelemetrySupport:
    """Tests for the backend capability guard."""

    def test_require_telemetry_support__accepts_implemented_backend(self) -> None:
        """Accept a backend that overrides both telemetry methods."""
        QueueMetricsExporter.require_telemetry_support(_backend())

    def test_require_telemetry_support__rejects_missing_queue_stats(self) -> None:
        """Reject a backend that leaves queue_stats unimplemented."""
        backend = StubTaskBackend("stub", {"QUEUES": ["default"]})
        with pytest.raises(NotImplementedError, match="does not implement queue_stats"):
            QueueMetricsExporter.require_telemetry_support(backend)

    def test_require_telemetry_support__rejects_missing_worker_telemetry(
        self,
    ) -> None:
        """Reject a backend that leaves worker_telemetry unimplemented."""
        backend = QueueStatsOnlyBackend("stub", {"QUEUES": ["default"]})
        with pytest.raises(
            NotImplementedError, match="does not implement worker_telemetry"
        ):
            QueueMetricsExporter.require_telemetry_support(backend)


class TestSampleOnce:
    """Tests for the single sampling pass."""

    async def test_sample_once__records_segments_and_backlog(self) -> None:
        """Record each segment count and the ready-plus-running backlog."""
        backend = _backend()
        recorder = RecordingMetrics()
        backend.telemetry = [
            BackendTelemetry(
                queues={
                    "default": _stats(
                        ready=3,
                        running=2,
                        deferred=1,
                        successful=4,
                        failed=5,
                    )
                }
            )
        ]

        await QueueMetricsExporter(backend=backend, metrics=recorder).sample_once()

        assert recorder.calls == [
            ("queue_tasks", {"queue_name": "default", "status": "ready", "count": 3}),
            (
                "queue_tasks",
                {"queue_name": "default", "status": "running", "count": 2},
            ),
            (
                "queue_tasks",
                {"queue_name": "default", "status": "deferred", "count": 1},
            ),
            (
                "queue_tasks",
                {"queue_name": "default", "status": "successful", "count": 4},
            ),
            (
                "queue_tasks",
                {"queue_name": "default", "status": "failed", "count": 5},
            ),
            ("queue_backlog", {"queue_name": "default", "count": 5}),
        ]

    async def test_sample_once__skips_unconfigured_queues(self) -> None:
        """Drop snapshots for queues this backend does not serve."""
        backend = _backend(queues=("default",))
        recorder = RecordingMetrics()
        backend.telemetry = [
            BackendTelemetry(
                queues={
                    "default": _stats(ready=1),
                    "extra": _stats(ready=9),
                }
            )
        ]

        await QueueMetricsExporter(backend=backend, metrics=recorder).sample_once()

        assert recorder.calls_named("queue_tasks") == [
            {"queue_name": "default", "status": "ready", "count": 1},
            {"queue_name": "default", "status": "running", "count": 0},
            {"queue_name": "default", "status": "deferred", "count": 0},
            {"queue_name": "default", "status": "successful", "count": 0},
            {"queue_name": "default", "status": "failed", "count": 0},
        ]
        assert recorder.calls_named("queue_backlog") == [
            {"queue_name": "default", "count": 1}
        ]

    async def test_sample_once__records_nothing_for_empty_snapshot(self) -> None:
        """Record no series when the backend reports no queues."""
        backend = _backend()
        recorder = RecordingMetrics()

        await QueueMetricsExporter(backend=backend, metrics=recorder).sample_once()

        assert recorder.calls == []


class TestSamplePeriodically:
    """Tests for the sampling loop."""

    async def test_sample_periodically__samples_until_cancelled(self) -> None:
        """Keep sampling at the configured interval until cancelled."""
        backend = _backend()
        recorder = RecordingMetrics()
        backend.telemetry = [BackendTelemetry(queues={"default": _stats(ready=1)})]
        exporter = QueueMetricsExporter(
            backend=backend, metrics=recorder, interval=0.001
        )

        task = asyncio.create_task(exporter.sample_periodically())
        await _wait_until(lambda: recorder.calls_named("queue_backlog"))
        await _cancel(task)

        assert backend.queue_stats_calls >= 1

    async def test_sample_periodically__logs_failure_and_continues(
        self, caplog
    ) -> None:
        """Log a transient sampling failure and keep the loop running."""
        backend = _backend()
        recorder = RecordingMetrics()
        backend.telemetry = [
            RuntimeError("sampling failed"),
            BackendTelemetry(queues={"default": _stats(ready=2)}),
        ]
        exporter = QueueMetricsExporter(
            backend=backend, metrics=recorder, interval=0.001
        )

        with caplog.at_level(logging.ERROR, logger="threadmill.queue_metrics"):
            task = asyncio.create_task(exporter.sample_periodically())
            await _wait_until(lambda: recorder.calls_named("queue_backlog"))
            await _cancel(task)

        assert "Failed to sample queue state." in caplog.text
        assert backend.queue_stats_calls >= 2

    @pytest.mark.parametrize(
        "error",
        [NotImplementedError("unsupported"), TypeError("bad contract")],
        ids=["not-implemented", "type-error"],
    )
    async def test_sample_periodically__reraises_contract_errors(self, error) -> None:
        """Surface contract violations instead of retrying forever."""
        backend = _backend()
        backend.telemetry = [error]
        exporter = QueueMetricsExporter(
            backend=backend, metrics=RecordingMetrics(), interval=0.001
        )

        with pytest.raises(type(error), match=str(error)):
            await exporter.sample_periodically()


class TestConsumeEvents:
    """Tests for the pub/sub event loop."""

    async def test_consume_events__counts_ingress_and_egress(self, monkeypatch) -> None:
        """Count both directions for configured queues and drop foreign ones."""
        monkeypatch.setattr("threadmill.queue_metrics.STREAM_RECONNECT_DELAY_S", 0.0)
        backend = _backend()
        recorder = RecordingMetrics()
        backend.events = [
            [
                TelemetryEvent(
                    direction=TelemetryDirection.INGRESS, queue_name="default"
                ),
                TelemetryEvent(
                    direction=TelemetryDirection.EGRESS, queue_name="default"
                ),
                TelemetryEvent(
                    direction=TelemetryDirection.INGRESS, queue_name="extra"
                ),
            ]
        ]

        task = asyncio.create_task(
            QueueMetricsExporter(backend=backend, metrics=recorder).consume_events()
        )
        await _wait_until(lambda: recorder.calls_named("queue_egress"))
        await _cancel(task)

        assert recorder.calls_named("queue_ingress") == [{"queue_name": "default"}]
        assert recorder.calls_named("queue_egress") == [{"queue_name": "default"}]

    async def test_consume_events__reconnects_after_stream_failure(
        self, monkeypatch, caplog
    ) -> None:
        """Log a dropped stream and reconnect after the delay."""
        monkeypatch.setattr("threadmill.queue_metrics.STREAM_RECONNECT_DELAY_S", 0.0)
        backend = _backend()
        recorder = RecordingMetrics()
        backend.events = [
            RuntimeError("stream dropped"),
            [TelemetryEvent(direction=TelemetryDirection.EGRESS, queue_name="default")],
        ]

        with caplog.at_level(logging.ERROR, logger="threadmill.queue_metrics"):
            task = asyncio.create_task(
                QueueMetricsExporter(backend=backend, metrics=recorder).consume_events()
            )
            await _wait_until(lambda: recorder.calls_named("queue_egress"))
            await _cancel(task)

        assert "Worker telemetry stream failed; reconnecting." in caplog.text
        assert backend.telemetry_calls >= 2

    @pytest.mark.parametrize(
        "error",
        [NotImplementedError("unsupported"), TypeError("bad contract")],
        ids=["not-implemented", "type-error"],
    )
    async def test_consume_events__reraises_contract_errors(
        self, monkeypatch, error
    ) -> None:
        """Surface contract violations instead of reconnecting forever."""
        monkeypatch.setattr("threadmill.queue_metrics.STREAM_RECONNECT_DELAY_S", 0.0)
        backend = _backend()
        backend.events = [error]

        with pytest.raises(type(error), match=str(error)):
            await QueueMetricsExporter(
                backend=backend, metrics=RecordingMetrics()
            ).consume_events()


class TestRun:
    """Tests for concurrent sampling and event consumption."""

    async def test_run__propagates_either_loop_failure(self) -> None:
        """Cancel the sibling loop and propagate a contract failure."""
        backend = _backend()
        backend.telemetry = [NotImplementedError("missing queue stats")]

        with pytest.raises(ExceptionGroup) as exc_info:
            await QueueMetricsExporter(
                backend=backend, metrics=RecordingMetrics()
            ).run()

        assert any(
            isinstance(error, NotImplementedError)
            for error in exc_info.value.exceptions
        )
