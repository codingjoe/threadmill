"""Tests for worker telemetry data types and the psutil sampler."""

from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock, patch

import pytest

from threadmill.backends.base import (
    NodeTelemetry,
    ThreadmillTaskBackend,
    WorkerProcessTelemetry,
    WorkerTelemetry,
)
from threadmill.telemetry import TelemetrySampler

utc = datetime.UTC


def _make_worker_telemetry(
    *,
    hostname: str = "node-1",
    worker_name: str = "node-1:1234-0",
    sampled_at: datetime.datetime | None = None,
) -> WorkerTelemetry:
    """Build a minimal WorkerTelemetry snapshot for tests."""
    sampled_at = sampled_at or datetime.datetime.now(tz=utc)
    worker = WorkerProcessTelemetry(
        name=worker_name,
        pid=1234,
        queues=("default",),
        thread_count=2,
        task_count=10,
        tasks_per_minute=30.0,
        sampled_at=sampled_at,
    )
    node = NodeTelemetry(
        hostname=hostname,
        queues=("default",),
        process_count=1,
        thread_count=2,
        cpu_percent=45.0,
        memory_percent=60.0,
        memory_bytes=8_000_000_000,
        tasks_per_minute=30.0,
        workers={worker_name: worker},
        sampled_at=sampled_at,
    )
    return WorkerTelemetry(
        nodes={hostname: node},
        queues={"default": (hostname,)},
        sampled_at=sampled_at,
    )


class TestWorkerTelemetryDataTypes:
    """Tests for the telemetry dataclasses defined in backends/base.py."""

    def test_worker_process_telemetry_fields(self):
        """WorkerProcessTelemetry stores all expected fields."""
        now = datetime.datetime.now(tz=utc)
        worker = WorkerProcessTelemetry(
            name="host:100-0",
            pid=100,
            queues=("default",),
            thread_count=4,
            task_count=50,
            tasks_per_minute=12.0,
            sampled_at=now,
        )
        assert worker.name == "host:100-0"
        assert worker.pid == 100
        assert worker.queues == ("default",)
        assert worker.thread_count == 4
        assert worker.task_count == 50
        assert worker.tasks_per_minute == 12.0
        assert worker.sampled_at == now

    def test_node_telemetry_fields(self):
        """NodeTelemetry stores workers dict and node-level metrics."""
        now = datetime.datetime.now(tz=utc)
        worker = WorkerProcessTelemetry(
            name="host:100-0",
            pid=100,
            queues=("default",),
            thread_count=1,
            task_count=0,
            tasks_per_minute=0.0,
            sampled_at=now,
        )
        node = NodeTelemetry(
            hostname="host",
            queues=("default",),
            process_count=1,
            thread_count=1,
            cpu_percent=20.0,
            memory_percent=50.0,
            memory_bytes=4_000_000_000,
            tasks_per_minute=0.0,
            workers={"host:100-0": worker},
            sampled_at=now,
        )
        assert node.hostname == "host"
        assert node.process_count == 1
        assert node.thread_count == 1
        assert node.cpu_percent == 20.0
        assert node.memory_percent == 50.0
        assert node.memory_bytes == 4_000_000_000
        assert "host:100-0" in node.workers

    def test_worker_telemetry_queues_and_nodes(self):
        """WorkerTelemetry maps queues to hostnames and nodes to NodeTelemetry."""
        now = datetime.datetime.now(tz=utc)
        worker = WorkerProcessTelemetry(
            name="h:1-0",
            pid=1,
            queues=("default",),
            thread_count=1,
            task_count=0,
            tasks_per_minute=0.0,
            sampled_at=now,
        )
        node = NodeTelemetry(
            hostname="h",
            queues=("default",),
            process_count=1,
            thread_count=1,
            cpu_percent=0.0,
            memory_percent=0.0,
            memory_bytes=0,
            tasks_per_minute=0.0,
            workers={"h:1-0": worker},
            sampled_at=now,
        )
        telemetry = WorkerTelemetry(
            nodes={"h": node},
            queues={"default": ("h",)},
            sampled_at=now,
        )
        assert "h" in telemetry.nodes
        assert telemetry.queues["default"] == ("h",)
        assert telemetry.sampled_at == now


class TestBaseBackendHooks:
    """Tests for the default backend hooks."""

    class _ConcreteBackend(ThreadmillTaskBackend):
        """Minimal concrete backend for hook testing."""

        def enqueue(self, task, args, kwargs):
            raise NotImplementedError

    def test_publish_worker_telemetry_is_noop(self):
        """The base publish hook does nothing and does not raise."""
        backend = self._ConcreteBackend(alias="test", params={})
        backend.publish_worker_telemetry(_make_worker_telemetry())

    def test_worker_telemetry_returns_empty_snapshot(self):
        """The base worker_telemetry hook returns an empty but valid snapshot."""
        backend = self._ConcreteBackend(alias="test", params={})
        snapshot = backend.worker_telemetry()
        assert snapshot.nodes == {}
        assert snapshot.queues == {}
        assert snapshot.sampled_at is not None


class TestTelemetrySampler:
    """Tests for the TelemetrySampler with stubbed psutil."""

    def _make_worker_process(self):
        """Build a fake worker process with the attributes the sampler reads."""
        proc = MagicMock()
        proc.name = "testhost:100-0"
        proc.queues = ("default",)
        proc.thread_count = 2
        proc.task_count = 5
        proc.pid = 100
        return proc

    def test_is_available_when_psutil_imported(self):
        """is_available is True when psutil is importable."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
        )
        # psutil is installed in the test environment
        assert sampler.is_available is True

    def test_sample_returns_worker_telemetry(self):
        """sample() builds a WorkerTelemetry with node and worker data."""
        fake_vmem = MagicMock()
        fake_vmem.percent = 55.0
        fake_vmem.total = 8_000_000_000

        worker_process = self._make_worker_process()
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=worker_process,
        )

        with (
            patch("threadmill.telemetry.psutil") as fake_psutil,
            patch("threadmill.telemetry.socket.gethostname", return_value="testhost"),
        ):
            fake_psutil.Process.return_value = MagicMock(pid=100)
            fake_psutil.cpu_percent.return_value = 42.0
            fake_psutil.virtual_memory.return_value = fake_vmem

            snapshot = sampler.sample()

        assert "testhost" in snapshot.nodes
        node = snapshot.nodes["testhost"]
        assert node.hostname == "testhost"
        assert node.cpu_percent == 42.0
        assert node.memory_percent == 55.0
        assert node.memory_bytes == 8_000_000_000
        assert node.process_count == 1
        assert node.thread_count == 2
        assert "testhost:100-0" in node.workers
        worker = node.workers["testhost:100-0"]
        assert worker.pid == 100
        assert worker.task_count == 5
        assert "default" in snapshot.queues
        assert "testhost" in snapshot.queues["default"]

    def test_tasks_per_minute_first_call_returns_zero(self):
        """The first throughput sample returns 0 (no previous baseline)."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
            clock=lambda: 100.0,
        )
        assert sampler._tasks_per_minute(10, 100.0) == 0.0

    def test_tasks_per_minute_computes_delta(self):
        """Throughput is (delta_tasks / elapsed_seconds) * 60."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
            clock=lambda: 100.0,
        )
        sampler._tasks_per_minute(10, 100.0)
        rate = sampler._tasks_per_minute(25, 130.0)
        assert rate == pytest.approx(30.0)

    def test_tasks_per_minute_zero_elapsed_returns_zero(self):
        """When elapsed time is zero, throughput returns 0 to avoid division by zero."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
            clock=lambda: 100.0,
        )
        sampler._tasks_per_minute(10, 100.0)
        rate = sampler._tasks_per_minute(20, 100.0)
        assert rate == 0.0

    def test_start_without_psutil_is_noop(self):
        """start() does nothing when psutil is not available."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
        )
        with patch("threadmill.telemetry.psutil", None):
            sampler.start()
        assert sampler._thread is None

    def test_stop_without_start_is_noop(self):
        """stop() does not raise when the sampler was never started."""
        sampler = TelemetrySampler(
            backend=MagicMock(),
            worker_process=self._make_worker_process(),
        )
        sampler.stop()

    def test_publish_one_sample_calls_backend(self):
        """_publish_one_sample calls backend.publish_worker_telemetry."""
        backend = MagicMock()
        worker_process = self._make_worker_process()
        sampler = TelemetrySampler(
            backend=backend,
            worker_process=worker_process,
        )

        fake_vmem = MagicMock()
        fake_vmem.percent = 40.0
        fake_vmem.total = 4_000_000_000

        with (
            patch("threadmill.telemetry.psutil") as fake_psutil,
            patch("threadmill.telemetry.socket.gethostname", return_value="h"),
        ):
            fake_psutil.Process.return_value = MagicMock(pid=100)
            fake_psutil.cpu_percent.return_value = 10.0
            fake_psutil.virtual_memory.return_value = fake_vmem
            sampler._publish_one_sample()

        backend.publish_worker_telemetry.assert_called_once()
        snapshot = backend.publish_worker_telemetry.call_args.args[0]
        assert isinstance(snapshot, WorkerTelemetry)
        assert "h" in snapshot.nodes

    def test_run_logs_exception_on_sample_failure(self, caplog):
        """_run logs an exception when _publish_one_sample raises."""
        backend = MagicMock()
        backend.publish_worker_telemetry.side_effect = RuntimeError("boom")
        worker_process = self._make_worker_process()
        sampler = TelemetrySampler(
            backend=backend,
            worker_process=worker_process,
            interval_seconds=0.01,
        )

        fake_vmem = MagicMock()
        fake_vmem.percent = 40.0
        fake_vmem.total = 4_000_000_000

        with (
            patch("threadmill.telemetry.psutil") as fake_psutil,
            patch("threadmill.telemetry.socket.gethostname", return_value="h"),
        ):
            fake_psutil.Process.return_value = MagicMock(pid=100)
            fake_psutil.cpu_percent.return_value = 10.0
            fake_psutil.virtual_memory.return_value = fake_vmem
            with caplog.at_level(logging.ERROR):
                sampler.start()
                import time as _time

                _time.sleep(0.1)
                sampler.stop()

        assert "Failed to sample worker telemetry" in caplog.text
