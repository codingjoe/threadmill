import argparse
import datetime
import importlib.util
import logging
import math
import re
import signal
from unittest.mock import patch

import pytest
from django.core.management import CommandError, call_command
from django.tasks import default_task_backend, task_backends

from tests.testapp.backends import RecordingBroker
from tests.testapp.metrics import RecordingMetrics
from tests.testapp.tasks import (
    compute_workload,
    io_workload,
    memory_workload,
)
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
)
from threadmill.executor import JsonFormatter, handler
from threadmill.management.commands import threadmill


@pytest.fixture(autouse=True)
def restore_log_formatter():
    """Restore the default JSON log formatter after each test."""
    yield
    handler.setFormatter(JsonFormatter())


def _stats(*, ready: int = 0, running: int = 0) -> QueueStats:
    """Build queue stats with explicit ready and running counts."""
    return QueueStats(
        counts=QueueCounts(
            ready=ready,
            running=running,
            deferred=0,
            successful=0,
            failed=0,
        ),
        rates=QueueRates(interval=datetime.timedelta(seconds=60), ingress=0, egress=0),
    )


def _configure_recording_metrics(monkeypatch):
    """Replace the metrics factory with a recording recorder and capture calls."""
    recorder = RecordingMetrics(events=RecordingBroker.events)
    calls = []

    def configure(*, backend_alias=None):
        calls.append(backend_alias)
        return recorder

    monkeypatch.setattr(threadmill, "configure_metrics", configure)
    return recorder, calls


class TestKillSoftly:
    def test_kill_softly__raise_keyboard_interrupt_with_signal_name(self):
        """Raise KeyboardInterrupt with signal metadata in message."""
        with pytest.raises(KeyboardInterrupt, match="SIGINT"):
            threadmill.kill_softly(signal.SIGINT, None)


class TestCommand:
    def test_add_arguments__register_all_worker_options(self):
        """Register command arguments for worker runtime configuration."""
        parser = argparse.ArgumentParser()

        threadmill.WorkerCommand().add_arguments(parser)
        parsed_arguments = parser.parse_args([])

        assert parsed_arguments.backend == "default"
        assert parsed_arguments.queues == ["default"]
        assert parsed_arguments.threads == 1
        assert parsed_arguments.max_tasks == 0
        assert parsed_arguments.max_tasks_jitter == 0
        assert parsed_arguments.poll_interval == 0.01
        assert parsed_arguments.poll_max_interval == 1
        assert parsed_arguments.prefetch_count is None
        assert parsed_arguments.log_format is None

    def test_add_arguments__parse_poll_intervals_as_floats(self):
        """Parse poll options as floats."""
        parser = argparse.ArgumentParser()
        threadmill.WorkerCommand().add_arguments(parser)
        parsed_arguments = parser.parse_args(
            ["--poll-interval", "0.05", "--poll-max-interval", "0.2"]
        )
        assert parsed_arguments.poll_interval == 0.05
        assert parsed_arguments.poll_max_interval == 0.2

    def test_add_arguments__parse_prefetch_count(self):
        """Parse the prefetch count option as an int."""
        parser = argparse.ArgumentParser()
        threadmill.WorkerCommand().add_arguments(parser)
        parsed_arguments = parser.parse_args(["--prefetch-count", "8"])
        assert parsed_arguments.prefetch_count == 8

    def test_call_command__log_format(self):
        """Run the worker with the given log format string."""
        call_command(
            "threadmill",
            "worker",
            verbosity=0,
            workers=1,
            exit_empty=True,
            log_format="%(levelname)s %(message)s",
        )
        record = logging.LogRecord(
            "threadmill", logging.INFO, __file__, 1, "Hello %s", ("world",), None
        )
        assert handler.formatter.format(record) == "INFO Hello world"

    @pytest.mark.parametrize("log_format", ["100% done", "%(message)s 100% done", 123])
    def test_call_command__log_format__raise_command_error(self, log_format):
        """Raise CommandError for a log format that fails to format a record."""
        with pytest.raises(
            CommandError, match=re.escape(f"Invalid log format: {log_format!r}")
        ):
            call_command(
                "threadmill",
                "worker",
                verbosity=0,
                workers=1,
                exit_empty=True,
                log_format=log_format,
            )

    def test_call_command__log_format__defaults_to_json(self):
        """Default to the JSON log formatter."""
        handler.setFormatter(logging.Formatter("%(message)s"))
        call_command(
            "threadmill",
            "worker",
            verbosity=0,
            workers=1,
            exit_empty=True,
        )
        assert isinstance(handler.formatter, JsonFormatter)

    def test_call_command__log_format__empty_string(self):
        """Honor an explicitly empty log format string."""
        handler.setFormatter(JsonFormatter())
        call_command(
            "threadmill",
            "worker",
            verbosity=0,
            workers=1,
            exit_empty=True,
            log_format="",
        )
        record = logging.LogRecord(
            "threadmill", logging.INFO, __file__, 1, "Hello %s", ("world",), None
        )
        assert handler.formatter.format(record) == "Hello world"

    def test_call_command__prefetch_count(self):
        """Pass the prefetch count through to the running task executor."""
        with patch.object(threadmill.TaskExecutor, "run", autospec=True) as run:
            call_command(
                "threadmill",
                "worker",
                verbosity=0,
                prefetch_count=7,
            )
        assert run.call_args.args[0].prefetch_count == 7

    def test_call_command__prefetch_count__raise_command_error(self):
        """Reject a negative prefetch count with a CommandError."""
        with pytest.raises(CommandError, match=re.escape("Invalid prefetch count: -1")):
            call_command(
                "threadmill",
                "worker",
                verbosity=0,
                prefetch_count=-1,
            )

    def test_call_command__poll_intervals(self):
        """Convert poll options to timedeltas for the task executor."""
        with patch.object(threadmill.TaskExecutor, "run", autospec=True) as run:
            call_command(
                "threadmill",
                "worker",
                verbosity=0,
                poll_interval=0.05,
                poll_max_interval=0.2,
            )
        executor = run.call_args.args[0]
        assert executor.poll_interval == datetime.timedelta(seconds=0.05)
        assert executor.poll_max_interval == datetime.timedelta(seconds=0.2)

    def test_call_command__poll_intervals__default_to_backend_defaults(self):
        """Pass the backend default poll options to the task executor by default."""
        with patch.object(threadmill.TaskExecutor, "run", autospec=True) as run:
            call_command("threadmill", "worker", verbosity=0)
        executor = run.call_args.args[0]
        assert executor.poll_interval == datetime.timedelta(seconds=0.01)
        assert executor.poll_max_interval == datetime.timedelta(seconds=1)

    @pytest.mark.benchmark
    def test_call_command__benchmark_compute(
        self,
        benchmark,
    ):
        """Benchmark command execution for compute tasks."""
        backend = default_task_backend
        for _ in range(100):
            backend.enqueue(compute_workload, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                "worker",
                verbosity=0,
                queues=["compute"],
                exit_empty=True,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

    @pytest.mark.benchmark
    def test_call_command__benchmark_io(
        self,
        benchmark,
    ):
        """Benchmark command execution for IO tasks."""
        backend = default_task_backend
        for _ in range(100):
            backend.enqueue(io_workload, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                "worker",
                verbosity=0,
                queues=["io"],
                threads=6,
                exit_empty=True,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

    @pytest.mark.benchmark
    def test_call_command__benchmark_compute_and_io(
        self,
        benchmark,
    ):
        """Benchmark command execution for compute and IO tasks."""
        backend = default_task_backend
        for _ in range(100):
            backend.enqueue(compute_workload, args=[])
            backend.enqueue(io_workload, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                "worker",
                verbosity=0,
                queues=["compute", "io"],
                exit_empty=True,
                threads=2,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

    @pytest.mark.benchmark
    def test_call_command__benchmark_memory_leak_recovery(
        self,
        benchmark,
    ):
        """Benchmark command execution for memory leak recovery."""
        backend = default_task_backend
        for _ in range(1000):
            backend.enqueue(memory_workload, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                "worker",
                verbosity=0,
                queues=["memory"],
                exit_empty=True,
                max_tasks=10,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

    @pytest.mark.benchmark
    def test_call_command__benchmark_default_queue(self, benchmark):
        """Benchmark command execution for default queue tasks."""
        backend = default_task_backend
        task = default_task_backend.task_class(
            func=compute_workload.func, queue_name="default"
        )
        for _ in range(100):
            backend.enqueue(task, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                "worker",
                verbosity=0,
                exit_empty=True,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )


class TestInspectorCommand:
    def test_add_arguments__registers_defaults(self):
        """InspectorCommand registers the backend option."""
        pytest.importorskip("textual.widgets")
        parser = argparse.ArgumentParser()
        threadmill.InspectorCommand().add_arguments(parser)
        args = parser.parse_args([])
        assert args.backend == "default"

    def test_handle__raises_command_error_for_invalid_backend(self):
        """An unknown backend alias raises CommandError."""
        pytest.importorskip("textual.widgets")
        with pytest.raises(CommandError, match="Invalid backend"):
            threadmill.InspectorCommand().handle(backend="nonexistent")

    def test_handle__raises_command_error_for_unsupported_backend(self):
        """A backend without ThreadmillTaskBackend support raises CommandError."""
        pytest.importorskip("textual.widgets")
        with pytest.raises(CommandError, match="does not support inspection"):
            threadmill.InspectorCommand().handle(backend="immediate")

    def test_handle__launches_inspector_app(self):
        """A supported backend launches the inspector app."""
        pytest.importorskip("textual.widgets")
        from threadmill.inspector.app import InspectorApp

        with patch.object(InspectorApp, "run"):
            threadmill.InspectorCommand().handle(backend="default")


class TestRequireThreadmillBackend:
    """Tests for the shared backend resolver."""

    def test_require_threadmill_backend__returns_configured_backend(self):
        """Return the backend registered under the alias."""
        backend = threadmill.require_threadmill_backend("default", feature="test")
        assert backend.alias == "default"

    def test_require_threadmill_backend__rejects_invalid_alias(self):
        """Turn an unknown alias into a command error naming it."""
        with pytest.raises(CommandError, match="Invalid backend: 'nope'"):
            threadmill.require_threadmill_backend("nope", feature="test")

    def test_require_threadmill_backend__rejects_other_backend(self):
        """Turn a non-threadmill backend into a command error naming the feature."""
        with pytest.raises(CommandError, match="does not support test"):
            threadmill.require_threadmill_backend("immediate", feature="test")


class TestInstallSignalHandlers:
    """Tests for the platform signal installer."""

    def test_install_signal_handlers__posix(self, monkeypatch):
        """Install HUP, TERM, and INT handlers on POSIX."""
        calls = []
        monkeypatch.setattr(
            threadmill.signal,
            "signal",
            lambda signum, handler: calls.append((signum, handler)),
        )
        monkeypatch.setattr(threadmill.sys, "platform", "linux")

        threadmill.install_signal_handlers()

        assert calls == [
            (signal.SIGHUP, threadmill.kill_softly),
            (signal.SIGTERM, threadmill.kill_softly),
            (signal.SIGINT, threadmill.kill_softly),
        ]

    def test_install_signal_handlers__windows(self, monkeypatch):
        """Install BREAK, TERM, and INT handlers on Windows."""
        calls = []
        monkeypatch.setattr(
            threadmill.signal,
            "signal",
            lambda signum, handler: calls.append((signum, handler)),
        )
        monkeypatch.setattr(threadmill.signal, "SIGBREAK", 21, raising=False)
        monkeypatch.setattr(threadmill.sys, "platform", "win32")

        threadmill.install_signal_handlers()

        assert calls == [
            (21, threadmill.kill_softly),
            (signal.SIGTERM, threadmill.kill_softly),
            (signal.SIGINT, threadmill.kill_softly),
        ]


class TestOpenTelemetryCommand:
    """Tests for the otel subcommand."""

    @pytest.fixture(autouse=True)
    def reset_telemetry_doubles(self):
        """Reset broker knobs and scripted backend state around each test."""
        RecordingBroker.instances.clear()
        RecordingBroker.events.clear()
        RecordingBroker.alive_after_shutdown = True
        RecordingBroker.fail_shutdown = False
        RecordingBroker.fail_join = False
        for alias in ("telemetry", "telemetry-no-broker"):
            backend = task_backends[alias]
            backend.telemetry = []
            backend.events = []
        yield
        RecordingBroker.instances.clear()
        RecordingBroker.events.clear()
        RecordingBroker.alive_after_shutdown = True
        RecordingBroker.fail_shutdown = False
        RecordingBroker.fail_join = False

    def test_add_arguments__registers_defaults(self):
        """Register the backend, interval, and once defaults."""
        parser = argparse.ArgumentParser()
        threadmill.OpenTelemetryCommand().add_arguments(parser)

        args = parser.parse_args([])

        assert args.backend == "default"
        assert args.interval == 15.0
        assert args.once is False

        args = parser.parse_args(["-b", "telemetry", "--interval", "2.5", "--once"])
        assert args.backend == "telemetry"
        assert args.interval == 2.5
        assert args.once is True

    @pytest.mark.parametrize("interval", [math.nan, math.inf, -math.inf, 0.0, -1.0])
    def test_handle__rejects_invalid_interval(self, interval):
        """Reject non-finite and non-positive sampling intervals."""
        with pytest.raises(CommandError, match="Invalid interval"):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=interval, once=True
            )

    def test_handle__rejects_invalid_backend(self):
        """Reject an unknown backend alias."""
        with pytest.raises(CommandError, match="Invalid backend"):
            threadmill.OpenTelemetryCommand().handle(
                backend="nope", interval=1.0, once=True
            )

    def test_handle__rejects_unsupported_backend(self):
        """Reject a backend that does not support queue telemetry."""
        with pytest.raises(CommandError, match="does not support queue telemetry"):
            threadmill.OpenTelemetryCommand().handle(
                backend="immediate", interval=1.0, once=True
            )

    def test_handle__rejects_missing_sdk(self, monkeypatch):
        """Require the optional SDK before configuring the exporter."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        original_find_spec = importlib.util.find_spec

        def find_spec(name, *args, **kwargs):
            if name == "opentelemetry.sdk.metrics":
                return None
            return original_find_spec(name, *args, **kwargs)

        monkeypatch.setattr(importlib.util, "find_spec", find_spec)
        with pytest.raises(CommandError, match="OpenTelemetry SDK is not installed"):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=1.0, once=True
            )

    @pytest.mark.parametrize(
        ("name", "value"),
        [("OTEL_METRICS_EXPORTER", "none"), ("OTEL_SDK_DISABLED", "true")],
    )
    def test_handle__rejects_disabled_export(self, monkeypatch, name, value):
        """Reject disabled telemetry with the enabling instructions."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
        monkeypatch.setenv(name, value)
        with pytest.raises(CommandError, match="Metrics export is disabled"):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=1.0, once=True
            )

    def test_handle__rejects_backend_without_telemetry(self, monkeypatch):
        """Reject a backend without telemetry before configuring metrics."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        calls = []

        def configure(*, backend_alias=None):
            calls.append(backend_alias)
            return RecordingMetrics()

        monkeypatch.setattr(threadmill, "configure_metrics", configure)
        with pytest.raises(CommandError, match="does not implement queue_stats"):
            threadmill.OpenTelemetryCommand().handle(
                backend="stub", interval=1.0, once=True
            )
        assert calls == []

    def test_handle__once_samples_without_signals_or_broker(self, monkeypatch):
        """Sample once, flush metrics, and leave signals and brokers untouched."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, calls = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [
            BackendTelemetry(queues={"default": _stats(ready=2, running=1)})
        ]
        before = signal.getsignal(signal.SIGTERM)

        threadmill.OpenTelemetryCommand().handle(
            backend="telemetry", interval=5.0, once=True
        )

        assert calls == ["telemetry"]
        assert recorder.calls_named("queue_backlog") == [
            {"queue_name": "default", "count": 3}
        ]
        assert recorder.shutdown_called
        assert RecordingBroker.instances == []
        assert signal.getsignal(signal.SIGTERM) is before

    def test_handle__continuous_starts_and_stops_broker(self, monkeypatch):
        """Run the broker and flush metrics before propagating a loop failure."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [NotImplementedError("queue stats unavailable")]

        with pytest.raises(ExceptionGroup):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=5.0, once=False
            )

        (broker,) = RecordingBroker.instances
        assert broker.started
        assert broker.shutdown_called
        assert broker.join_called
        assert recorder.shutdown_called
        assert RecordingBroker.events == [
            "broker.start",
            "broker.shutdown",
            "broker.join",
            "metrics.shutdown",
        ]

    def test_handle__continuous_without_broker_skips_lifecycle(self, monkeypatch):
        """Skip broker lifecycle when the backend declares no broker."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry-no-broker"]
        backend.telemetry = [NotImplementedError("queue stats unavailable")]

        with pytest.raises(ExceptionGroup):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry-no-broker", interval=5.0, once=False
            )

        assert RecordingBroker.instances == []
        assert recorder.shutdown_called

    def test_handle__broker_shutdown_failure_still_flushes_metrics(self, monkeypatch):
        """Flush metrics even when broker shutdown fails."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [NotImplementedError("queue stats unavailable")]
        RecordingBroker.fail_shutdown = True

        with pytest.raises(RuntimeError, match="broker shutdown failed"):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=5.0, once=False
            )

        assert recorder.shutdown_called

    def test_handle__broker_join_failure_still_flushes_metrics(self, monkeypatch):
        """Flush metrics even when broker join fails."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [NotImplementedError("queue stats unavailable")]
        RecordingBroker.fail_join = True

        with pytest.raises(RuntimeError, match="broker join failed"):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=5.0, once=False
            )

        assert recorder.shutdown_called

    def test_handle__dead_broker_skips_join(self, monkeypatch):
        """Skip the join when shutdown already stopped the broker."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [NotImplementedError("queue stats unavailable")]
        RecordingBroker.alive_after_shutdown = False

        with pytest.raises(ExceptionGroup):
            threadmill.OpenTelemetryCommand().handle(
                backend="telemetry", interval=5.0, once=False
            )

        (broker,) = RecordingBroker.instances
        assert broker.shutdown_called
        assert not broker.join_called
        assert recorder.shutdown_called

    def test_handle__keyboard_interrupt_flushes_metrics(self, monkeypatch, capsys):
        """Report a graceful stop and flush through the nested finally."""
        monkeypatch.setenv("OTEL_METRICS_EXPORTER", "console")
        recorder, _ = _configure_recording_metrics(monkeypatch)
        backend = task_backends["telemetry"]
        backend.telemetry = [KeyboardInterrupt("stop requested")]

        threadmill.OpenTelemetryCommand().handle(
            backend="telemetry", interval=5.0, once=True
        )

        assert "stop requested" in capsys.readouterr().out
        assert recorder.shutdown_called


class TestParentCommand:
    def test_add_arguments__registers_subcommands(self):
        """The parent command registers worker and inspector subparsers."""
        parser = argparse.ArgumentParser()
        threadmill.Command().add_arguments(parser)
        assert parser.parse_args(["worker"]).subcommand == "worker"
        assert parser.parse_args(["inspector"]).subcommand == "inspector"

    def test_add_arguments__registers_otel_subcommand(self):
        """The parent command registers the otel subparser."""
        parser = argparse.ArgumentParser()
        threadmill.Command().add_arguments(parser)
        assert parser.parse_args(["otel"]).subcommand == "otel"

    def test_handle__dispatches_to_subcommand(self):
        """The parent command dispatches to the selected subcommand in-process."""
        with patch.object(threadmill.WorkerCommand, "execute") as execute:
            threadmill.Command().handle(
                subcommand="worker", queues=["compute"], exit_empty=True
            )
        execute.assert_called_once()
        _, kwargs = execute.call_args
        assert kwargs["queues"] == ["compute"]
        assert kwargs["exit_empty"] is True
