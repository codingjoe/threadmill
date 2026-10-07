import argparse
import datetime
import logging
import re
import signal
from unittest.mock import patch

import pytest
from django.core.management import CommandError, call_command
from django.tasks import default_task_backend

from tests.testapp.tasks import (
    compute_workload,
    io_workload,
    memory_workload,
)
from threadmill.executor import JsonFormatter, handler
from threadmill.management.commands import threadmill


@pytest.fixture(autouse=True)
def restore_log_formatter():
    """Restore the default JSON log formatter after each test."""
    yield
    handler.setFormatter(JsonFormatter())


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


class TestParentCommand:
    def test_add_arguments__registers_subcommands(self):
        """The parent command registers worker and inspector subparsers."""
        parser = argparse.ArgumentParser()
        threadmill.Command().add_arguments(parser)
        assert parser.parse_args(["worker"]).subcommand == "worker"
        assert parser.parse_args(["inspector"]).subcommand == "inspector"

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
