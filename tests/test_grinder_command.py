from __future__ import annotations

import argparse
import signal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from grinder.management.commands import grinder


class TaskExecutorDouble:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.run_called = False
        self.shutdown_called = False
        self.exception: Exception | None = None

    def run(self) -> None:
        self.run_called = True
        if self.exception is not None:
            raise self.exception

    def shutdown(self) -> None:
        self.shutdown_called = True


class TestKillSoftly:
    def test_kill_softly__raise_keyboard_interrupt_with_signal_name(self) -> None:
        """Raise KeyboardInterrupt with signal metadata in message."""
        with pytest.raises(KeyboardInterrupt, match="SIGINT"):
            grinder.kill_softly(signal.SIGINT, None)


class TestCommand:
    def test_add_arguments__register_all_worker_options(self) -> None:
        """Register command arguments for worker runtime configuration."""
        parser = argparse.ArgumentParser()

        grinder.Command().add_arguments(parser)
        parsed_arguments = parser.parse_args([])

        assert parsed_arguments.backends == "default"
        assert parsed_arguments.queues == "default"
        assert parsed_arguments.threads == 1
        assert parsed_arguments.max_tasks == 0
        assert parsed_arguments.max_tasks_jitter == 0
        assert parsed_arguments.task_timeout == 3600.0

    def test_handle__initialize_and_run_task_executor(self, monkeypatch) -> None:
        """Initialize executor and run worker loop with backend alias."""
        signal_call = Mock()
        monkeypatch.setattr(grinder.signal, "signal", signal_call)
        monkeypatch.setattr(grinder, "task_backends", {"default": "backend"})

        task_executor = TaskExecutorDouble()

        def create_task_executor(**kwargs):
            task_executor.kwargs = kwargs
            return task_executor

        monkeypatch.setattr(grinder, "TaskExecutor", create_task_executor)

        command = grinder.Command()
        command.handle(
            verbosity=1,
            backends=["default"],
            queues=["default"],
            workers=2,
            threads=3,
            max_tasks=5,
            max_tasks_jitter=1,
            task_timeout=33.0,
        )

        assert task_executor.run_called is True
        assert task_executor.kwargs == {
            "backend": "backend",
            "workers": 2,
            "threads": 3,
            "max_tasks": 5,
            "max_tasks_jitter": 1,
            "task_timeout": 33.0,
        }
        assert signal_call.call_count == 3

    def test_handle__register_sigbreak_on_windows(self, monkeypatch) -> None:
        """Register SIGBREAK handler when running on Windows."""
        signal_call = Mock()
        monkeypatch.setattr(grinder.signal, "signal", signal_call)
        monkeypatch.setattr(grinder.signal, "SIGBREAK", signal.SIGTERM, raising=False)
        monkeypatch.setattr(grinder.sys, "platform", "win32")
        monkeypatch.setattr(grinder, "task_backends", {"default": "backend"})
        monkeypatch.setattr(
            grinder, "TaskExecutor", lambda **kwargs: TaskExecutorDouble(**kwargs)
        )

        grinder.Command().handle(
            verbosity=1,
            backends="default",
            queues=["default"],
            workers=1,
            threads=1,
            max_tasks=0,
            max_tasks_jitter=0,
            task_timeout=10.0,
        )

        assert signal_call.call_args_list[0].args[0] == signal.SIGBREAK

    def test_handle__shutdown_executor_on_keyboard_interrupt(self, monkeypatch) -> None:
        """Shut down executor when worker loop receives keyboard interrupt."""
        monkeypatch.setattr(grinder.signal, "signal", Mock())
        monkeypatch.setattr(grinder, "task_backends", {"default": "backend"})

        task_executor = TaskExecutorDouble()
        task_executor.exception = KeyboardInterrupt("stop")
        monkeypatch.setattr(grinder, "TaskExecutor", lambda **kwargs: task_executor)

        output = []

        def write(value: str) -> None:
            output.append(value)

        command = grinder.Command()
        command.stdout = SimpleNamespace(write=write)
        command.handle(
            verbosity=1,
            backends="default",
            queues=["default"],
            workers=1,
            threads=1,
            max_tasks=0,
            max_tasks_jitter=0,
            task_timeout=10.0,
        )

        assert task_executor.shutdown_called is True
        assert any("Shutting down scheduler" in message for message in output)
