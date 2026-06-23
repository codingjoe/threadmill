from __future__ import annotations

import argparse
import signal

import pytest
from django.core.management import call_command
from django.tasks import Task, default_task_backend

from tests.testapp.tasks import compute_workload, io_workload, memory_workload
from threadmill.management.commands import threadmill


class TestKillSoftly:
    def test_kill_softly__raise_keyboard_interrupt_with_signal_name(self):
        """Raise KeyboardInterrupt with signal metadata in message."""
        with pytest.raises(KeyboardInterrupt, match="SIGINT"):
            threadmill.kill_softly(signal.SIGINT, None)


class TestCommand:
    def test_add_arguments__register_all_worker_options(self):
        """Register command arguments for worker runtime configuration."""
        parser = argparse.ArgumentParser()

        threadmill.Command().add_arguments(parser)
        parsed_arguments = parser.parse_args([])

        assert parsed_arguments.backend == "default"
        assert parsed_arguments.queues == ["default"]
        assert parsed_arguments.threads == 1
        assert parsed_arguments.max_tasks == 0
        assert parsed_arguments.max_tasks_jitter == 0

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
        task = Task(func=compute_workload.func, queue_name="default")
        for _ in range(100):
            backend.enqueue(task, args=[])

        benchmark.pedantic(
            lambda: call_command(
                "threadmill",
                verbosity=0,
                exit_empty=True,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )
