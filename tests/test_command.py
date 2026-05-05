from __future__ import annotations

import argparse
import signal

import pytest
from django.core.management import call_command
from django.tasks import default_task_backend
from threadmill.management.commands import threadmill


class TestKillSoftly:
    def test_kill_softly__raise_keyboard_interrupt_with_signal_name(self) -> None:
        """Raise KeyboardInterrupt with signal metadata in message."""
        with pytest.raises(KeyboardInterrupt, match="SIGINT"):
            threadmill.kill_softly(signal.SIGINT, None)


class TestCommand:
    def test_add_arguments__register_all_worker_options(self) -> None:
        """Register command arguments for worker runtime configuration."""
        parser = argparse.ArgumentParser()

        threadmill.Command().add_arguments(parser)
        parsed_arguments = parser.parse_args([])

        assert parsed_arguments.backends == "default"
        assert parsed_arguments.queues == "default"
        assert parsed_arguments.threads == 1
        assert parsed_arguments.max_tasks == 0
        assert parsed_arguments.max_tasks_jitter == 0
        assert parsed_arguments.task_timeout == 3600.0

    @pytest.mark.benchmark
    def test_call_command__benchmark_compute(
        self,
        benchmark,
    ) -> None:
        """Benchmark command execution for one CPU intense task solved 100 times."""
        default_task_backend.reset()
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

        assert default_task_backend.solved_task_count == 100

    @pytest.mark.benchmark
    def test_call_command__benchmark_io(
        self,
        benchmark,
    ) -> None:
        """Benchmark command execution for one CPU intense task solved 100 times."""
        default_task_backend.reset()
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

        assert default_task_backend.solved_task_count == 100

    @pytest.mark.benchmark
    def test_call_command__benchmark_compute_and_io(
        self,
        benchmark,
    ) -> None:
        """Benchmark command execution for one CPU intense task solved 100 times."""
        default_task_backend.reset()
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

        assert default_task_backend.solved_task_count == 200

    @pytest.mark.benchmark
    def test_call_command__benchmark_memory_leak_recovery(
        self,
        benchmark,
    ) -> None:
        """Benchmark command execution for one CPU intense task solved 100 times."""
        default_task_backend.reset(1000)
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
