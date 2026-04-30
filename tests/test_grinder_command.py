from __future__ import annotations

import argparse
import signal

import pytest
from django.core.management import call_command
from grinder.management.commands import grinder

from tests.testapp.backends import CPUHeavyTaskBackend


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

    @pytest.mark.benchmark
    def test_call_command__benchmark_cpu_intense_task_1000_times(
        self,
        benchmark,
    ) -> None:
        """Benchmark command execution for one CPU intense task solved 1000 times."""
        benchmark.pedantic(
            lambda: call_command(
                "grinder",
                verbosity=0,
                backends="cpu",
                queues=["default"],
                exit_empty=True,
            ),
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

        assert CPUHeavyTaskBackend.solved_task_count == 100
