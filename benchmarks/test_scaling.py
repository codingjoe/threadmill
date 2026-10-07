"""Benchmarks comparing process and thread parallelism for CPU-bound tasks.

The backend comparison measures queue overhead with a trivial echo task, which
says nothing about the parallelism a worker pool actually delivers. Only a
CPU-bound task can show that, so this module drains the same fixed workload with
several worker process and thread counts and reports how long each takes.

Run both interpreters and compare the ``(1, 1)`` baseline against the threaded
configurations:

    uv run pytest benchmarks/test_scaling.py -m benchmark --benchmark-json=scaling.json
    uv run --python 3.14t pytest benchmarks/test_scaling.py -m benchmark --benchmark-json=scaling-free-threaded.json

On a GIL build extra threads cannot shorten the drain, because only one thread
runs Python at a time. On a free-threaded build they can. Every measurement
includes the worker pool's fixed start cost of one to two seconds, which is a
larger share of the faster configurations, so compare each configuration against
the ``(1, 1)`` baseline instead of reading the times as pure throughput.

Measured on 16 CPU-bound tasks of about a second each, worker start cost included.
On a free-threaded build one process with four threads reaches the same
throughput as four processes with one thread each, about 2.7x, and four
processes with four threads match them: worker count sets the parallelism, and
threads reach it with less memory. On a GIL build extra threads never help, and
they cost once a pool holds more worker threads in total than the queue holds
tasks. A process requests a prefetch batch sized by its thread count, so one
process can take the whole queue and leave its neighbours idle.

That is why the defaults stay one process per core with a single thread. They
already reach full parallelism on a free-threaded build, and a crash or a task
recycling stays confined to one process. Threads trade that isolation for a
smaller memory and connection footprint, which is a per-deployment choice rather
than a default.
"""

import collections
import dataclasses
import sys

import pytest
from django.core.management import call_command
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    TaskResultStatus,
    task_backends,
)

from tests.testapp.tasks import compute_workload
from threadmill.executor import is_free_threaded_build, is_gil_enabled

TASK_COUNT = 16
"""CPU-bound tasks drained per measurement, about one second of work each."""

MEASUREMENT_ROUNDS = 2
"""Repeats per configuration, reduced to the median by the benchmark plugin."""

SCALING_QUEUE_NAME = "scaling"
"""Queue dedicated to this benchmark.

A separate queue keeps the measurement honest: a worker process left behind by
another test or worktree sharing the same Redis instance would otherwise steal
tasks and report a drain that never did the work.
"""

scaling_workload = dataclasses.replace(compute_workload, queue_name=SCALING_QUEUE_NAME)
"""The CPU-bound workload task, pinned to the benchmark's own queue."""


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class WorkerParallelism:
    """A worker process count and a worker thread count to measure."""

    workers: int
    threads: int

    @property
    def label(self) -> str:
        """Return a short identifier for benchmark reports."""
        return f"{self.workers}p-{self.threads}t"


PARALLELISMS_UNDER_TEST = (
    WorkerParallelism(workers=1, threads=1),
    WorkerParallelism(workers=4, threads=1),
    WorkerParallelism(workers=1, threads=4),
    WorkerParallelism(workers=4, threads=4),
)
"""Configurations spanning process-only, thread-only and mixed parallelism."""


@dataclasses.dataclass(kw_only=True, slots=True)
class CpuWorkload:
    """A fixed CPU-bound workload processed by one worker pool configuration."""

    parallelism: WorkerParallelism
    task_count: int = TASK_COUNT
    enqueued_ids: list[str] = dataclasses.field(default_factory=list, init=False)

    def drain(self) -> None:
        """Queue the workload, then process it with the configured worker pool."""
        queued_task = scaling_workload.using(backend=DEFAULT_TASK_BACKEND_ALIAS)
        self.enqueued_ids.extend(
            queued_task.enqueue().id for _ in range(self.task_count)
        )
        call_command(
            "threadmill",
            "worker",
            backend=DEFAULT_TASK_BACKEND_ALIAS,
            queues=[SCALING_QUEUE_NAME],
            workers=self.parallelism.workers,
            threads=self.parallelism.threads,
            exit_empty=True,
            verbosity=0,
        )

    def verify(self) -> None:
        """Assert every queued task succeeded, so a fast drain cannot be a lost task."""
        backend = task_backends[DEFAULT_TASK_BACKEND_ALIAS]
        statuses = collections.Counter(
            backend.get_result(task_id).status for task_id in self.enqueued_ids
        )
        assert statuses[TaskResultStatus.SUCCESSFUL] == len(self.enqueued_ids), (
            f"drained {statuses[TaskResultStatus.SUCCESSFUL]} of"
            f" {len(self.enqueued_ids)} tasks: {statuses}"
        )


@pytest.fixture
def empty_queues():
    """Delete queued tasks before and after each measurement."""
    client = task_backends[DEFAULT_TASK_BACKEND_ALIAS].client

    def delete_queued_tasks() -> None:
        if keys := client.keys("threadmill:*"):
            client.delete(*keys)

    delete_queued_tasks()
    yield
    delete_queued_tasks()


class TestThreadScaling:
    """Measure how long a fixed CPU-bound workload takes to drain."""

    @pytest.mark.benchmark
    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize(
        "parallelism",
        PARALLELISMS_UNDER_TEST,
        ids=lambda parallelism: parallelism.label,
    )
    def test_drain_cpu_workload__benchmark(self, benchmark, parallelism, empty_queues):
        """Benchmark the time to drain the workload with one pool configuration."""
        workload = CpuWorkload(parallelism=parallelism)
        benchmark.extra_info.update(
            {
                "workers": parallelism.workers,
                "threads": parallelism.threads,
                "tasks": TASK_COUNT,
                "python": sys.version.split()[0],
                "free_threaded_build": is_free_threaded_build(),
                "gil_enabled": is_gil_enabled(),
            }
        )

        benchmark.pedantic(
            workload.drain,
            rounds=MEASUREMENT_ROUNDS,
            iterations=1,
            warmup_rounds=0,
        )

        # Outside the timed region: every round must have run every task.
        workload.verify()
