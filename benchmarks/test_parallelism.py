"""Benchmark how much CPU parallelism each framework delivers.

The queue comparison in ``test_backends.py`` measures queue overhead with a
trivial echo task, and ``test_scaling.py`` measures threadmill alone. Neither
answers whether threadmill's free-threading parallelism is unusual, so this
module runs the same CPU-bound workload through every framework that can spread
work across threads and reports how long each takes.

Only a thread-based configuration can use a free-threaded interpreter:

- threadmill: one process with N threads
- dramatiq: one process with N threads, its native model
- celery: ``--pool=threads --concurrency=N``

django-tasks-db processes one task at a time, and RQ forks a process for each
job, so neither has a thread-based configuration to compare.

Every configuration is measured with one thread and with four, from the same
framework and the same queue. The ratio between the two is the useful comparison
rather than the raw times, because each framework carries a different fixed start
cost and that cost sits in both drains. threadmill starts a pool of processes and
pays for the forkserver, the interpreter and Django setup in each one, which costs
seconds, while Celery and dramatiq start a worker in a fraction of a second. A
framework with a cheaper start therefore shows a larger ratio at the same
parallelism. Read the ratio as the speedup of the whole pool, and subtract the
start cost before reading it as the parallelism of the work.

Each drain is measured until every task reported completion, not until a sentinel
was seen: with several threads a free thread can take a sentinel before its
predecessors finish, which would report a drain that never did the work.

Run both interpreters and compare:

    uv run pytest benchmarks/test_parallelism.py -m benchmark
    uv run --python 3.14t pytest benchmarks/test_parallelism.py -m benchmark
"""

import collections.abc
import dataclasses
import os
import subprocess
import sys
import tempfile
import time
import typing

import pytest
import redis
from django.core.management import call_command
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    TaskResultStatus,
    task_backends,
)

from benchmarks import cpu_work
from benchmarks.celery_app import REDIS_URL, celery_compute
from benchmarks.dramatiq_app import dramatiq_compute
from tests.testapp.tasks import compute_workload

TASK_COUNT = 16
"""CPU-bound tasks drained per measurement, about one second of work each.

Deep enough that every worker in every configuration gets a share. threadmill
reads ahead by ``threads * 4``, so one process running four threads asks for the
whole queue and spreads it over its threads, while four processes running one
thread each ask for four apiece and spread it evenly over the processes. With
fewer tasks the process pool is lopsided, because whichever process starts first
takes the queue and the rest boot into an empty one.
"""

THREAD_COUNT = 4
"""Threads in the parallel configuration, and the speedup denominator is one."""

MEASUREMENT_ROUNDS = 1
"""Drains per configuration.

Each drain starts a pool or a worker process and processes the queue, and the
tasks are queued once. A second round would find an empty queue and only pay the
start cost again, so a median over rounds would average work against no work. The
external frameworks also stop their worker outside the measured region, so a
second round would measure two workers competing.
"""

SCALING_QUEUE_NAME = "scaling"
"""Queue dedicated to the CPU benchmarks.

A separate queue keeps the measurement honest: a worker process left behind by
another test or worktree sharing the same Redis instance would otherwise steal
tasks and report a drain that never did the work.
"""

WORKER_STOP_TIMEOUT_SECONDS = 20
"""Seconds to wait for a worker process to stop after SIGTERM."""

DRAIN_TIMEOUT_SECONDS = 300
"""Seconds to wait for a worker to process every queued task."""

READ_AHEAD = 128
"""Messages each worker reads ahead, so a drain pulls its whole queue at once."""

CELERY_SINGLE_THREAD_WORKER = (
    sys.executable,
    "-m",
    "celery",
    "-A",
    "benchmarks.celery_app:celery_app",
    "worker",
    "--pool=solo",
    "-Q",
    cpu_work.CPU_QUEUE,
    f"--prefetch-multiplier={READ_AHEAD}",
    "--loglevel=WARNING",
    "--without-gossip",
    "--without-mingle",
    "--without-heartbeat",
)
"""Celery running one task at a time. The prefork pool crashes on CPython 3.14."""

CELERY_MULTI_THREAD_WORKER = (
    sys.executable,
    "-m",
    "celery",
    "-A",
    "benchmarks.celery_app:celery_app",
    "worker",
    "--pool=threads",
    f"--concurrency={THREAD_COUNT}",
    "-Q",
    cpu_work.CPU_QUEUE,
    f"--prefetch-multiplier={READ_AHEAD}",
    "--loglevel=WARNING",
    "--without-gossip",
    "--without-mingle",
    "--without-heartbeat",
)
"""Celery running its thread pool."""

DRAMATIQ_SINGLE_THREAD_WORKER = (
    sys.executable,
    "-m",
    "dramatiq",
    "benchmarks.dramatiq_app:redis_broker",
    "--processes",
    "1",
    "--threads",
    "1",
    "--queues",
    cpu_work.CPU_QUEUE,
)
"""dramatiq running one process with one thread, its smallest configuration."""

DRAMATIQ_MULTI_THREAD_WORKER = (
    sys.executable,
    "-m",
    "dramatiq",
    "benchmarks.dramatiq_app:redis_broker",
    "--processes",
    "1",
    "--threads",
    str(THREAD_COUNT),
    "--queues",
    cpu_work.CPU_QUEUE,
)
"""dramatiq running one process with several threads, its native model."""

DRAMATIQ_ENV = {**os.environ, "dramatiq_queue_prefetch": str(READ_AHEAD)}
"""Environment carrying the dramatiq read-ahead, which has no command line flag."""

scaling_workload = dataclasses.replace(compute_workload, queue_name=SCALING_QUEUE_NAME)
"""The CPU-bound Django task, pinned to the benchmark's own queue."""


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class WorkerProcess:
    """A worker subprocess started by a benchmark, with its captured log."""

    process: subprocess.Popen
    log: typing.IO[bytes]


running_workers: list[WorkerProcess] = []
"""Workers this benchmark started, stopped once each measurement ends."""


def enqueue_threadmill(count: int) -> None:
    """Queue `count` CPU tasks on the threadmill backend."""
    queued_task = scaling_workload.using(backend=DEFAULT_TASK_BACKEND_ALIAS)
    for _ in range(count):
        queued_task.enqueue()


def enqueue_celery(count: int) -> None:
    """Queue `count` CPU tasks on the Celery broker."""
    for index in range(count):
        celery_compute.delay(index)


def enqueue_dramatiq(count: int) -> None:
    """Queue `count` CPU tasks on the dramatiq broker."""
    for index in range(count):
        dramatiq_compute.send(index)


def drain_with_threadmill_worker() -> None:
    """Drain the queue with one threadmill process running one thread."""
    _drain_with_threadmill_pool(workers=1, threads=1)


def drain_with_threadmill_threads() -> None:
    """Drain the queue with one threadmill process running several threads."""
    _drain_with_threadmill_pool(workers=1, threads=THREAD_COUNT)


def drain_with_threadmill_processes() -> None:
    """Drain the queue with several threadmill processes running one thread each."""
    _drain_with_threadmill_pool(workers=THREAD_COUNT, threads=1)


def _drain_with_threadmill_pool(*, workers: int, threads: int) -> None:
    """Drain the queue and exit, so the measurement includes the pool start cost."""
    call_command(
        "threadmill",
        "worker",
        backend=DEFAULT_TASK_BACKEND_ALIAS,
        queues=[SCALING_QUEUE_NAME],
        workers=workers,
        threads=threads,
        exit_empty=True,
        verbosity=0,
    )


def drain_with_celery_single_thread() -> None:
    """Drain the queue with a one-thread Celery worker."""
    drain_with_external_worker(CELERY_SINGLE_THREAD_WORKER)


def drain_with_celery_threads() -> None:
    """Drain the queue with a multi-thread Celery worker."""
    drain_with_external_worker(CELERY_MULTI_THREAD_WORKER)


def drain_with_dramatiq_single_thread() -> None:
    """Drain the queue with a one-thread dramatiq worker."""
    drain_with_external_worker(DRAMATIQ_SINGLE_THREAD_WORKER, env=DRAMATIQ_ENV)


def drain_with_dramatiq_threads() -> None:
    """Drain the queue with a multi-thread dramatiq worker."""
    drain_with_external_worker(DRAMATIQ_MULTI_THREAD_WORKER, env=DRAMATIQ_ENV)


def drain_with_external_worker(
    argv: collections.abc.Sequence[str],
    env: collections.abc.Mapping[str, str] | None = None,
) -> None:
    """Run a worker CLI until every queued task reported completion.

    The worker is stopped after the measurement, by the ``stop_workers`` fixture,
    because a graceful shutdown takes seconds and would dominate a short drain.
    """
    client = redis.Redis.from_url(REDIS_URL)
    client.delete(cpu_work.COMPLETION_KEY)
    log = tempfile.TemporaryFile()
    # The command is a fixed worker CLI, never caller input.
    process = subprocess.Popen(  # noqa: S603
        argv,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    running_workers.append(WorkerProcess(process=process, log=log))
    wait_until_completed(client, process, log)


def wait_until_completed(
    client: redis.Redis, process: subprocess.Popen, log: typing.IO[bytes]
) -> None:
    """Wait until every CPU task incremented the completion counter."""
    deadline = time.monotonic() + DRAIN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if int(client.get(cpu_work.COMPLETION_KEY) or 0) >= TASK_COUNT:
            return
        if process.poll() is not None:
            raise AssertionError(
                f"Worker exited with {process.returncode} after "
                f"{int(client.get(cpu_work.COMPLETION_KEY) or 0)} of {TASK_COUNT}"
                f" tasks:\n{read_log_tail(log)}"
            )
        time.sleep(0.001)
    raise AssertionError(
        f"Worker processed {int(client.get(cpu_work.COMPLETION_KEY) or 0)} of"
        f" {TASK_COUNT} tasks within {DRAIN_TIMEOUT_SECONDS}s:\n{read_log_tail(log)}"
    )


def read_log_tail(log: typing.IO[bytes], line_count: int = 40) -> str:
    """Return the last lines of a worker log."""
    log.seek(0)
    lines = log.read().decode(errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def stop_process(process: subprocess.Popen) -> None:
    """Ask a worker to stop, killing it if it does not exit in time."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=WORKER_STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=WORKER_STOP_TIMEOUT_SECONDS)


def verify_threadmill_drain() -> None:
    """Assert the threadmill pool left no task queued and every task succeeded."""
    backend = task_backends[DEFAULT_TASK_BACKEND_ALIAS]
    for status in (TaskResultStatus.READY, TaskResultStatus.RUNNING):
        remaining = sum(
            1 for _ in backend.peek(SCALING_QUEUE_NAME, status=status, count=0)
        )
        assert remaining == 0, f"{remaining} tasks left in {status.name}"
    successful = sum(
        1
        for _ in backend.peek(
            SCALING_QUEUE_NAME, status=TaskResultStatus.SUCCESSFUL, count=0
        )
    )
    assert successful == TASK_COUNT, f"{successful} of {TASK_COUNT} tasks succeeded"


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class FrameworkParallelism:
    """One framework and thread configuration to measure."""

    name: str
    """Identifier in the benchmark report."""

    enqueue: collections.abc.Callable[[int], None]
    """Queue the CPU workload."""

    drain: collections.abc.Callable[[], None]
    """Process the queued workload."""

    verify: collections.abc.Callable[[], None] | None = None
    """Assert the drain did the work, where the drain does not already prove it."""


PARALLELISMS_UNDER_TEST = (
    FrameworkParallelism(
        name="threadmill-1t",
        enqueue=enqueue_threadmill,
        drain=drain_with_threadmill_worker,
        verify=verify_threadmill_drain,
    ),
    FrameworkParallelism(
        name="threadmill-4t",
        enqueue=enqueue_threadmill,
        drain=drain_with_threadmill_threads,
        verify=verify_threadmill_drain,
    ),
    FrameworkParallelism(
        name="threadmill-4p",
        enqueue=enqueue_threadmill,
        drain=drain_with_threadmill_processes,
        verify=verify_threadmill_drain,
    ),
    FrameworkParallelism(
        name="celery-1t", enqueue=enqueue_celery, drain=drain_with_celery_single_thread
    ),
    FrameworkParallelism(
        name="celery-4t", enqueue=enqueue_celery, drain=drain_with_celery_threads
    ),
    FrameworkParallelism(
        name="dramatiq-1t",
        enqueue=enqueue_dramatiq,
        drain=drain_with_dramatiq_single_thread,
    ),
    FrameworkParallelism(
        name="dramatiq-4t",
        enqueue=enqueue_dramatiq,
        drain=drain_with_dramatiq_threads,
    ),
)
"""Configurations spanning each framework at one thread and at several.

The queue overhead benchmark already compares threadmill, django-tasks-db and
django-tasks-rq for throughput with an echo task, so this one covers the
thread-based configurations those two cannot offer.
"""


def identify_parallelism(parallelism: FrameworkParallelism) -> str:
    """Return the benchmark identifier of a configuration."""
    return parallelism.name


@pytest.fixture(autouse=True)
def stop_workers(empty_queues):
    """Stop every worker a measurement started, after the measurement.

    Depends on ``empty_queues`` so that the cleanup runs after the workers stop,
    rather than while one is still writing to Redis.
    """
    yield
    while running_workers:
        worker = running_workers.pop()
        stop_process(worker.process)
        worker.log.close()


@pytest.fixture
def empty_queues():
    """Delete queued tasks from every compared queue before and after a measurement."""
    client = redis.Redis.from_url(REDIS_URL)
    threadmill_client = task_backends[DEFAULT_TASK_BACKEND_ALIAS].client

    def delete_queued_tasks() -> None:
        if keys := threadmill_client.keys("threadmill:*"):
            threadmill_client.delete(*keys)
        # The Celery queue is a bare list named after the queue, so it has no
        # prefix to match and is named here.
        for key_pattern in ("celery*", "_kombu*", cpu_work.CPU_QUEUE, "dramatiq:*"):
            if keys := client.keys(key_pattern):
                client.delete(*keys)
        client.delete(cpu_work.COMPLETION_KEY)

    delete_queued_tasks()
    yield
    delete_queued_tasks()


class TestCpuParallelism:
    """Measure how long a CPU-bound workload takes to drain."""

    @pytest.mark.benchmark
    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize(
        "parallelism",
        PARALLELISMS_UNDER_TEST,
        ids=identify_parallelism,
    )
    def test_drain_cpu_workload__benchmark(self, benchmark, parallelism, empty_queues):
        """Benchmark the time for every task to report completion."""
        benchmark.extra_info.update(
            {
                "tasks": TASK_COUNT,
                "threads": THREAD_COUNT,
                "python": sys.version.split()[0],
            }
        )
        parallelism.enqueue(TASK_COUNT)

        benchmark.pedantic(
            parallelism.drain,
            rounds=MEASUREMENT_ROUNDS,
            iterations=1,
            warmup_rounds=0,
        )

        # Outside the timed region: a drain that skipped work must not look fast.
        if parallelism.verify:
            parallelism.verify()
