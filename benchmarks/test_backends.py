"""Benchmarks comparing threadmill against other Django task queues.

Every backend is measured on the same trivial echo task and the same queue, so
the numbers reflect queue and worker overhead instead of task work:

- ``test_enqueue__benchmark``: time for a queue to accept a single task.
- ``test_start_worker__benchmark``: time for a worker to start and process one queued task.
- ``test_process_queue__benchmark``: time for a worker to process a full queue.

Both worker benchmarks include the worker's fixed start cost, and the queues
that exit on their own include their stop cost too. Subtract
``test_start_worker__benchmark`` from ``test_process_queue__benchmark`` and divide
the queue depth by the difference to get the marginal throughput of a busy queue.

Threadmill, django-tasks-db and django-tasks-redis run one worker process that
drains a queue and exits. Celery has no such mode, so the benchmark queues a
sentinel task last and waits for it to be processed. That wait is what proves the
queue was drained. Its worker is stopped after the measurement, because a graceful
shutdown takes seconds and would dominate a short drain.
"""

import collections.abc
import dataclasses
import io
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
    DEFAULT_TASK_QUEUE_NAME,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
from django_tasks_db.models import DBTaskResult

from benchmarks.celery_app import (
    PROCESSED_KEY,
    REDIS_URL,
    celery_echo,
    celery_mark_processed,
)
from tests.testapp.tasks import echo

ENQUEUE_ITERATIONS = 500
"""Tasks enqueued within one enqueue benchmark round."""

QUEUE_DEPTH = 5000
"""Tasks queued before one processing benchmark round."""

CELERY_WORKER = (
    sys.executable,
    "-m",
    "celery",
    "-A",
    "benchmarks.celery_app:celery_app",
    "worker",
    "--pool=solo",
    "--prefetch-multiplier=1",
    "--loglevel=WARNING",
    "--without-gossip",
    "--without-mingle",
    "--without-heartbeat",
)
"""Celery worker running as one process with one thread, reading one message at a time.

The default prefork pool crashes on CPython 3.14, where the pool child loses
the task handler state it expects.
"""

WORKER_STOP_TIMEOUT_SECONDS = 20
"""Seconds to wait for a worker process to stop after SIGTERM."""

QUEUE_DRAIN_TIMEOUT_SECONDS = 300
"""Seconds to wait for a worker to process every queued task."""


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class WorkerProcess:
    """A worker subprocess started by a benchmark, with its captured log."""

    process: subprocess.Popen
    log: typing.IO[bytes]


@dataclasses.dataclass(frozen=True, kw_only=True, slots=True)
class QueueUnderTest:
    """A task queue, how to accept tasks with it, and how to drain it."""

    name: str
    """Identifier of the queue in the benchmark report."""

    enqueue: collections.abc.Callable[[int], TaskResult | None]
    """Accept the given number of echo tasks and return the newest task result."""

    drain: collections.abc.Callable[[], None] | None = None
    """Run the queue's worker until its queue is empty."""

    verify: collections.abc.Callable[[TaskResult | None], None] | None = None
    """Assert that the drained queue was processed."""


def drain_with_threadmill_worker() -> None:
    """Process every queued task with a single threadmill worker process."""
    call_command(
        "threadmill",
        "worker",
        backend=DEFAULT_TASK_BACKEND_ALIAS,
        queues=[DEFAULT_TASK_QUEUE_NAME],
        workers=1,
        exit_empty=True,
        verbosity=0,
    )


def drain_with_django_tasks_db_worker() -> None:
    """Process every queued task with the django-tasks-db worker."""
    call_command(
        "db_worker",
        "--backend",
        "django-tasks-db",
        "--batch",
        "--interval",
        "0.01",
        "--no-startup-delay",
        verbosity=0,
        stdout=io.StringIO(),
    )


def drain_with_django_tasks_redis_worker() -> None:
    """Process every queued task with the django-tasks-redis worker."""
    call_command(
        "run_redis_tasks",
        backend_name="django-tasks-redis",
        verbosity=0,
        stdout=io.StringIO(),
    )


def drain_with_celery_worker() -> None:
    """Process every queued task with a single-process Celery worker."""
    celery_mark_processed.delay()
    drain_with_subprocess_worker(CELERY_WORKER)


def drain_with_subprocess_worker(argv: collections.abc.Sequence[str]) -> None:
    """Run a worker CLI until the sentinel task queued last was processed."""
    client = redis.Redis.from_url(REDIS_URL)
    client.delete(PROCESSED_KEY)
    log = tempfile.TemporaryFile()
    # The command is a fixed worker CLI, never caller input.
    process = subprocess.Popen(  # noqa: S603
        argv,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    running_workers.append(WorkerProcess(process=process, log=log))
    wait_until_processed(client, process, log)


def wait_until_processed(
    client: redis.Redis,
    process: subprocess.Popen,
    log: typing.IO[bytes],
) -> None:
    """Wait until the sentinel task incremented the processed counter."""
    deadline = time.monotonic() + QUEUE_DRAIN_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if client.get(PROCESSED_KEY):
            return
        if process.poll() is not None:
            raise AssertionError(
                f"Worker exited with {process.returncode} before draining its queue:\n"
                f"{read_log_tail(log)}"
            )
        time.sleep(0.001)
    raise AssertionError(
        f"Worker did not drain its queue within {QUEUE_DRAIN_TIMEOUT_SECONDS}s:\n"
        f"{read_log_tail(log)}"
    )


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


def read_log_tail(log: typing.IO[bytes], line_count: int = 40) -> str:
    """Return the last lines of a worker log."""
    log.seek(0)
    lines = log.read().decode(errors="replace").splitlines()
    return "\n".join(lines[-line_count:])


def django_task_enqueuer(
    alias: str,
) -> tuple[
    collections.abc.Callable[[int], TaskResult],
    collections.abc.Callable[[TaskResult | None], None],
]:
    """Build an enqueuer and a verifier for a Django task backend."""
    task = echo.using(backend=alias)

    def enqueue(count: int) -> TaskResult:
        """Accept `count` echo tasks, returning the newest task result."""
        return [task.enqueue(index) for index in range(count)][-1]

    def verify_processed(enqueued_task_result: TaskResult | None) -> None:
        """Assert that the backend executed the benchmark tasks."""
        assert enqueued_task_result is not None, "enqueue() must return a task result"
        task_result = task_backends[alias].get_result(enqueued_task_result.id)
        assert task_result.status is TaskResultStatus.SUCCESSFUL, (
            f"{alias} did not execute the benchmark tasks"
        )

    return enqueue, verify_processed


def enqueue_celery_tasks(count: int) -> None:
    """Accept `count` echo tasks on the Celery queue."""
    for index in range(count):
        celery_echo.delay(index)


def django_task_backend(
    name: str,
    alias: str,
    drain: collections.abc.Callable[[], None] | None = None,
) -> QueueUnderTest:
    """Build a comparison entry for a Django task backend."""
    enqueue, verify = django_task_enqueuer(alias)
    return QueueUnderTest(name=name, enqueue=enqueue, drain=drain, verify=verify)


WORKER_QUEUES = (
    django_task_backend(
        "threadmill", DEFAULT_TASK_BACKEND_ALIAS, drain_with_threadmill_worker
    ),
    django_task_backend(
        "django-tasks-db", "django-tasks-db", drain_with_django_tasks_db_worker
    ),
    django_task_backend(
        "django-tasks-redis", "django-tasks-redis", drain_with_django_tasks_redis_worker
    ),
    QueueUnderTest(
        name="celery",
        enqueue=enqueue_celery_tasks,
        drain=drain_with_celery_worker,
    ),
)
"""Queues that ship a worker to process queued tasks."""

TASK_BACKENDS_UNDER_TEST = (
    *WORKER_QUEUES,
    django_task_backend("django.tasks.immediate", "immediate"),
    django_task_backend("django.tasks.dummy", "dummy"),
)
"""Queues that accept tasks, including those that execute or discard them inline."""


def identify_backend(queue: QueueUnderTest) -> str:
    """Return the benchmark identifier of a queue."""
    return queue.name


running_workers: list[WorkerProcess] = []
"""Workers started by the running benchmark, stopped once it is measured."""


@pytest.fixture(autouse=True)
def stop_workers(empty_queues):
    """Stop every worker a benchmark started, after its measurement.

    Depends on ``empty_queues`` so that the queue cleanup runs after the workers
    stop, rather than while one is still writing to Redis.
    """
    yield
    while running_workers:
        worker = running_workers.pop()
        stop_process(worker.process)
        worker.log.close()


@pytest.fixture
def empty_queues():
    """Delete queued tasks from every compared queue before and after a benchmark."""
    client = task_backends[DEFAULT_TASK_BACKEND_ALIAS].client

    def delete_queued_tasks() -> None:
        for key_pattern in ("threadmill:*", "django_tasks:*", "celery*", "_kombu*"):
            if keys := client.keys(key_pattern):
                client.delete(*keys)
        client.delete(PROCESSED_KEY)
        DBTaskResult.objects.all().delete()

    delete_queued_tasks()
    yield
    delete_queued_tasks()


class TestEnqueue:
    """Measure how fast each queue accepts a task."""

    @pytest.mark.benchmark
    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize(
        "queue_under_test",
        TASK_BACKENDS_UNDER_TEST,
        ids=identify_backend,
    )
    def test_enqueue__benchmark(self, benchmark, queue_under_test, empty_queues):
        """Benchmark the time to enqueue a single task."""
        benchmark.pedantic(
            queue_under_test.enqueue,
            args=(1,),
            rounds=1,
            iterations=ENQUEUE_ITERATIONS,
            warmup_rounds=0,
        )


class TestWorkerStart:
    """Measure how long a worker takes to start and process one queued task."""

    @pytest.mark.benchmark
    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize("queue_under_test", WORKER_QUEUES, ids=identify_backend)
    def test_start_worker__benchmark(self, benchmark, queue_under_test, empty_queues):
        """Benchmark the time for a worker to start and process one queued task."""
        queue_under_test.enqueue(1)

        benchmark.pedantic(
            queue_under_test.drain,
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )


class TestQueueProcessing:
    """Measure how long a worker takes to process a full queue."""

    @pytest.mark.benchmark
    @pytest.mark.django_db(transaction=True)
    @pytest.mark.parametrize("queue_under_test", WORKER_QUEUES, ids=identify_backend)
    def test_process_queue__benchmark(self, benchmark, queue_under_test, empty_queues):
        """Benchmark the time to process QUEUE_DEPTH queued tasks."""
        enqueued_task_result = queue_under_test.enqueue(QUEUE_DEPTH)
        benchmark.extra_info["tasks"] = QUEUE_DEPTH

        benchmark.pedantic(
            queue_under_test.drain,
            rounds=1,
            iterations=1,
            warmup_rounds=0,
        )

        if queue_under_test.verify:
            queue_under_test.verify(enqueued_task_result)
