from __future__ import annotations

import dataclasses
import multiprocessing
import sys
import threading
import time
import uuid

from django.tasks import (
    TaskContext,
    TaskResult,
    TaskResultStatus,
    default_task_backend,
    task,
)
from django.utils import timezone

from tests.testapp.tasks import boom, echo
from threadmill.backends.base import Broker  # noqa: E402
from threadmill.executor import TaskExecutor, WorkerProcess, WorkerThread  # noqa: E402


@task(queue_name="default")
def _add(x, y):
    return x + y


@task(queue_name="default", takes_context=True)
def _context_captor(context):
    return context


@task(queue_name="default")
async def _async_task():
    return 99


def _task_result(task, *args, **kwargs) -> TaskResult:
    """Build a READY `TaskResult` without touching Redis."""
    return TaskResult(
        task=task,
        id=str(uuid.uuid4()),
        status=TaskResultStatus.READY,
        enqueued_at=timezone.now(),
        started_at=None,
        finished_at=None,
        last_attempted_at=None,
        args=list(args),
        kwargs=dict(kwargs),
        backend="default",
        errors=[],
        worker_ids=[],
    )


def _make_worker(*, max_tasks: int | None = None) -> WorkerProcess:
    """Build an unstarted `WorkerProcess`."""
    return WorkerProcess(
        thread_count=1,
        max_tasks=max_tasks,
        backend_alias="default",
        queues=("default",),
    )


class TestTaskExecutor:
    """Tests for the TaskExecutor dataclass and its methods."""

    def test_post_init__sets_process_count_from_workers(self):
        """__post_init__ uses explicit workers value."""
        executor = TaskExecutor(
            backend=default_task_backend, workers=3, queues=("default",)
        )
        assert executor.process_count == 3
        assert executor.thread_count == 1

    def test_post_init__defaults_process_count_to_cpu_minus_one(self):
        """__post_init__ defaults to cpu_count - 1 when workers is None."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        expected = max(multiprocessing.cpu_count() - 1, 1)
        assert executor.process_count == expected

    def test_post_init__thread_count_at_least_one(self):
        """__post_init__ ensures thread_count is at least 1."""
        executor = TaskExecutor(
            backend=default_task_backend, threads=0, queues=("default",)
        )
        assert executor.thread_count == 1

    def test_get_maximum_tasks_per_child__returns_none_when_max_tasks_is_zero(self):
        """get_maximum_tasks_per_child returns None when max_tasks is 0."""
        executor = TaskExecutor(
            backend=default_task_backend, max_tasks=0, queues=("default",)
        )
        assert executor.get_maximum_tasks_per_child() is None

    def test_get_maximum_tasks_per_child__returns_value_when_set(self):
        """get_maximum_tasks_per_child returns max_tasks // thread_count with jitter."""
        executor = TaskExecutor(
            backend=default_task_backend,
            max_tasks=100,
            max_tasks_jitter=0,
            threads=4,
            queues=("default",),
        )
        assert executor.get_maximum_tasks_per_child() == 25  # 100 // 4

    def test_get_maximum_tasks_per_child__applies_jitter(self):
        """get_maximum_tasks_per_child adds random jitter to max_tasks."""
        executor = TaskExecutor(
            backend=default_task_backend,
            max_tasks=100,
            max_tasks_jitter=10,
            threads=1,
            queues=("default",),
        )
        result = executor.get_maximum_tasks_per_child()
        assert 100 <= result <= 110  # (100 + randint(0, 10)) // 1

    def test_create_worker_process__starts_worker(self):
        """create_worker_process creates and starts a WorkerProcess."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        worker = executor.create_worker_process()
        assert worker.is_alive()
        worker.shutdown()

    def test_run__processes_enqueued_tasks_end_to_end(self):
        """run() acquires, executes, and acknowledges tasks through to Redis."""
        count = 3
        enqueued = [default_task_backend.enqueue(echo, args=[i]) for i in range(count)]
        executor = TaskExecutor(
            backend=default_task_backend,
            workers=1,
            threads=2,
            queues=("default",),
        )

        run_thread = threading.Thread(target=executor.run, daemon=True)
        run_thread.start()
        time.sleep(2)
        executor.shutdown()
        run_thread.join(timeout=5)
        assert not run_thread.is_alive()

        results = list(
            default_task_backend.peek(
                "default", status=TaskResultStatus.SUCCESSFUL, count=count
            )
        )
        assert {r.id for r in results} == {r.id for r in enqueued}
        assert all(r.status == TaskResultStatus.SUCCESSFUL for r in results)

    def test_worker_acquires_updates_and_acknowledges(self):
        """Worker acquires, executes, and acknowledges via its own backend."""
        enqueued = default_task_backend.enqueue(echo, args=[42])

        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()

        thread = WorkerThread(
            worker=worker,
            index=0,
            backend=default_task_backend,
        )
        thread.run()

        persisted = default_task_backend.get_result(enqueued.id)
        assert persisted.status == TaskResultStatus.SUCCESSFUL

    def test_shutdown__stops_publishing(self):
        """Shutdown stops publishing."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        executor.shutdown()
        assert not executor.is_publishing

    def test_shutdown__shuts_down_broker(self):
        """Shutdown calls broker.shutdown when a broker is set."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        executor.broker = Broker(default_task_backend)
        executor.shutdown()
        assert executor.broker.shutdown_requested.is_set()

    def test_shutdown__shuts_down_worker_processes(self):
        """Shutdown calls shutdown on all worker processes."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        worker = executor.create_worker_process()
        executor.worker_processes = [worker]
        executor.shutdown()
        assert not worker.is_alive()

    def test_maintain_worker_pool__restarts_dead_workers(self):
        """maintain_worker_pool replaces dead workers with new ones."""
        executor = TaskExecutor(
            backend=default_task_backend, workers=1, threads=1, queues=("default",)
        )
        worker = executor.create_worker_process()
        executor.worker_processes = [worker]
        worker.shutdown()
        assert not worker.is_alive()

        maintain_thread = threading.Thread(
            target=executor.maintain_worker_pool, daemon=True
        )
        maintain_thread.start()
        time.sleep(0.1)
        executor.is_publishing = False
        maintain_thread.join(timeout=2)

        assert executor.worker_processes[0] is not worker
        assert executor.worker_processes[0].is_alive()
        executor.worker_processes[0].shutdown()


class TestWorkerProcess:
    """Tests for the WorkerProcess class."""

    def test_record_task__increments_count(self):
        """record_task increments task_count."""
        worker = _make_worker(max_tasks=5)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()
        worker.record_task()
        assert worker.task_count == 1

    def test_record_task__sets_expired_when_max_reached(self):
        """record_task sets expired event when max_tasks is reached."""
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()
        worker.record_task()
        assert worker.expired.is_set()

    def test_record_task__noop_when_max_tasks_is_none(self):
        """record_task is a no-op when max_tasks is None."""
        worker = _make_worker(max_tasks=None)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()
        worker.record_task()
        assert worker.task_count == 0
        assert not worker.expired.is_set()

    def test_record_task__noop_before_run_sets_lock_and_expired(self):
        """record_task is a safe no-op before run() initializes lock/expired."""
        worker = _make_worker(max_tasks=5)
        worker.record_task()
        assert worker.task_count == 0

    def test_shutdown_requested__is_settable(self):
        """shutdown_requested event can be set on an unstarted worker."""
        worker = _make_worker()
        worker.shutdown_requested.set()
        assert worker.shutdown_requested.is_set()


class TestWorkerThread:
    """Tests for the WorkerThread class."""

    def test_execute_task_result__successful_execution(self):
        """execute_task_result runs a task and returns SUCCESSFUL result."""
        result = WorkerThread(
            worker=_make_worker(), index=0, backend=default_task_backend
        ).execute_task_result(_task_result(echo, 42))
        assert result.status == TaskResultStatus.SUCCESSFUL
        assert result.started_at is not None
        assert result.finished_at is not None
        assert result._return_value == 42

    def test_execute_task_result__failed_execution(self):
        """execute_task_result returns FAILED result when task raises."""
        result = WorkerThread(
            worker=_make_worker(), index=0, backend=default_task_backend
        ).execute_task_result(_task_result(boom))
        assert result.status == TaskResultStatus.FAILED
        assert len(result.errors) == 1
        assert "ValueError" in result.errors[0].exception_class_path

    def test_execute_task_result__preserves_worker_ids(self):
        """execute_task_result preserves worker_ids set by acquire."""
        thread = WorkerThread(
            worker=_make_worker(), index=0, backend=default_task_backend
        )
        task_result = _task_result(echo, 1)
        task_result = dataclasses.replace(task_result, worker_ids=["pre-set-worker"])
        result = thread.execute_task_result(task_result)
        assert result.worker_ids == ["pre-set-worker"]

    def test_call_task__calls_function_with_args(self):
        """call_task invokes the task function with args and kwargs."""
        result = WorkerThread.call_task(_task_result(_add, 1, y=2))
        assert result == 3

    def test_call_task__passes_context_when_takes_context(self):
        """call_task passes TaskContext when task.takes_context is True."""
        result = WorkerThread.call_task(_task_result(_context_captor))
        assert isinstance(result, TaskContext)

    def test_call_task__runs_async_function(self):
        """call_task runs async task functions with asyncio.run."""
        result = WorkerThread.call_task(_task_result(_async_task))
        assert result == 99

    def test_create_task_error__builds_task_error(self):
        """create_task_error builds a TaskError with exception info."""
        try:
            raise RuntimeError("test error")
        except RuntimeError:
            error = WorkerThread.create_task_error(sys.exc_info()[1])
        assert "RuntimeError" in error.exception_class_path
        assert "test error" in error.traceback
