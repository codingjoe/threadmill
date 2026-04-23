from __future__ import annotations

import asyncio
import datetime
import threading
from queue import Empty
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from django.tasks import TaskResult
from django.tasks.base import TaskResultStatus
from grinder import executor


class RecordingTask:
    def __init__(
        self,
        *,
        takes_context: bool,
        return_value=None,
        exception: Exception | None = None,
    ):
        self.takes_context = takes_context
        self.return_value = return_value
        self.exception = exception
        self.calls: list[tuple[tuple, dict]] = []

    def call(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if self.exception is not None:
            raise self.exception
        return self.return_value


class FakeJoinableQueue:
    def __init__(self, *, items: list | None = None):
        self.items = [] if items is None else [*items]
        self.put_calls: list = []
        self.task_done_calls = 0

    def put(self, item):
        self.put_calls.append(item)

    def get(self, *, timeout: float | None = None, block: bool = True):
        if not self.items:
            raise Empty
        return self.items.pop(0)

    def task_done(self):
        self.task_done_calls += 1

    def empty(self) -> bool:
        return not self.items


class QueueRaisingEmpty:
    def __init__(self, *, is_empty: bool):
        self.is_empty = is_empty
        self.task_done_calls = 0

    def get(self, *, timeout: float | None = None, block: bool = True):
        raise Empty

    def empty(self) -> bool:
        return self.is_empty

    def task_done(self):
        self.task_done_calls += 1


class QueueRaiseThenReturn:
    def __init__(self, *, item):
        self.item = item
        self.calls = 0
        self.task_done_calls = 0

    def get(self, *, timeout: float | None = None, block: bool = True):
        self.calls += 1
        if self.calls == 1:
            raise Empty
        return self.item

    def empty(self) -> bool:
        return False

    def task_done(self):
        self.task_done_calls += 1


class FakeWorkerProcess:
    def __init__(self, *, alive: bool):
        self.alive = alive
        self.join_calls: list[float] = []
        self.shutdown_called = False

    def is_alive(self) -> bool:
        return self.alive

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)

    def shutdown(self) -> None:
        self.shutdown_called = True


def create_task_result(*, task: RecordingTask, args=None, kwargs=None) -> TaskResult:
    return TaskResult(
        task=task,
        id="task-id",
        status=TaskResultStatus.READY,
        enqueued_at=None,
        started_at=None,
        finished_at=None,
        last_attempted_at=None,
        args=[] if args is None else args,
        kwargs={} if kwargs is None else kwargs,
        backend="default",
        errors=[],
        worker_ids=[],
    )


def create_task_error_from_value_error() -> None:
    try:
        raise ValueError("benchmark")
    except ValueError as exception:
        executor.WorkerThread.create_task_error(exception)


class TestTaskExecutor:
    def test_run__create_tasks_for_all_executor_loops(self, monkeypatch) -> None:
        """Create orchestration tasks for acquire, acknowledge, and maintenance loops."""
        created_tasks = []

        def create_task(coroutine):
            created_tasks.append(coroutine.cr_code.co_name)
            coroutine.close()
            return coroutine.cr_code.co_name

        gather = Mock()
        monkeypatch.setattr(executor.asyncio, "create_task", create_task)
        monkeypatch.setattr(executor.asyncio, "gather", gather)
        monkeypatch.setattr(
            executor.TaskExecutor,
            "create_worker_process",
            lambda task_executor_self: FakeWorkerProcess(alive=True),
        )

        task_executor = executor.TaskExecutor(backend=SimpleNamespace(), workers=1)

        task_executor.run()

        assert len(task_executor.worker_processes) == 1
        assert created_tasks == [
            "acquire_tasks",
            "acknowledge_tasks",
            "maintain_worker_pool",
        ]
        gather.assert_called_once_with(
            "acquire_tasks", "acknowledge_tasks", "maintain_worker_pool"
        )

    def test_acquire_tasks__put_acquired_task_in_shared_queue(self) -> None:
        """Put acquired task result into shared queue."""

        class AcquireBackend:
            def __init__(self):
                self.task_executor = None

            def acquire(self):
                self.task_executor.is_acquiring = False
                return "task-result"

        backend = AcquireBackend()
        task_executor = executor.TaskExecutor(backend=backend, workers=1)
        backend.task_executor = task_executor

        asyncio.run(task_executor.acquire_tasks())

        assert task_executor.shared_task_queue.get(timeout=0.1) == "task-result"

    def test_acknowledge_tasks__acknowledge_processed_task_and_mark_done(self) -> None:
        """Acknowledge processed task and mark queue item done."""

        class AcknowledgeBackend:
            def __init__(self):
                self.calls: list = []
                self.task_executor = None

            def acknowledge(self, task_result):
                self.calls.append(task_result)
                self.task_executor.is_publishing = False

        backend = AcknowledgeBackend()
        task_executor = executor.TaskExecutor(backend=backend, workers=1)
        backend.task_executor = task_executor
        task_executor.processed_task_queue.put("processed-result")

        asyncio.run(task_executor.acknowledge_tasks())

        assert backend.calls == ["processed-result"]

    def test_get_maximum_tasks_per_child__return_none_without_max_tasks(self) -> None:
        """Return None when task recycling is disabled."""
        task_executor = executor.TaskExecutor(backend=SimpleNamespace(), max_tasks=0)

        assert task_executor.get_maximum_tasks_per_child() is None

    def test_get_maximum_tasks_per_child__calculate_recycle_limit_with_jitter(
        self, monkeypatch
    ) -> None:
        """Calculate worker recycle limit with jitter and thread count."""
        monkeypatch.setattr(executor.random, "randint", lambda start, end: 4)
        task_executor = executor.TaskExecutor(
            backend=SimpleNamespace(),
            threads=2,
            max_tasks=6,
            max_tasks_jitter=4,
        )

        assert task_executor.get_maximum_tasks_per_child() == 5

    def test_create_worker_process__start_and_return_worker(self, monkeypatch) -> None:
        """Create and start a worker process."""
        started = Mock()

        class WorkerProcessDouble:
            def __init__(self, *args):
                self.args = args

            def start(self):
                started()

        monkeypatch.setattr(executor, "WorkerProcess", WorkerProcessDouble)
        task_executor = executor.TaskExecutor(
            backend=SimpleNamespace(), workers=1, threads=3
        )

        worker_process = task_executor.create_worker_process()

        assert isinstance(worker_process, WorkerProcessDouble)
        assert worker_process.args[2] == 3
        started.assert_called_once_with()

    def test_shutdown__stop_flags_join_queues_and_shutdown_workers(self) -> None:
        """Stop publishing and shut down all workers."""
        task_executor = executor.TaskExecutor(backend=SimpleNamespace(), workers=1)
        worker_process = FakeWorkerProcess(alive=True)
        task_executor.worker_processes = [worker_process]

        task_executor.shutdown()

        assert task_executor.is_acquiring is False
        assert task_executor.is_publishing is False
        assert worker_process.shutdown_called is True

    def test_maintain_worker_pool__replace_dead_worker(self, monkeypatch) -> None:
        """Replace dead worker process during pool maintenance."""
        task_executor = executor.TaskExecutor(backend=SimpleNamespace(), workers=2)
        dead_worker = FakeWorkerProcess(alive=False)
        healthy_worker = FakeWorkerProcess(alive=True)
        replacement_worker = FakeWorkerProcess(alive=True)
        task_executor.worker_processes = [dead_worker, healthy_worker]

        def create_worker_process(task_executor_self):
            task_executor.is_publishing = False
            return replacement_worker

        monkeypatch.setattr(
            executor.TaskExecutor,
            "create_worker_process",
            create_worker_process,
        )

        asyncio.run(task_executor.maintain_worker_pool())

        assert dead_worker.join_calls == [0]
        assert task_executor.worker_processes == [replacement_worker, healthy_worker]


class TestWorkerProcess:
    def test_run__initialize_sync_primitives_and_start_worker_threads(
        self, monkeypatch
    ) -> None:
        """Initialize lock and expiration event before starting worker threads."""
        run_worker_process = Mock()
        monkeypatch.setattr(
            executor.WorkerProcess, "run_worker_process", run_worker_process
        )
        worker_process = executor.WorkerProcess(
            FakeJoinableQueue(),
            FakeJoinableQueue(),
            thread_count=1,
            task_timeout=datetime.timedelta(seconds=1),
        )

        worker_process.run()

        assert isinstance(worker_process.lock, type(threading.Lock()))
        assert isinstance(worker_process.expired, type(threading.Event()))
        run_worker_process.assert_called_once_with(worker_process)

    def test_run_worker_process__start_and_join_each_consumer_thread(
        self, monkeypatch
    ) -> None:
        """Start and join every consumer thread created for the process."""
        thread_events: list[str] = []

        class WorkerThreadDouble:
            def __init__(self, *, worker, index):
                self.index = index

            def start(self):
                thread_events.append(f"start:{self.index}")

            def join(self, timeout):
                thread_events.append(f"join:{self.index}:{timeout}")

        monkeypatch.setattr(executor, "WorkerThread", WorkerThreadDouble)
        worker_process = SimpleNamespace(
            thread_count=2, task_timeout=datetime.timedelta(seconds=3)
        )

        executor.WorkerProcess.run_worker_process(worker_process)

        assert thread_events == [
            "start:0",
            "start:1",
            "join:0:3.0",
            "join:1:3.0",
        ]

    def test_record_task__set_expired_after_reaching_max_tasks(self) -> None:
        """Set expiration event when processed task limit is reached."""
        worker_process = executor.WorkerProcess(
            FakeJoinableQueue(),
            FakeJoinableQueue(),
            thread_count=1,
            task_timeout=datetime.timedelta(seconds=1),
            max_tasks=2,
        )
        worker_process.lock = threading.Lock()
        worker_process.expired = threading.Event()

        worker_process.record_task()
        worker_process.record_task()

        assert worker_process.expired.is_set() is True

    def test_record_task__ignore_when_limit_or_state_is_missing(self) -> None:
        """Ignore task recording when worker state is incomplete."""
        worker_process = executor.WorkerProcess(
            FakeJoinableQueue(),
            FakeJoinableQueue(),
            thread_count=1,
            task_timeout=datetime.timedelta(seconds=1),
            max_tasks=None,
        )

        worker_process.record_task()

        assert worker_process.task_count == 0

    def test_record_task__ignore_when_sync_state_not_initialized(self) -> None:
        """Ignore recording when synchronization objects are missing."""
        worker_process = executor.WorkerProcess(
            FakeJoinableQueue(),
            FakeJoinableQueue(),
            thread_count=1,
            task_timeout=datetime.timedelta(seconds=1),
            max_tasks=1,
        )

        worker_process.record_task()

        assert worker_process.task_count == 0

    def test_shutdown__set_shutdown_flag_and_join_process(self, monkeypatch) -> None:
        """Set shutdown event and wait for worker process exit."""
        worker_process = executor.WorkerProcess(
            FakeJoinableQueue(),
            FakeJoinableQueue(),
            thread_count=1,
            task_timeout=datetime.timedelta(seconds=1),
        )
        join = Mock()
        monkeypatch.setattr(worker_process, "join", join)

        worker_process.shutdown()

        assert worker_process.shutdown_requested.is_set() is True
        join.assert_called_once_with()


class TestWorkerThread:
    def test_run__return_when_shutdown_requested_and_queue_is_empty(self) -> None:
        """Return when shutdown is requested and queue has no pending task."""
        worker = SimpleNamespace(
            expired=threading.Event(),
            shutdown_requested=threading.Event(),
            task_queue=FakeJoinableQueue(),
            processed_task_queue=FakeJoinableQueue(),
            record_task=Mock(),
            pid=100,
        )
        worker.shutdown_requested.set()
        worker_thread = executor.WorkerThread(worker=worker, index=1)

        worker_thread.run()

        worker.record_task.assert_not_called()

    def test_run__process_single_task_and_finish(self, monkeypatch) -> None:
        """Process one task, acknowledge queue bookkeeping, and stop."""
        task_result = create_task_result(
            task=RecordingTask(takes_context=False, return_value=1)
        )
        task_queue = FakeJoinableQueue(items=[task_result])

        expired = threading.Event()

        def record_task() -> None:
            expired.set()

        worker = SimpleNamespace(
            expired=expired,
            shutdown_requested=threading.Event(),
            task_queue=task_queue,
            processed_task_queue=FakeJoinableQueue(),
            record_task=record_task,
            pid=200,
        )
        worker_thread = executor.WorkerThread(worker=worker, index=1)
        monkeypatch.setattr(worker_thread, "execute_task_result", lambda result: result)

        worker_thread.run()

        assert worker.processed_task_queue.put_calls == [task_result]
        assert task_queue.task_done_calls == 1

    def test_run__return_after_empty_queue_when_shutdown_requested(self) -> None:
        """Return after queue timeout when shutdown has been requested."""
        worker = SimpleNamespace(
            expired=threading.Event(),
            shutdown_requested=threading.Event(),
            task_queue=QueueRaisingEmpty(is_empty=False),
            processed_task_queue=FakeJoinableQueue(),
            record_task=Mock(),
            pid=201,
        )
        worker.shutdown_requested.set()
        worker_thread = executor.WorkerThread(worker=worker, index=2)

        worker_thread.run()

        worker.record_task.assert_not_called()

    def test_run__continue_on_empty_queue_without_shutdown(self, monkeypatch) -> None:
        """Continue polling after timeout while shutdown has not been requested."""
        task_result = create_task_result(
            task=RecordingTask(takes_context=False, return_value=2)
        )
        worker = SimpleNamespace(
            expired=threading.Event(),
            shutdown_requested=threading.Event(),
            task_queue=QueueRaiseThenReturn(item=task_result),
            processed_task_queue=FakeJoinableQueue(),
            record_task=Mock(),
            pid=202,
        )

        def execute_task_result(_task_result):
            worker.expired.set()
            return _task_result

        worker_thread = executor.WorkerThread(worker=worker, index=3)
        monkeypatch.setattr(worker_thread, "execute_task_result", execute_task_result)

        worker_thread.run()

        worker.record_task.assert_called_once_with()

    @pytest.mark.benchmark
    def test_call_task__benchmark_without_context(self, benchmark) -> None:
        """Benchmark context-free task execution path."""
        task_result = create_task_result(
            task=RecordingTask(takes_context=False),
            args=[1, 2, 3],
            kwargs={"count": 4},
        )

        benchmark(executor.WorkerThread.call_task, task_result)

    @pytest.mark.benchmark
    def test_call_task__benchmark_with_context(self, benchmark) -> None:
        """Benchmark context-aware task execution path."""
        task_result = create_task_result(
            task=RecordingTask(takes_context=True),
            args=[1, 2, 3],
            kwargs={"count": 4},
        )

        benchmark(executor.WorkerThread.call_task, task_result)

    @pytest.mark.benchmark
    def test_create_task_error__benchmark(self, benchmark) -> None:
        """Benchmark task error payload creation from raised exceptions."""
        benchmark(create_task_error_from_value_error)

    def test_call_task__pass_context_when_task_requires_context(self) -> None:
        """Pass task context as first argument for context-aware tasks."""
        task = RecordingTask(takes_context=True, return_value="ok")
        task_result = create_task_result(task=task, args=[1], kwargs={"value": 2})

        return_value = executor.WorkerThread.call_task(task_result)

        assert return_value == "ok"
        args, kwargs = task.calls[0]
        assert kwargs == {"value": 2}
        assert args[1:] == (1,)
        assert args[0].task_result is task_result

    def test_call_task__call_without_context_when_not_required(self) -> None:
        """Call task with regular positional and keyword arguments."""
        task = RecordingTask(takes_context=False, return_value="done")
        task_result = create_task_result(task=task, args=[3], kwargs={"count": 4})

        return_value = executor.WorkerThread.call_task(task_result)

        assert return_value == "done"
        assert task.calls == [((3,), {"count": 4})]

    def test_create_task_error__include_exception_type_and_traceback(self) -> None:
        """Create task error payload with exception class path and traceback."""
        try:
            raise RuntimeError("worker failed")
        except RuntimeError as exception:
            task_error = executor.WorkerThread.create_task_error(exception)

        assert task_error.exception_class_path == "builtins.RuntimeError"
        assert "RuntimeError: worker failed" in task_error.traceback

    def test_execute_task_result__set_success_status_and_return_value(
        self, monkeypatch
    ) -> None:
        """Set success lifecycle fields after task execution succeeds."""
        monkeypatch.setattr(executor.task_enqueued, "send", Mock())
        monkeypatch.setattr(executor.task_started, "send", Mock())
        monkeypatch.setattr(executor.task_finished, "send", Mock())

        task_result = create_task_result(
            task=RecordingTask(takes_context=False, return_value={"value": 5}),
        )
        worker = SimpleNamespace(pid=321)
        worker_thread = executor.WorkerThread(worker=worker, index=7)

        processed_task_result = worker_thread.execute_task_result(task_result)

        assert processed_task_result.status is TaskResultStatus.SUCCESSFUL
        assert processed_task_result._return_value is None
        assert processed_task_result.finished_at is not None
        assert processed_task_result.started_at is not None
        assert worker_thread.name in processed_task_result.worker_ids

    def test_execute_task_result__set_failed_status_and_append_error(
        self, monkeypatch
    ) -> None:
        """Set failure status and append task error when execution fails."""
        monkeypatch.setattr(executor.task_enqueued, "send", Mock())
        monkeypatch.setattr(executor.task_started, "send", Mock())
        monkeypatch.setattr(executor.task_finished, "send", Mock())

        task_result = create_task_result(
            task=RecordingTask(takes_context=False, exception=ValueError("invalid")),
        )
        worker = SimpleNamespace(pid=111)
        worker_thread = executor.WorkerThread(worker=worker, index=3)

        processed_task_result = worker_thread.execute_task_result(task_result)

        assert processed_task_result.status is TaskResultStatus.FAILED
        assert processed_task_result.finished_at is not None
        assert len(processed_task_result.errors) == 1
        assert (
            processed_task_result.errors[0].exception_class_path
            == "builtins.ValueError"
        )
