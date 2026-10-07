import dataclasses
import datetime
import io
import json
import logging
import multiprocessing
import queue
import sys
import threading
import time
import uuid

import pytest
from django.tasks import (
    TaskContext,
    TaskResult,
    TaskResultStatus,
    default_task_backend,
    task,
)
from django.utils import timezone

from tests.testapp.tasks import (
    boom,
    boom_no_retry,
    boom_retry_raises,
    boom_with_retry,
    count_users,
    echo,
    log_message,
)
from threadmill.backends.base import Broker, ThreadmillTaskBackend
from threadmill.executor import (
    JsonFormatter,
    TaskExecutor,
    TaskPrefetcher,
    WorkerProcess,
    WorkerThread,
    configure_logging,
    handler,
)


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
        id=str(uuid.uuid7()),
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


def _make_worker(
    *,
    max_tasks: int | None = None,
    prefetch_count: int = 1,
    poll_interval: datetime.timedelta | None = None,
    poll_max_interval: datetime.timedelta | None = None,
) -> WorkerProcess:
    """Build an unstarted `WorkerProcess`."""
    options = {}
    if poll_interval is not None:
        options["poll_interval"] = poll_interval
    if poll_max_interval is not None:
        options["poll_max_interval"] = poll_max_interval
    return WorkerProcess(
        thread_count=1,
        max_tasks=max_tasks,
        prefetch_count=prefetch_count,
        backend_alias="default",
        queues=("default",),
        log_formatter=JsonFormatter(),
        **options,
    )


def _prefetched_thread(
    task_result: TaskResult,
    *,
    max_tasks: int = 1,
    backend: ThreadmillTaskBackend = default_task_backend,
) -> WorkerThread:
    """Build a worker thread whose prefetch buffer already holds one task."""
    worker = _make_worker(max_tasks=max_tasks)
    worker.lock = threading.Lock()
    worker.expired = threading.Event()
    prefetcher = TaskPrefetcher(worker=worker, backend=backend, prefetch_count=1)
    prefetcher.buffer(task_result)
    prefetcher.finished.set()
    worker.prefetcher = prefetcher
    return WorkerThread(worker=worker, index=0, backend=backend)


class StubPrefetchBackend:
    """Scripted backend stub for prefetcher tests without broker round-trips."""

    def __init__(self, *responses: list[TaskResult] | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[dict] = []

    def acquire(self, *queue_names, count=1, timeout=None, worker=""):
        """Return the next scripted batch or raise the next scripted error."""
        self.calls.append(
            {
                "queues": queue_names,
                "count": count,
                "timeout": timeout,
                "worker": worker,
            }
        )
        response = self.responses.pop(0) if self.responses else TimeoutError("drained")
        if isinstance(response, Exception):
            raise response
        return response


class StubPrefetchWorker:
    """Minimal worker stub exposing the state the prefetcher reads."""

    @property
    def task_wait_timeout(self) -> datetime.timedelta:
        """Follow the worker's timeout, so patching it reaches this stub too."""
        return WorkerProcess.task_wait_timeout

    def __init__(self, *, exit_empty: bool = False) -> None:
        self.pid = 4242
        self.queues = ("default",)
        self.exit_empty = exit_empty
        self.expired = threading.Event()
        self.shutdown_requested = threading.Event()


class CountingEvent(threading.Event):
    """Event that counts is_set() checks to observe consumer poll cycles."""

    def __init__(self) -> None:
        super().__init__()
        self.checks = 0

    def is_set(self) -> bool:
        self.checks += 1
        return super().is_set()


def _make_prefetcher(
    backend: StubPrefetchBackend,
    *,
    prefetch_count: int = 4,
    exit_empty: bool = False,
) -> TaskPrefetcher:
    """Build a prefetcher over a scripted stub backend and worker."""
    return TaskPrefetcher(
        worker=StubPrefetchWorker(exit_empty=exit_empty),
        backend=backend,
        prefetch_count=prefetch_count,
    )


class TestJsonFormatter:
    """Tests for the JsonFormatter class."""

    def test_format__returns_json_payload(self) -> None:
        """Return a JSON object with structured record fields."""
        record = logging.LogRecord(
            "multiprocessing",
            logging.INFO,
            __file__,
            1,
            "Task successful %r",
            ("abc",),
            None,
        )
        payload = json.loads(JsonFormatter().format(record))
        assert set(payload) == {
            "created_at",
            "level",
            "logger",
            "message",
            "process",
            "process_name",
            "thread",
        }
        assert payload["message"] == "Task successful 'abc'"
        assert payload["level"] == "INFO"
        assert payload["logger"] == "multiprocessing"
        assert payload["thread"] == "MainThread"
        assert (
            datetime.datetime.fromisoformat(payload["created_at"]).utcoffset()
            == datetime.datetime.fromtimestamp(
                record.created, tz=timezone.get_current_timezone()
            ).utcoffset()
        )

    def test_format__includes_extra_attributes(self) -> None:
        """Include extra record attributes in the JSON payload."""
        record = logging.LogRecord(
            "multiprocessing",
            logging.INFO,
            __file__,
            1,
            "Task successful %r",
            ("abc",),
            None,
        )
        record.request_id = "abc"
        record.duration_ms = 42
        payload = json.loads(JsonFormatter().format(record))
        assert payload["request_id"] == "abc"
        assert payload["duration_ms"] == 42

    def test_format__includes_exception_traceback(self) -> None:
        """Include the exception traceback when the record carries exc_info."""
        try:
            raise ValueError("boom")
        except ValueError:
            record = logging.LogRecord(
                "multiprocessing",
                logging.ERROR,
                __file__,
                1,
                "Task failed %r",
                ("abc",),
                sys.exc_info(),
            )
        payload = json.loads(JsonFormatter().format(record))
        assert "ValueError: boom" in payload["exception"]


class TestConfigureLogging:
    """Tests for the configure_logging function."""

    @pytest.fixture(autouse=True)
    def restore_log_formatter(self):
        """Restore the shared log formatter after each test."""
        formatter = handler.formatter
        yield
        handler.setFormatter(formatter)

    def test_configure_logging__installs_handler_on_root(self):
        """Route the records of every logger through the shared handler."""
        formatter = logging.Formatter("%(levelname)s %(message)s")
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.ERROR)
        configure_logging(formatter)
        assert root_logger.handlers == [handler]
        assert root_logger.level == logging.ERROR
        assert handler.formatter is formatter

    def test_configure_logging__replaces_foreign_handlers(self):
        """Replace handlers of existing loggers so their records reach the root."""
        task_logger = logging.getLogger("tests.testapp.tasks")
        task_logger.addHandler(logging.NullHandler())
        task_logger.propagate = False
        configure_logging(JsonFormatter())
        assert task_logger.handlers == []
        assert task_logger.propagate is True

    def test_configure_logging__keeps_placeholder_loggers(self):
        """Ignore placeholder entries in the logger registry."""
        logging.getLogger("tests.test_executor.placeholder.child")
        configure_logging(JsonFormatter())
        assert isinstance(
            logging.root.manager.loggerDict["tests.test_executor.placeholder"],
            logging.PlaceHolder,
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

    @pytest.mark.parametrize("prefetch_count", [None, 0])
    def test_post_init__derives_prefetch_count_from_threads(self, prefetch_count):
        """__post_init__ defaults the prefetch count to four tasks per thread."""
        executor = TaskExecutor(
            backend=default_task_backend,
            prefetch_count=prefetch_count,
            threads=3,
            queues=("default",),
        )
        assert executor.prefetch_count == 12

    def test_post_init__keeps_explicit_prefetch_count(self):
        """__post_init__ keeps an explicitly configured prefetch count."""
        executor = TaskExecutor(
            backend=default_task_backend, prefetch_count=7, queues=("default",)
        )
        assert executor.prefetch_count == 7

    @pytest.mark.parametrize("prefetch_count", [-1, -5])
    def test_post_init__floors_prefetch_count_at_one(self, prefetch_count):
        """__post_init__ floors a negative count, which would fetch nothing at all."""
        executor = TaskExecutor(
            backend=default_task_backend,
            prefetch_count=prefetch_count,
            queues=("default",),
        )
        assert executor.prefetch_count == 1

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

    def test_get_maximum_tasks_per_child__floors_at_one(self):
        """get_maximum_tasks_per_child never returns less than one task per child."""
        executor = TaskExecutor(
            backend=default_task_backend,
            max_tasks=2,
            max_tasks_jitter=0,
            threads=8,
            queues=("default",),
        )
        assert executor.get_maximum_tasks_per_child() == 1

    def test_create_worker_process__starts_worker(self):
        """create_worker_process creates and starts a WorkerProcess."""
        executor = TaskExecutor(backend=default_task_backend, queues=("default",))
        worker = executor.create_worker_process()
        assert worker.is_alive()
        assert worker.log_formatter is executor.log_formatter
        assert executor.prefetch_count == 4
        assert worker.prefetch_count == executor.prefetch_count
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
        assert handler.formatter is executor.log_formatter
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

    def test_run__routes_task_logs_to_stdout(self, capfd):
        """Emit task log records as JSON on standard output."""
        enqueued = default_task_backend.enqueue(log_message, args=["hello from task"])
        original_start_method = multiprocessing.get_start_method()
        # A forkserver worker inherits the stdout of the long-lived forkserver
        # instead of the file descriptor this fixture replaces, so its records
        # would never reach capfd.
        multiprocessing.set_start_method("spawn", force=True)
        try:
            TaskExecutor(
                backend=default_task_backend,
                workers=1,
                threads=1,
                queues=("default",),
                exit_empty=True,
            ).run()
        finally:
            multiprocessing.set_start_method(original_start_method, force=True)

        captured = capfd.readouterr()
        assert "hello from task" not in captured.err
        records = [
            json.loads(line)
            for line in captured.out.splitlines()
            if line.startswith("{")
        ]
        assert any(
            record["logger"] == "tests.testapp.tasks"
            and record["message"] == "hello from task"
            and record["level"] == "INFO"
            for record in records
        )
        assert (
            default_task_backend.get_result(enqueued.id).status
            is TaskResultStatus.SUCCESSFUL
        )

    def test_run__executes_model_task_in_spawned_worker(self):
        """run() executes a model-accessing task in a spawned worker process."""
        original_start_method = multiprocessing.get_start_method()
        multiprocessing.set_start_method("spawn", force=True)
        try:
            enqueued = default_task_backend.enqueue(count_users)
            executor = TaskExecutor(
                backend=default_task_backend,
                workers=1,
                threads=1,
                queues=("default",),
            )
            run_thread = threading.Thread(target=executor.run, daemon=True)
            run_thread.start()
            time.sleep(3)
            executor.shutdown()
            run_thread.join(timeout=5)
            assert not run_thread.is_alive()
            result = default_task_backend.get_result(enqueued.id)
            assert result.status == TaskResultStatus.SUCCESSFUL
        finally:
            multiprocessing.set_start_method(original_start_method, force=True)

    @pytest.mark.django_db(transaction=True)
    def test_worker_acquires_updates_and_acknowledges(self):
        """Worker thread executes and acknowledges a prefetched task via its backend."""
        enqueued = default_task_backend.enqueue(echo, args=[42])
        (acquired,) = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="test-worker"
        )

        _prefetched_thread(acquired).run()

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

    @pytest.fixture(autouse=True)
    def restore_backend_poll_options(self):
        """Restore the backend poll options after each test."""
        backend = default_task_backend
        saved = (backend.poll_interval, backend.poll_max_interval)
        yield
        backend.poll_interval, backend.poll_max_interval = saved

    def test_run__applies_poll_overrides_to_backend(self):
        """Apply poll overrides to the backend before starting consumer threads."""
        backend = default_task_backend
        poll_interval = datetime.timedelta(seconds=0.02)
        poll_max_interval = datetime.timedelta(seconds=0.3)
        enqueued = backend.enqueue(echo, args=[1])
        worker = _make_worker(
            max_tasks=1,
            poll_interval=poll_interval,
            poll_max_interval=poll_max_interval,
        )
        worker.shutdown_requested.set()
        # The backend registry resolves one instance per thread; running in this
        # thread applies the overrides to the instance asserted on below.
        worker.run()
        assert backend.poll_interval == poll_interval
        assert backend.poll_max_interval == poll_max_interval
        assert backend.get_result(enqueued.id).status is TaskResultStatus.SUCCESSFUL

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

    def test_run__applies_log_formatter_and_stops(self):
        """run() applies the log formatter and returns when shutdown is requested."""
        worker = _make_worker()
        worker.shutdown_requested.set()
        run_thread = threading.Thread(target=worker.run)
        run_thread.start()
        run_thread.join(timeout=5)
        assert not run_thread.is_alive()
        assert handler.formatter is worker.log_formatter

    def test_run__raises_when_prefetcher_fails(self, monkeypatch):
        """Re-raise a prefetch failure so the worker process exits non-zero."""
        thread_failures = []
        monkeypatch.setattr(threading, "excepthook", thread_failures.append)
        monkeypatch.setattr(
            "threadmill.executor.WorkerProcess.task_wait_timeout",
            datetime.timedelta(seconds=0.01),
        )
        worker = WorkerProcess(
            thread_count=1,
            backend_alias="stub",
            queues=("default",),
            log_formatter=JsonFormatter(),
        )

        with pytest.raises(RuntimeError, match="backend unavailable"):
            worker.run()

        assert worker.prefetcher is not None
        assert isinstance(worker.prefetcher.failure, RuntimeError)
        assert worker.prefetcher.finished.is_set()
        assert len(thread_failures) == 1
        assert isinstance(thread_failures[0].exc_value, RuntimeError)

    def test_run__child_exits_non_zero_on_prefetch_failure(self, capfd):
        """A child whose prefetcher failed exits non-zero and logs the failure."""
        worker = WorkerProcess(
            thread_count=1,
            backend_alias="stub",
            queues=("default",),
            log_formatter=JsonFormatter(),
        )

        worker.start()
        worker.join(timeout=5)
        if worker.is_alive():
            worker.terminate()

        assert worker.exitcode == 1
        assert "exits after a fetch failure" in capfd.readouterr().out


class TestWorkerThread:
    """Tests for the WorkerThread class."""

    pytestmark = pytest.mark.django_db(transaction=True)

    @pytest.fixture(autouse=True)
    def fast_task_wait(self, monkeypatch):
        """Shorten the buffer wait so drained run() tests return quickly."""
        monkeypatch.setattr(
            "threadmill.executor.WorkerProcess.task_wait_timeout",
            datetime.timedelta(seconds=0.01),
        )

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

    def test_execute_task_result__logs_info_records(self, monkeypatch):
        """Log task start and success records at INFO level."""
        stream = io.StringIO()
        monkeypatch.setattr(handler, "stream", stream)
        configure_logging(JsonFormatter())
        logging.getLogger().setLevel(logging.INFO)
        task_result = _task_result(log_message, "hello")
        WorkerThread(
            worker=_make_worker(), index=0, backend=default_task_backend
        ).execute_task_result(task_result)

        progress_levels = {
            record["message"]: record["level"]
            for record in map(json.loads, stream.getvalue().splitlines())
            if record["logger"] == "multiprocessing"
        }
        assert (
            progress_levels[
                f"Executing task '{task_result.id}@{log_message.module_path}'"
            ]
            == "INFO"
        )
        assert (
            progress_levels[
                f"Task '{task_result.id}@{log_message.module_path}' succeeded"
            ]
            == "INFO"
        )

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

    def test_run__requeues_failed_task_with_retry(self) -> None:
        """run() requeues a FAILED task when retry_delay returns a timedelta."""
        enqueued = default_task_backend.enqueue(boom_with_retry, args=[])
        (acquired,) = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="test-worker"
        )

        _prefetched_thread(acquired).run()

        # The task should have been requeued to the deferred set, not acknowledged
        from threadmill.backends.redis import RedisTaskBackend

        deferred_key = RedisTaskBackend.DEFERRED_KEY.format(
            prefix=default_task_backend.key_prefix, queue_name="default"
        )
        assert default_task_backend.client.zscore(deferred_key, enqueued.id) is not None

    def test_run__acknowledges_failed_task_without_retry(self) -> None:
        """run() acknowledges a FAILED task when retry_delay returns None."""
        enqueued = default_task_backend.enqueue(boom_no_retry, args=[])
        (acquired,) = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="test-worker"
        )

        _prefetched_thread(acquired).run()

        # The task should be acknowledged (FAILED result, not in deferred)
        from threadmill.backends.redis import RedisTaskBackend

        deferred_key = RedisTaskBackend.DEFERRED_KEY.format(
            prefix=default_task_backend.key_prefix, queue_name="default"
        )
        assert default_task_backend.client.zscore(deferred_key, enqueued.id) is None

        result = default_task_backend.get_result(enqueued.id)
        assert result.status == TaskResultStatus.FAILED

    def test_run__acknowledges_failed_task_when_callback_raises(self) -> None:
        """run() acknowledges a FAILED task when the retry callback raises."""
        enqueued = default_task_backend.enqueue(boom_retry_raises, args=[])
        (acquired,) = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="test-worker"
        )

        _prefetched_thread(acquired).run()

        from threadmill.backends.redis import RedisTaskBackend

        deferred_key = RedisTaskBackend.DEFERRED_KEY.format(
            prefix=default_task_backend.key_prefix, queue_name="default"
        )
        assert default_task_backend.client.zscore(deferred_key, enqueued.id) is None

        result = default_task_backend.get_result(enqueued.id)
        assert result.status == TaskResultStatus.FAILED

    def test_run__returns_when_buffer_drained_and_finished(self) -> None:
        """run() returns once the buffer is drained and the prefetcher finished."""
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()
        prefetcher = TaskPrefetcher(
            worker=worker, backend=default_task_backend, prefetch_count=1
        )
        prefetcher.finished.set()
        worker.prefetcher = prefetcher

        WorkerThread(worker=worker, index=0, backend=default_task_backend).run()

    def test_run__waits_for_buffer_until_finished(self) -> None:
        """run() keeps polling an empty buffer while the prefetcher is alive."""
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()
        prefetcher = TaskPrefetcher(
            worker=worker, backend=default_task_backend, prefetch_count=1
        )
        finished = CountingEvent()
        prefetcher.finished = finished
        worker.prefetcher = prefetcher

        reader = threading.Thread(
            target=WorkerThread(
                worker=worker, index=0, backend=default_task_backend
            ).run,
            daemon=True,
        )
        reader.start()
        deadline = time.monotonic() + 2
        while finished.checks < 1 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert reader.is_alive()

        finished.set()
        reader.join(timeout=1)

        assert not reader.is_alive()
        assert finished.checks >= 2


class TestTaskPrefetcher:
    """Tests for the TaskPrefetcher thread."""

    def test_run__buffers_until_stop_requested(self):
        """Fill the buffer from batches and poll again after an empty acquire."""
        first = _task_result(echo, 1)
        second = _task_result(echo, 2)
        backend = StubPrefetchBackend(
            TimeoutError("drained"), [first, second], TimeoutError("drained")
        )
        prefetcher = _make_prefetcher(backend, prefetch_count=4)
        thread = threading.Thread(target=prefetcher.run)
        thread.start()
        try:
            deadline = time.monotonic() + 2
            while prefetcher.task_buffer.qsize() < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            assert prefetcher.task_buffer.qsize() == 2
        finally:
            prefetcher.stop_requested.set()
            thread.join(timeout=2)
        assert not thread.is_alive()
        assert prefetcher.finished.is_set()
        assert prefetcher.failure is None
        assert [
            prefetcher.task_buffer.get_nowait().task_result.id for _ in range(2)
        ] == [first.id, second.id]
        assert all(call["count"] == 4 for call in backend.calls)

    def test_run__stops_on_expired(self):
        """Return immediately when the worker has already expired."""
        backend = StubPrefetchBackend()
        prefetcher = _make_prefetcher(backend)

        prefetcher.worker.expired.set()
        prefetcher.run()

        assert prefetcher.finished.is_set()
        assert prefetcher.failure is None
        assert backend.calls == []

    def test_run__stops_on_stop_requested(self):
        """Return immediately when a stop was requested before the loop starts."""
        backend = StubPrefetchBackend()
        prefetcher = _make_prefetcher(backend)

        prefetcher.stop_requested.set()
        prefetcher.run()

        assert prefetcher.finished.is_set()
        assert backend.calls == []

    def test_run__acquires_a_full_buffer(self):
        """Acquire a full buffer; the worker budget only stops the loop."""
        task_result = _task_result(echo, 1)
        backend = StubPrefetchBackend([task_result])
        prefetcher = _make_prefetcher(backend, prefetch_count=5)

        prefetcher.worker.shutdown_requested.set()
        prefetcher.run()

        assert backend.calls[0]["count"] == 5
        assert prefetcher.task_buffer.get_nowait().task_result.id == task_result.id
        assert len(backend.calls) == 1
        assert prefetcher.finished.is_set()

    def test_run__stops_after_batch_when_shutdown_requested(self):
        """Buffer one final batch, then stop when a shutdown was requested."""
        first = _task_result(echo, 1)
        second = _task_result(echo, 2)
        backend = StubPrefetchBackend([first, second])
        prefetcher = _make_prefetcher(backend, prefetch_count=2)

        prefetcher.worker.shutdown_requested.set()
        prefetcher.run()

        assert [
            prefetcher.task_buffer.get_nowait().task_result.id for _ in range(2)
        ] == [first.id, second.id]
        assert len(backend.calls) == 1
        assert prefetcher.finished.is_set()

    def test_run__stops_on_empty_when_exit_empty(self):
        """Break out of the fetch loop when the queue drained and exit_empty is set."""
        backend = StubPrefetchBackend(TimeoutError("drained"))
        prefetcher = _make_prefetcher(backend, exit_empty=True)

        prefetcher.run()

        assert len(backend.calls) == 1
        assert prefetcher.finished.is_set()
        assert prefetcher.failure is None

    def test_run__stops_on_empty_when_shutdown_requested(self):
        """Break out of the fetch loop when a shutdown was requested."""
        backend = StubPrefetchBackend(queue.Empty("drained"))
        prefetcher = _make_prefetcher(backend)

        prefetcher.worker.shutdown_requested.set()
        prefetcher.run()

        assert len(backend.calls) == 1
        assert prefetcher.finished.is_set()

    def test_run__records_failure_and_reraises(self):
        """Record a fetch failure, mark the fetcher finished, and re-raise."""
        backend = StubPrefetchBackend(RuntimeError("backend unavailable"))
        prefetcher = _make_prefetcher(backend)

        with pytest.raises(RuntimeError, match="backend unavailable"):
            prefetcher.run()

        assert isinstance(prefetcher.failure, RuntimeError)
        assert prefetcher.finished.is_set()

    def test_buffer__returns_false_when_full_and_stop_requested(self):
        """Abandon a full buffer as soon as the prefetcher must stop."""
        buffered = _task_result(echo, 1)
        overflow = _task_result(echo, 2)
        backend = StubPrefetchBackend([overflow])
        prefetcher = _make_prefetcher(backend, prefetch_count=1)
        prefetcher.buffer(buffered)

        thread = threading.Thread(target=prefetcher.run)
        thread.start()
        try:
            time.sleep(0.2)
        finally:
            prefetcher.stop_requested.set()
            thread.join(timeout=3)

        assert not thread.is_alive()
        assert prefetcher.finished.is_set()
        assert prefetcher.failure is None
        assert prefetcher.task_buffer.qsize() == 1
        assert prefetcher.task_buffer.get_nowait().task_result.id == buffered.id

    def test_buffer__returns_true_when_space_available(self):
        """Buffer a task result when the queue has room."""
        task_result = _task_result(echo, 1)
        prefetcher = _make_prefetcher(StubPrefetchBackend())

        assert prefetcher.buffer(task_result) is True
        assert prefetcher.task_buffer.get_nowait().task_result.id == task_result.id

    def test_buffer__dispatches_highest_priority_first(self):
        """Hand out the highest priority task first and keep fetch order on ties."""
        low = _task_result(dataclasses.replace(echo, priority=1), 1)
        high = _task_result(dataclasses.replace(echo, priority=5), 2)
        middle = _task_result(dataclasses.replace(echo, priority=3), 3)
        later_high = _task_result(dataclasses.replace(echo, priority=5), 4)
        prefetcher = _make_prefetcher(StubPrefetchBackend())

        for task_result in (low, high, middle, later_high):
            assert prefetcher.buffer(task_result) is True

        assert [
            prefetcher.task_buffer.get_nowait().task_result.id for _ in range(4)
        ] == [high.id, later_high.id, middle.id, low.id]
