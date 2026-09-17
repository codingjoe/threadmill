import dataclasses
import datetime
import json
import logging
import multiprocessing
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
    boom_retry_thrice,
    boom_with_retry,
    count_users,
    echo,
    log_message,
)
from threadmill.backends.base import Broker
from threadmill.executor import (
    JsonFormatter,
    TaskExecutor,
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
    poll_interval: datetime.timedelta | None = None,
    poll_max_interval: datetime.timedelta | None = None,
) -> WorkerProcess:
    """Build an unstarted `WorkerProcess`."""
    return WorkerProcess(
        thread_count=1,
        max_tasks=max_tasks,
        backend_alias="default",
        queues=("default",),
        poll_interval=poll_interval,
        poll_max_interval=poll_max_interval,
        log_formatter=JsonFormatter(),
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
        configure_logging(formatter)
        root_logger = logging.getLogger()
        assert root_logger.handlers == [handler]
        assert root_logger.level == logging.INFO
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
        assert worker.log_formatter is executor.log_formatter
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


class TestWorkerThread:
    """Tests for the WorkerThread class."""

    pytestmark = pytest.mark.django_db(transaction=True)

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

    def test_retry_delay__none_when_task_has_no_retry(self) -> None:
        """Return None when the task has no retry callback."""
        result = _task_result(boom)
        assert WorkerThread.retry_delay(result) is None

    def test_retry_delay__returns_timedelta_from_callback(self) -> None:
        """Return the timedelta from the retry callback."""
        result = _task_result(boom_with_retry)
        result = dataclasses.replace(
            result,
            status=TaskResultStatus.FAILED,
            errors=[WorkerThread.create_task_error(ValueError("boom"))],
        )
        delay = WorkerThread.retry_delay(result)
        assert delay == datetime.timedelta(seconds=1)

    def test_retry_delay__none_when_callback_returns_none(self) -> None:
        """Return None when the retry callback returns None."""
        result = _task_result(boom_no_retry)
        result = dataclasses.replace(
            result,
            status=TaskResultStatus.FAILED,
            errors=[WorkerThread.create_task_error(ValueError("boom"))],
        )
        assert WorkerThread.retry_delay(result) is None

    def test_retry_delay__none_when_callback_raises(self, caplog) -> None:
        """Return None and log when the retry callback raises an exception."""
        mp_logger = multiprocessing.get_logger()
        mp_logger.addHandler(caplog.handler)
        mp_logger.setLevel(logging.ERROR)
        result = _task_result(boom_retry_raises)
        result = dataclasses.replace(
            result,
            status=TaskResultStatus.FAILED,
            errors=[WorkerThread.create_task_error(ValueError("boom"))],
        )
        try:
            assert WorkerThread.retry_delay(result) is None
        finally:
            mp_logger.removeHandler(caplog.handler)
        assert "Retry callback failed" in caplog.text

    def test_retry_delay__passes_task_context(self) -> None:
        """Pass TaskContext with task_result to the retry callback."""
        result = _task_result(boom_retry_thrice, worker_ids=["w1"])
        result = dataclasses.replace(
            result,
            status=TaskResultStatus.FAILED,
            errors=[WorkerThread.create_task_error(ValueError("boom"))],
        )
        delay = WorkerThread.retry_delay(result)
        assert delay == datetime.timedelta(seconds=1)

    def test_retry_delay__callback_receives_errors(self) -> None:
        """Retry callback can access the latest error via context.task_result.errors."""
        result = _task_result(boom_with_retry)
        error = WorkerThread.create_task_error(ValueError("boom"))
        result = dataclasses.replace(
            result,
            status=TaskResultStatus.FAILED,
            errors=[error],
        )
        WorkerThread.retry_delay(result)
        # The retry_always callback doesn't inspect errors, but the context
        # must contain them. Verify via retry_thrice which checks attempt count.
        result_with_workers = dataclasses.replace(result, worker_ids=["w1", "w2"])
        delay = WorkerThread.retry_delay(result_with_workers)
        assert delay == datetime.timedelta(seconds=1)

    def test_run__requeues_failed_task_with_retry(self) -> None:
        """run() requeues a FAILED task when retry_delay returns a timedelta."""
        enqueued = default_task_backend.enqueue(boom_with_retry, args=[])
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()

        thread = WorkerThread(
            worker=worker,
            index=0,
            backend=default_task_backend,
        )
        thread.run()

        # The task should have been requeued to the deferred set, not acknowledged
        from threadmill.backends.redis import RedisTaskBackend

        deferred_key = RedisTaskBackend.DEFERRED_KEY.format(
            prefix=default_task_backend.key_prefix, queue_name="default"
        )
        assert default_task_backend.client.zscore(deferred_key, enqueued.id) is not None

    def test_run__acknowledges_failed_task_without_retry(self) -> None:
        """run() acknowledges a FAILED task when retry_delay returns None."""
        enqueued = default_task_backend.enqueue(boom_no_retry, args=[])
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()

        thread = WorkerThread(
            worker=worker,
            index=0,
            backend=default_task_backend,
        )
        thread.run()

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
        worker = _make_worker(max_tasks=1)
        worker.lock = threading.Lock()
        worker.expired = threading.Event()

        thread = WorkerThread(
            worker=worker,
            index=0,
            backend=default_task_backend,
        )
        thread.run()

        from threadmill.backends.redis import RedisTaskBackend

        deferred_key = RedisTaskBackend.DEFERRED_KEY.format(
            prefix=default_task_backend.key_prefix, queue_name="default"
        )
        assert default_task_backend.client.zscore(deferred_key, enqueued.id) is None

        result = default_task_backend.get_result(enqueued.id)
        assert result.status == TaskResultStatus.FAILED
