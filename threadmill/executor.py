"""Task worker executor implementation."""

import asyncio
import dataclasses
import datetime
import json
import logging
import multiprocessing
import random
import socket
import sys
import threading
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from inspect import iscoroutinefunction
from queue import Empty
from traceback import format_exception

import django
from django.core.serializers.json import DjangoJSONEncoder
from django.tasks import TaskResult, task_backends
from django.tasks.base import TaskContext, TaskError, TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

if typing.TYPE_CHECKING:
    from .backends.base import Broker, ThreadmillTaskBackend


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    standard_attributes = frozenset(
        {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "created_at": datetime.datetime.fromtimestamp(
                record.created, tz=timezone.get_current_timezone()
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "process": record.process,
            "process_name": record.processName,
            "thread": record.threadName,
        } | {
            key: value
            for key, value in record.__dict__.items()
            if key not in self.standard_attributes
        }
        if record.exc_info:
            payload["exception"] = "".join(format_exception(*record.exc_info))
        return json.dumps(payload, cls=DjangoJSONEncoder)


logger = multiprocessing.get_logger()
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())


def configure_logging(formatter: logging.Formatter) -> None:
    """Route every log record of this process through the threadmill handler."""
    handler.setFormatter(formatter)
    for existing_logger in logging.root.manager.loggerDict.values():
        if isinstance(existing_logger, logging.Logger):
            existing_logger.handlers.clear()
            existing_logger.propagate = True
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


@dataclasses.dataclass(kw_only=True, slots=True)
class TaskExecutor:
    """Tasks consumed from shared joinable queues via process and thread pools."""

    backend: ThreadmillTaskBackend
    workers: int | None = None
    threads: int = 1
    max_tasks: int = 0
    max_tasks_jitter: int = 0
    poll_interval: datetime.timedelta = datetime.timedelta(seconds=0.01)
    poll_max_interval: datetime.timedelta = datetime.timedelta(seconds=1)
    is_publishing: bool = dataclasses.field(default=True, init=False)
    worker_processes: list[WorkerProcess] = dataclasses.field(
        default_factory=list, init=False
    )
    process_count: int = dataclasses.field(init=False)
    thread_count: int = dataclasses.field(init=False)
    queues: tuple[str]
    broker: Broker | None = dataclasses.field(default=None, init=False)
    exit_empty: bool = False
    log_formatter: logging.Formatter = dataclasses.field(default_factory=JsonFormatter)

    def __post_init__(self) -> None:
        """Initialize derived orchestration fields and queues."""
        self.process_count = self.workers or max(multiprocessing.cpu_count() - 1, 1)
        self.thread_count = max(self.threads, 1)

    def get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            return (
                self.max_tasks + random.randint(0, self.max_tasks_jitter)  # noqa: S311
            ) // self.thread_count

    def create_worker_process(self) -> WorkerProcess:
        """Create and start a new worker process."""
        worker = WorkerProcess(
            thread_count=self.thread_count,
            max_tasks=self.get_maximum_tasks_per_child(),
            backend_alias=self.backend.alias,
            queues=self.queues,
            exit_empty=self.exit_empty,
            poll_interval=self.poll_interval,
            poll_max_interval=self.poll_max_interval,
            log_formatter=self.log_formatter,
        )
        worker.start()
        return worker

    def run(self) -> None:
        """Start consuming tasks until shutdown is requested."""
        configure_logging(self.log_formatter)
        self.worker_processes = [
            self.create_worker_process() for _ in range(self.process_count)
        ]
        threads = [
            threading.Thread(target=self.maintain_worker_pool, daemon=True),
        ]
        if self.backend.broker_class:
            self.broker = self.backend.broker_class(self.backend)
            threads.append(self.broker)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def shutdown(self) -> None:
        """Stop queue consumption and terminate all worker processes."""
        logger.info("Shutting down task executor")
        if self.broker is not None:
            self.broker.shutdown()
        with ThreadPoolExecutor(max_workers=self.process_count) as executor:
            executor.map(lambda worker: worker.shutdown(), self.worker_processes)
        self.is_publishing = False

    def maintain_worker_pool(self) -> None:
        """Restart worker processes that have exited."""
        while self.is_publishing:
            all_dead = True
            for index, worker in enumerate(self.worker_processes):
                if worker.is_alive():
                    all_dead = False
                    continue
                worker.join(timeout=0)
                if self.exit_empty:
                    continue
                self.worker_processes[index] = self.create_worker_process()
            if all_dead and self.exit_empty:
                self.shutdown()
                return
            time.sleep(1)


class WorkerProcess(multiprocessing.Process):
    """Single worker process running thread_count consumer threads."""

    def __init__(
        self,
        *,
        thread_count: int,
        max_tasks: int | None = None,
        backend_alias: str = "",
        queues: tuple[str, ...] = (),
        exit_empty: bool = False,
        poll_interval: datetime.timedelta = datetime.timedelta(seconds=0.01),
        poll_max_interval: datetime.timedelta = datetime.timedelta(seconds=1),
        log_formatter: logging.Formatter,
    ) -> None:
        """Create process with dedicated thread pool for task execution."""
        self.shutdown_requested = multiprocessing.Event()
        super().__init__(daemon=True)
        self.thread_count = thread_count
        self.max_tasks = max_tasks
        self.backend_alias = backend_alias
        self.queues = queues
        self.exit_empty = exit_empty
        self.poll_interval = poll_interval
        self.poll_max_interval = poll_max_interval
        self.log_formatter = log_formatter
        self.task_count = 0
        self.lock: threading.Lock | None = None
        self.expired: threading.Event | None = None

    def run(self) -> None:
        """Start consumer execution inside this process."""
        django.setup()
        configure_logging(self.log_formatter)
        logger.info("Starting worker process %s", self.name)
        self.lock = threading.Lock()
        self.expired = threading.Event()
        backend = task_backends[self.backend_alias]
        backend.poll_interval = self.poll_interval
        backend.poll_max_interval = self.poll_max_interval
        consumer_threads = [
            WorkerThread(worker=self, index=index, backend=backend)
            for index in range(self.thread_count)
        ]
        for consumer_thread in consumer_threads:
            consumer_thread.start()
        for consumer_thread in consumer_threads:
            consumer_thread.join(
                backend.result_ttl.total_seconds() if backend.result_ttl else None
            )

    def record_task(self) -> None:
        """Record one processed task and stop when max_tasks is reached."""
        if self.max_tasks is None:
            return
        if self.lock is None or self.expired is None:
            return
        with self.lock:
            self.task_count += 1
            if self.task_count >= self.max_tasks:
                self.expired.set()

    def shutdown(self) -> None:
        """Request graceful worker stop and wait for process exit."""
        logger.info("Stopping worker process %s", self.name)
        self.shutdown_requested.set()
        self.join()


class WorkerThread(threading.Thread):
    """Single worker thread consuming tasks from the process queue."""

    def __init__(
        self,
        *,
        worker: WorkerProcess,
        index: int,
        backend: ThreadmillTaskBackend,
    ) -> None:
        """Create worker thread bound to process worker state."""
        super().__init__(name=f"{socket.gethostname()}:{worker.pid}-{index}")
        self.worker = worker
        self.backend = backend

    def run(self) -> None:
        """Start consuming tasks for this thread."""
        while self.worker.expired is None or not self.worker.expired.is_set():
            try:
                task_result = self.backend.acquire(
                    *self.worker.queues,
                    timeout=datetime.timedelta(seconds=1),
                    worker=self.name,
                )
            except Empty, TimeoutError:
                if self.worker.shutdown_requested.is_set() or self.worker.exit_empty:
                    return
                continue

            try:
                result = self.execute_task_result(task_result)
                if (
                    result.status is TaskResultStatus.FAILED
                    and (delay := self.retry_delay(result)) is not None
                ):
                    self.backend.requeue(result, timezone.now() + delay)
                else:
                    self.backend.acknowledge(result)
            finally:
                self.worker.record_task()

    @staticmethod
    def retry_delay(task_result: TaskResult) -> datetime.timedelta | None:
        """Return the retry delay for a failed task, or None to stop retrying."""
        if task_result.task.retry:
            try:
                return task_result.task.retry(TaskContext(task_result=task_result))
            except Exception:
                logger.exception(
                    "Retry callback failed for task '%s@%s'",
                    task_result.id,
                    task_result.task.module_path,
                )

    def execute_task_result(self, task_result: TaskResult) -> TaskResult:
        """Execute task from task result and update result lifecycle state."""
        logger.info(
            "Executing task '%s@%s'",
            task_result.id,
            task_result.task.module_path,
        )
        started_at = timezone.now()
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.RUNNING,
            started_at=task_result.started_at or started_at,
            last_attempted_at=started_at,
        )
        task_started.send(TaskExecutor, task_result=task_result)

        try:
            return_value = WorkerThread.call_task(task_result)
        except Exception as exception:
            task_result = dataclasses.replace(
                task_result,
                status=TaskResultStatus.FAILED,
                errors=[*task_result.errors, WorkerThread.create_task_error(exception)],
                finished_at=timezone.now(),
            )
            logger.exception(
                "Task '%s@%s' failed",
                task_result.id,
                task_result.task.module_path,
            )
        else:
            task_result = dataclasses.replace(
                task_result,
                status=TaskResultStatus.SUCCESSFUL,
                finished_at=timezone.now(),
            )
            object.__setattr__(
                task_result, "_return_value", normalize_json(return_value)
            )
            logger.info(
                "Task '%s@%s' succeeded",
                task_result.id,
                task_result.task.module_path,
            )
        finally:
            task_finished.send(TaskExecutor, task_result=task_result)

        return task_result

    @staticmethod
    def call_task(task_result: TaskResult) -> typing.Any:
        """Call a task with context when required."""
        task = task_result.task
        if task.takes_context:
            args = [TaskContext(task_result=task_result), *task_result.args]
        else:
            args = task_result.args
        if iscoroutinefunction(task.func):
            return asyncio.run(task.func(*args, **task_result.kwargs))
        return task.func(
            *args,
            **task_result.kwargs,
        )

    @staticmethod
    def create_task_error(exception: BaseException) -> TaskError:
        """Build a task error payload for failed execution."""
        exception_type = type(exception)
        return TaskError(
            exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
            traceback="".join(format_exception(exception)),
        )
