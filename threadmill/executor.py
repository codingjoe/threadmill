"""Task worker executor implementation."""

import asyncio
import dataclasses
import datetime
import json
import logging
import multiprocessing
import queue
import random
import socket
import sys
import threading
import time
import typing
from concurrent.futures import Future, ThreadPoolExecutor
from inspect import iscoroutinefunction
from queue import Empty, Full
from traceback import format_exception

import django
from django.core.serializers.json import DjangoJSONEncoder
from django.tasks import TaskResult, task_backends
from django.tasks.base import TaskContext, TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

if typing.TYPE_CHECKING:
    from .backends.base import Broker, ThreadmillTaskBackend, ThreadmillTaskResult


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


@dataclasses.dataclass(kw_only=True, slots=True)
class TaskExecutor:
    """Tasks consumed from shared joinable queues via process and thread pools."""

    backend: ThreadmillTaskBackend
    workers: int | None = None
    threads: int = 1
    max_tasks: int = 0
    max_tasks_jitter: int = 0
    prefetch_count: int | None = None
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
        self.prefetch_count = max(self.prefetch_count or self.thread_count * 4, 1)

    def get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            jitter = random.randint(0, self.max_tasks_jitter)  # noqa: S311
            return max((self.max_tasks + jitter) // self.thread_count, 1)

    def create_worker_process(self) -> WorkerProcess:
        """Create and start a new worker process."""
        worker = WorkerProcess(
            thread_count=self.thread_count,
            max_tasks=self.get_maximum_tasks_per_child(),
            prefetch_count=self.prefetch_count,
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
    """One worker process with one prefetcher and `thread_count` consumer threads."""

    task_wait_timeout: datetime.timedelta = datetime.timedelta(seconds=1)
    """How long a thread waits before it examines its stop condition again."""

    def __init__(
        self,
        *,
        thread_count: int,
        max_tasks: int | None = None,
        prefetch_count: int = 1,
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
        self.prefetch_count = prefetch_count
        self.backend_alias = backend_alias
        self.queues = queues
        self.exit_empty = exit_empty
        self.poll_interval = poll_interval
        self.poll_max_interval = poll_max_interval
        self.log_formatter = log_formatter
        self.task_count = 0
        self.lock: threading.Lock | None = None
        self.expired: threading.Event | None = None
        self.prefetcher: TaskPrefetcher | None = None

    def run(self) -> None:
        django.setup()
        configure_logging(self.log_formatter)
        logger.info("Starting worker process %s", self.name)
        self.lock = threading.Lock()
        self.expired = threading.Event()
        backend = task_backends[self.backend_alias]
        backend.poll_interval = self.poll_interval
        backend.poll_max_interval = self.poll_max_interval
        self.prefetcher = TaskPrefetcher(
            worker=self, backend=backend, prefetch_count=self.prefetch_count
        )
        self.prefetcher.start()
        consumer_threads = [
            WorkerThread(worker=self, index=index, backend=backend)
            for index in range(self.thread_count)
        ]
        for consumer_thread in consumer_threads:
            consumer_thread.start()
        join_timeout = (
            backend.result_ttl.total_seconds() if backend.result_ttl else None
        )
        for consumer_thread in consumer_threads:
            consumer_thread.join(join_timeout)
        self.prefetcher.stop_requested.set()
        self.prefetcher.join(join_timeout)
        completion = self.prefetcher.completion
        if completion.done() and (failure := completion.exception()) is not None:
            logger.error(
                "Worker process %s exits after a fetch failure",
                self.name,
                exc_info=failure,
            )
            raise SystemExit(1)

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


class TaskPrefetcher(threading.Thread):
    """The prefetcher thread of one worker process. It fills the task buffer."""

    def __init__(
        self,
        *,
        worker: WorkerProcess,
        backend: ThreadmillTaskBackend,
        prefetch_count: int,
    ) -> None:
        super().__init__(name=f"{socket.gethostname()}:{worker.pid}-fetch", daemon=True)
        self.worker = worker
        self.backend = backend
        self.prefetch_count = prefetch_count
        self.task_buffer: queue.Queue[ThreadmillTaskResult] = queue.Queue(
            maxsize=prefetch_count
        )
        self.completion: Future[None] = Future()
        self.stop_requested = threading.Event()

    def run(self) -> None:
        try:
            self.fill_buffer()
        except Exception as exception:
            self.completion.set_exception(exception)
        else:
            self.completion.set_result(None)

    def fill_buffer(self) -> None:
        """Fill the task buffer until the worker stops or the queue is drained."""
        while not self.stop_requested.is_set() and not self.worker.expired.is_set():
            try:
                batch = self.backend.acquire(
                    *self.worker.queues,
                    count=self.prefetch_count,
                    timeout=self.worker.task_wait_timeout,
                    worker=self.name,
                )
            except Empty, TimeoutError:
                if self.worker.exit_empty or self.worker.shutdown_requested.is_set():
                    break
            else:
                for task_result in batch:
                    if not self.buffer(task_result):
                        break
                if self.worker.shutdown_requested.is_set():
                    break

    def buffer(self, task_result: ThreadmillTaskResult) -> bool:
        """Buffer one task result. Return False when the prefetcher must stop."""
        while not self.stop_requested.is_set():
            try:
                self.task_buffer.put(
                    task_result, timeout=self.worker.task_wait_timeout.total_seconds()
                )
            except Full:
                continue
            return True
        return False


class WorkerThread(threading.Thread):
    """A worker thread that runs the tasks from the prefetch buffer."""

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
        prefetcher = self.worker.prefetcher
        while True:
            try:
                task_result = prefetcher.task_buffer.get(
                    timeout=self.worker.task_wait_timeout.total_seconds()
                )
            except Empty:
                if prefetcher.completion.done():
                    return
                continue

            try:
                result = self.execute_task_result(task_result)
                if (
                    result.status is TaskResultStatus.FAILED
                    and (delay := self.backend.retry_delay(result)) is not None
                ):
                    self.backend.requeue(result, timezone.now() + delay)
                else:
                    self.backend.acknowledge(result)
            finally:
                self.worker.record_task()

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
                errors=[
                    *task_result.errors,
                    self.backend.create_task_error(exception),
                ],
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
