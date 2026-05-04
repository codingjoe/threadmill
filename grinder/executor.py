"""Task worker executor implementation."""

from __future__ import annotations

import dataclasses
import datetime
import logging
import multiprocessing
import random
import socket
import threading
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from multiprocessing.queues import JoinableQueue
from queue import Empty
from traceback import format_exception

from django.tasks import TaskResult
from django.tasks.base import TaskContext, TaskError, TaskResultStatus
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone
from django.utils.json import normalize_json

if typing.TYPE_CHECKING:
    from .backends import AcknowledgeableTaskBackend


logger = multiprocessing.get_logger()
formatter = logging.Formatter(
    "%(levelname)s: %(asctime)s - pid=%(process)s - %(message)s"
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


@dataclasses.dataclass(kw_only=True, slots=True)
class TaskExecutor:
    """Consume tasks from a priority queue with process and thread pools."""

    backend: AcknowledgeableTaskBackend
    workers: int | None = None
    threads: int = 1
    max_tasks: int = 0
    max_tasks_jitter: int = 0
    task_timeout: datetime.timedelta = datetime.timedelta(hours=1)
    is_acquiring: bool = dataclasses.field(default=True, init=False)
    is_publishing: bool = dataclasses.field(default=True, init=False)
    worker_processes: list[WorkerProcess] = dataclasses.field(
        default_factory=list, init=False
    )
    process_count: int = dataclasses.field(init=False)
    thread_count: int = dataclasses.field(init=False)
    queues: tuple[str]
    shared_task_queue: multiprocessing.JoinableQueue[TaskResult] = dataclasses.field(
        init=False
    )
    processed_task_queue: multiprocessing.JoinableQueue[TaskResult] = dataclasses.field(
        init=False
    )
    exit_empty: bool = False

    def __post_init__(self) -> None:
        """Initialize derived orchestration fields and queues."""
        self.process_count = self.workers or max(multiprocessing.cpu_count() - 1, 1)
        self.thread_count = max(self.threads, 1)
        self.shared_task_queue = multiprocessing.JoinableQueue(
            maxsize=self.process_count * self.thread_count,
        )
        self.processed_task_queue = multiprocessing.JoinableQueue()

    def get_maximum_tasks_per_child(self) -> int | None:
        """Return worker recycling limit based on config and thread count."""
        if self.max_tasks:
            return (
                self.max_tasks + random.randint(0, self.max_tasks_jitter)  # noqa: S311
            ) // self.thread_count

    def create_worker_process(self) -> WorkerProcess:
        """Create and start a new worker process."""
        worker = WorkerProcess(
            self.shared_task_queue,
            self.processed_task_queue,
            self.thread_count,
            self.task_timeout,
            self.get_maximum_tasks_per_child(),
        )
        worker.start()
        return worker

    def run(self) -> None:
        """Start consuming tasks until shutdown is requested."""
        self.worker_processes = [
            self.create_worker_process() for _ in range(self.process_count)
        ]
        threads = [
            threading.Thread(target=self.acknowledge_tasks, daemon=True),
            threading.Thread(target=self.maintain_worker_pool, daemon=True),
            threading.Thread(target=self.acquire_tasks, daemon=True),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def acquire_tasks(self) -> None:
        """Buffer tasks in shared task queue."""
        while self.is_acquiring:
            try:
                work = self.backend.acquire(*self.queues)
            except Empty:
                if self.exit_empty:
                    logger.info("No more tasks to solve. Shutting down.")
                    self.shutdown()
                    return
                time.sleep(0.01)
            else:
                self.shared_task_queue.put(work)

    def acknowledge_tasks(self) -> None:
        """Acknowledge processed tasks and publish updated results in main process."""
        while self.is_publishing:
            try:
                task = self.processed_task_queue.get_nowait()
            except Empty:
                time.sleep(0.01)
            else:
                self.backend.acknowledge(task)
                self.processed_task_queue.task_done()

    def shutdown(self) -> None:
        """Stop queue consumption and terminate all worker processes."""
        logger.info("Shutting down task executor")
        self.is_acquiring = False
        with suppress(ValueError):
            self.shared_task_queue.join()
        with suppress(ValueError):
            self.processed_task_queue.join()
        with ThreadPoolExecutor(max_workers=self.process_count) as executor:
            executor.map(lambda worker: worker.shutdown(), self.worker_processes)
        self.is_publishing = False

    def maintain_worker_pool(self) -> None:
        """Restart worker processes that have exited."""
        while self.is_publishing:
            for index, worker in enumerate(self.worker_processes):
                if worker.is_alive():
                    continue
                worker.join(timeout=0)
                self.worker_processes[index] = self.create_worker_process()
            time.sleep(1)


class WorkerProcess(multiprocessing.Process):
    """Single worker process running thread_count consumer threads."""

    def __init__(
        self,
        task_queue: JoinableQueue[TaskResult],
        processed_task_queue: JoinableQueue[TaskResult],
        thread_count: int,
        task_timeout: datetime.timedelta,
        max_tasks: int | None = None,
    ) -> None:
        """Create process with dedicated thread pool for task execution."""
        self.shutdown_requested = multiprocessing.Event()
        super().__init__(daemon=True)
        self.task_queue = task_queue
        self.processed_task_queue = processed_task_queue
        self.thread_count = thread_count
        self.task_timeout = task_timeout
        self.max_tasks = max_tasks
        self.task_count = 0
        self.lock: threading.Lock | None = None
        self.expired: threading.Event | None = None

    def run(self) -> None:
        """Start consumer execution inside this process."""
        logger.info("Starting worker process %s", self.name)
        self.lock = threading.Lock()
        self.expired = threading.Event()
        consumer_threads = [
            WorkerThread(worker=self, index=index) for index in range(self.thread_count)
        ]
        for consumer_thread in consumer_threads:
            consumer_thread.start()
        for consumer_thread in consumer_threads:
            consumer_thread.join(self.task_timeout.total_seconds())

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
    ) -> None:
        """Create worker thread bound to process worker state."""
        super().__init__(name=f"{socket.gethostname()}:{worker.pid}-{index}")
        self.worker = worker

    def run(self) -> None:
        """Start consuming tasks for this thread."""
        while self.worker.expired is None or not self.worker.expired.is_set():
            if (
                self.worker.shutdown_requested.is_set()
                and self.worker.task_queue.empty()
            ):
                return
            try:
                task_result = self.worker.task_queue.get(timeout=1.0)
            except Empty:
                if self.worker.shutdown_requested.is_set():
                    return
                continue
            try:
                self.worker.processed_task_queue.put(
                    self.execute_task_result(
                        task_result,
                    )
                )
            finally:
                self.worker.task_queue.task_done()
                self.worker.record_task()

    def execute_task_result(self, task_result: TaskResult) -> TaskResult:
        """Execute task from task result and update result lifecycle state."""
        logger.info("Executing task %r", task_result.id)
        started_at = timezone.now()
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.RUNNING,
            started_at=started_at,
            last_attempted_at=started_at,
            worker_ids=[*task_result.worker_ids, self.name],
        )
        task_enqueued.send(TaskExecutor, task_result=task_result)
        task_started.send(TaskExecutor, task_result=task_result)

        try:
            return_value = WorkerThread.call_task(task_result)
        except Exception as exception:
            task_result = dataclasses.replace(
                task_result,
                status=TaskResultStatus.FAILED,
                errors=[*task_result.errors, WorkerThread.create_task_error(exception)],
            )
            logger.exception("Task failed %r", task_result.id)
        else:
            task_result = dataclasses.replace(
                task_result,
                status=TaskResultStatus.SUCCESSFUL,
            )
            object.__setattr__(
                task_result, "_return_value", normalize_json(return_value)
            )
            logger.info("Task successful %r", task_result.id)
        finally:
            task_result = dataclasses.replace(
                task_result,
                finished_at=timezone.now(),
            )
            task_finished.send(TaskExecutor, task_result=task_result)

        return task_result

    @staticmethod
    def call_task(task_result: TaskResult) -> typing.Any:
        """Call a task with context when required."""
        task = task_result.task
        if task.takes_context:
            return task.call(
                TaskContext(task_result=task_result),
                *task_result.args,
                **task_result.kwargs,
            )
        return task.call(*task_result.args, **task_result.kwargs)

    @staticmethod
    def create_task_error(exception: BaseException) -> TaskError:
        """Build a task error payload for failed execution."""
        exception_type = type(exception)
        return TaskError(
            exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
            traceback="".join(format_exception(exception)),
        )
