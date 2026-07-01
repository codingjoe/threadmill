from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import json
import threading
from abc import ABC

import django
from django.core.serializers.json import DjangoJSONEncoder
from django.tasks import DEFAULT_TASK_QUEUE_NAME, Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.utils.module_loading import import_string

if django.VERSION == (6, 0):
    # https://github.com/django/django/commit/8c8b833d32c02d3ae6f43b04bb1e45968796b402
    @dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
    class Task(Task):
        @classmethod
        def _reconstruct(cls, kwargs):
            func_path = kwargs["func"]
            try:
                func = import_string(func_path)
                kwargs["func"] = func.func
            except (ImportError, AttributeError) as e:
                msg = f"Expected {func_path!r} to point to a Task instance."
                raise ValueError(msg) from e
            return cls(**kwargs)

        def __reduce__(self):
            kwargs = {f.name: getattr(self, f.name) for f in dataclasses.fields(self)}
            kwargs["func"] = self.module_path

            return (self.__class__._reconstruct, (kwargs,))


@dataclasses.dataclass(kw_only=True, slots=True)
class QueueCounts:
    """Point-in-time cardinality of each queue segment."""

    ready: int
    running: int
    deferred: int
    successful: int
    failed: int


@dataclasses.dataclass(kw_only=True, slots=True)
class QueueRates:
    """Rolling ingress/egress throughput over a time window (time-series data)."""

    interval: datetime.timedelta
    ingress: int
    egress: int


@dataclasses.dataclass(kw_only=True, slots=True)
class QueueStats:
    """Telemetry for a single queue: point-in-time counts plus rolling rates."""

    counts: QueueCounts
    rates: QueueRates


@dataclasses.dataclass(kw_only=True, slots=True)
class BackendTelemetry:
    """Snapshot of counts and rates across a backend's queues."""

    queues: dict[str, QueueStats]


@dataclasses.dataclass(kw_only=True, slots=True)
class WorkerProcessTelemetry:
    """Telemetry for a single worker process (one OS process).

    CPU and memory are tracked at the node level only, since per-process
    memory accounting is noisy and per-process CPU adds too much clutter.
    """

    name: str
    pid: int
    queues: tuple[str, ...]
    thread_count: int
    task_count: int
    tasks_per_minute: float
    sampled_at: datetime.datetime


@dataclasses.dataclass(kw_only=True, slots=True)
class NodeTelemetry:
    """Telemetry for a single node (host) running worker processes.

    ``process_count`` and ``thread_count`` aggregate across all worker
    processes on this node.  CPU and memory are sampled at the system level
    via ``psutil``.
    """

    hostname: str
    queues: tuple[str, ...]
    process_count: int
    thread_count: int
    cpu_percent: float
    memory_percent: float
    memory_bytes: int
    tasks_per_minute: float
    workers: dict[str, WorkerProcessTelemetry]
    sampled_at: datetime.datetime


@dataclasses.dataclass(kw_only=True, slots=True)
class WorkerTelemetry:
    """Snapshot of worker pool health across a backend's queues and nodes.

    ``nodes`` is keyed by hostname; each node carries system-level CPU/mem
    plus a dict of worker-process entries (name, pid, threads, throughput).
    ``queues`` maps each queue name to the hostnames listening on it, so the
    inspector can render a Queue -> Node selection tree.
    """

    nodes: dict[str, NodeTelemetry]
    queues: dict[str, tuple[str, ...]]
    sampled_at: datetime.datetime


class Broker(threading.Thread):
    """Backend maintenance thread launched by the task executor."""

    def __init__(
        self,
        backend: ThreadmillTaskBackend | None = None,
        *,
        interval: datetime.timedelta = datetime.timedelta(seconds=1),
    ) -> None:
        super().__init__(daemon=True)
        self.backend = backend
        self.interval = interval
        self.shutdown_requested = threading.Event()

    def main(self) -> None:
        """Perform one maintenance pass."""

    def run(self) -> None:
        while not self.shutdown_requested.wait(self.interval.total_seconds()):
            self.main()

    def shutdown(self) -> None:
        """Request graceful shutdown."""
        self.shutdown_requested.set()


def _parse_datetime(value: object) -> object:
    """Parse an ISO datetime string, returning the value unchanged if not parseable."""
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


class TaskResultEncoder(DjangoJSONEncoder):
    """JSON encoder for TaskResult and TaskError objects."""

    def default(self, o):
        if isinstance(o, (TaskResult, TaskError)):
            return {
                field.name: getattr(o, field.name)
                for field in dataclasses.fields(type(o))
            }
        if isinstance(o, Task):
            return {
                field.name: getattr(o, field.name)
                for field in dataclasses.fields(Task)
                if field.name != "func"  # Exclude the function object itself
            } | {"func": o.module_path}
        return super().default(o)


class ThreadmillTaskBackend(BaseTaskBackend, ABC):
    """Interface for task queues to be processed by the executor."""

    task_class = Task  # can be removed in the future when Django 6.0 support is dropped
    supports_async_task = True
    supports_get_result = True
    broker_class: type[Broker] | None = None

    result_ttl: datetime.timedelta | None = None

    @staticmethod
    def serialize_task_result(task_result: TaskResult) -> str:
        return json.dumps(task_result, cls=TaskResultEncoder)

    @classmethod
    def deserialize_task_result(cls, data: str) -> TaskResult:
        def _object_hook(d: dict) -> dict | TaskResult:
            if "task" in d and isinstance(d["task"], dict) and "func" in d["task"]:
                task_data = d["task"]
                func = import_string(task_data["func"])
                if isinstance(func, cls.task_class):
                    func = func.func
                d["task"] = cls.task_class(
                    func=func,
                    **{
                        field.name: _parse_datetime(task_data[field.name])
                        for field in dataclasses.fields(cls.task_class)
                        if field.name not in {"func", "takes_context"}
                        and field.name in task_data
                    },
                )
                d["status"] = TaskResultStatus(d["status"])
                d["errors"] = [TaskError(**error) for error in d["errors"]]
                return_value = d.pop("_return_value", None)
                for key, value in d.items():
                    d[key] = _parse_datetime(value)
                result = TaskResult(**d)
                object.__setattr__(result, "_return_value", return_value)
                return result
            return d

        return json.loads(data, object_hook=_object_hook)

    def acquire(
        self,
        *queue_names: str,
        timeout: datetime.timedelta | None = None,
        worker: str = "",
    ) -> TaskResult:
        """
        Return and lock the next task to be processed without removing it from the queue.

        Args:
            queue_names: The names of the queues to acquire tasks from.
            timeout: The maximum time to wait for a task. If None, wait indefinitely.
            worker: The name of the worker thread acquiring the task.

        Raises:
            TimeoutError: If no task is available within the specified timeout.
            queue.Empty: If no task is available and timeout is None.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: TaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError

    def peek(
        self,
        queue_name: str = DEFAULT_TASK_QUEUE_NAME,
        *,
        status: TaskResultStatus,
        count: int = 1,
    ) -> collections.abc.Generator[TaskResult, None, None]:
        """
        Yield up to ``count`` tasks from a queue in the given status segment.

        Args:
            queue_name: The name of the queue to peek into.
            status: The status of the tasks to yield.
            count: The maximum number of tasks to yield. If 0, yield all available tasks.
        """
        raise NotImplementedError

    def telemetry(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        """Return a snapshot of stats for all configured queues.

        Args:
            interval: The time window for rolling rates.
        """
        raise NotImplementedError

    def publish_worker_telemetry(self, telemetry: WorkerTelemetry) -> None:
        """Publish a worker-pool telemetry snapshot to a shared store.

        Backends without a shared store implement this as a no-op so the
        worker command still runs without worker telemetry.
        """

    def worker_telemetry(self) -> WorkerTelemetry:
        """Return the latest worker-pool telemetry snapshot, or an empty one.

        Backends without a shared store return an empty snapshot so the
        inspector can render the worker view without raising.
        """
        return WorkerTelemetry(
            nodes={},
            queues={},
            sampled_at=datetime.datetime.now(tz=datetime.UTC),
        )
