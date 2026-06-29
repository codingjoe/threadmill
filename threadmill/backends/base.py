from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import json
import threading
from abc import ABC

from django.core.serializers.json import DjangoJSONEncoder
from django.tasks import DEFAULT_TASK_QUEUE_NAME, Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskError
from django.utils.module_loading import import_string


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


@dataclasses.dataclass(kw_only=True, slots=True)
class QueueStats:
    """Telemetry for a single queue; ingress and egress are rolling per-minute counts."""

    ingress: int
    egress: int
    ready: int
    running: int
    deferred: int
    successful: int
    failed: int


@dataclasses.dataclass(kw_only=True, slots=True)
class QueueTelemetry:
    """Snapshot of stats across a backend's queues."""

    queues: dict[str, QueueStats]


class ThreadmillTaskBackend(BaseTaskBackend, ABC):
    """Interface for task queues to be processed by the executor."""

    supports_async_task = True
    supports_get_result = True
    broker_class: type[Broker] | None = None

    result_ttl: datetime.timedelta | None = None

    def queue_telemetry(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> QueueTelemetry:
        """Return a snapshot of stats for all configured queues.

        Ingress and egress are rolling counts over ``interval``, so they
        reflect recent traffic rather than a lifetime total.
        """
        raise NotImplementedError

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
        status: TaskResultStatus | None = None,
        count: int = 1,
    ) -> collections.abc.Generator[TaskResult, None, None]:
        """Yield tasks from a queue, optionally filtered by status."""
        raise NotImplementedError
