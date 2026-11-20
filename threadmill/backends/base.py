import collections.abc
import dataclasses
import datetime
import enum
import json
import logging
import threading
import typing
from abc import ABC
from traceback import format_exception

from django.core.serializers.json import DjangoJSONEncoder
from django.tasks import DEFAULT_TASK_QUEUE_NAME, Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import TaskContext, TaskError
from django.tasks.exceptions import InvalidTask
from django.utils.inspect import is_module_level_function
from django.utils.module_loading import import_string

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class RetryTask(Task):
    """
    Task with an optional retry callback for backoff scheduling.

    Usage:

        def retry_callback(context: TaskContext) -> datetime.timedelta | None:
            if context.attempt < 3:
                return datetime.timedelta(seconds=60 * (2 ** context.attempt))

        @task(retry=retry_callback)
        def my_task():
            ...

    """

    retry: collections.abc.Callable[[TaskContext], datetime.timedelta | None] | None = (
        None
    )

    @property
    def retry_path(self) -> tuple[str, tuple, dict[str, typing.Any]] | str | None:
        """Importable dotted path of the retry callback, or None."""
        if self.retry:
            if hasattr(self.retry, "deconstruct"):
                return self.retry.deconstruct()
            return f"{self.retry.__module__}.{self.retry.__qualname__}"
        return None

    @classmethod
    def _reconstruct(cls, kwargs):
        retry_path = kwargs.pop("retry", None)
        if retry_path is not None:
            kwargs["retry"] = cls._import_retry(retry_path, kwargs["func"])
        return super()._reconstruct(kwargs)

    @staticmethod
    def _import_retry(retry_path, func_path):
        """Import a stored retry callback, falling back to the task definition."""
        try:
            try:
                path, pos_args, kw_args = retry_path
            except ValueError:
                return import_string(retry_path)
            return import_string(path)(*pos_args, **kw_args)
        except ImportError:
            return getattr(import_string(func_path), "retry", None)

    def __reduce__(self):
        reconstructor, (kwargs,) = super().__reduce__()
        kwargs["retry"] = self.retry_path
        return (reconstructor, (kwargs,))


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class UnreadablePayload:
    """A stored payload that could not be read back into a task result."""

    task_id: str
    queue_name: str
    payload: str
    error: BaseException


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class ThreadmillTaskResult(TaskResult):
    lease_token: str | None = None

    @classmethod
    def from_result(
        cls, task_result: TaskResult, *, lease_token: str | None
    ) -> ThreadmillTaskResult:
        return cls(
            **{
                field.name: getattr(task_result, field.name)
                for field in dataclasses.fields(TaskResult)
                if field.init
            },
            lease_token=lease_token,
        )


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


class TelemetryDirection(enum.Enum):
    """Direction of a telemetry event: a task entering or leaving a queue."""

    INGRESS = "ingress"
    EGRESS = "egress"


@dataclasses.dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """A single ingress/egress event published by a worker via pub/sub."""

    direction: TelemetryDirection
    queue_name: str


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
        if isinstance(o, TaskResult):
            return {
                field.name: getattr(o, field.name)
                for field in dataclasses.fields(TaskResult)
            }
        if isinstance(o, TaskError):
            return {
                field.name: getattr(o, field.name)
                for field in dataclasses.fields(TaskError)
            }
        if isinstance(o, RetryTask):
            return {
                field.name: getattr(o, field.name)
                for field in dataclasses.fields(RetryTask)
                if field.name not in {"func", "retry"}
            } | {"func": o.module_path, "retry": o.retry_path}
        return super().default(o)


class ThreadmillTaskBackend(BaseTaskBackend, ABC):
    """Interface for task queues to be processed by the executor."""

    task_class = RetryTask
    supports_async_task = True
    supports_get_result = True
    broker_class: type[Broker] | None = None

    result_ttl: datetime.timedelta | None = None

    @staticmethod
    def serialize_task_result(task_result: TaskResult) -> str:
        return json.dumps(task_result, cls=TaskResultEncoder)

    @classmethod
    def deserialize_task_result(cls, data: str) -> TaskResult:
        """Read a stored payload back into a task result."""

        def _object_hook(d: dict) -> dict | TaskResult:
            if "task" in d and isinstance(d["task"], dict) and "func" in d["task"]:
                task_data = d["task"]
                task_data["run_after"] = _parse_datetime(task_data["run_after"])
                status = TaskResultStatus(d["status"])
                d["task"] = cls._reconstruct_task(task_data, status)
                d["status"] = status
                d["errors"] = [TaskError(**error) for error in d["errors"]]
                return_value = d.pop("_return_value", None)
                for key, value in d.items():
                    d[key] = _parse_datetime(value)
                result = TaskResult(**d)
                object.__setattr__(result, "_return_value", return_value)
                return result
            return d

        return json.loads(data, object_hook=_object_hook)

    @classmethod
    def _reconstruct_task(cls, task_data: dict, status: TaskResultStatus) -> Task:
        """Rebuild the stored task, allowing a gone function only for FAILED records."""
        try:
            return cls.task_class._reconstruct(task_data)
        except (ImportError, ValueError) as exception:
            missing_reference = isinstance(exception, ImportError) or isinstance(
                exception.__cause__, (ImportError, AttributeError)
            )
            if status is not TaskResultStatus.FAILED or not missing_reference:
                raise
            return cls._create_missing_task(task_data)

    @classmethod
    def _create_missing_task(cls, task_data: dict) -> Task:
        """Build a placeholder task for a FAILED record whose function is gone."""
        func_path = task_data["func"]

        def missing_task(*args, **kwargs):
            message = f"Task {func_path!r} is no longer in the code base."
            raise RuntimeError(message)

        module_path, _, qualname = func_path.rpartition(".")
        missing_task.__module__ = module_path
        missing_task.__qualname__ = qualname
        task_data = {**task_data, "func": missing_task}
        task_data.pop("retry", None)
        return cls.task_class(**task_data)

    @classmethod
    def rebuild_failed_task_result(
        cls, data: str, error: BaseException, *, finished_at: datetime.datetime
    ) -> TaskResult | None:
        """Rebuild a stored payload as a FAILED result, or None when it cannot be read."""
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict):
                return None
            task_data = payload.get("task")
            if not isinstance(task_data, dict) or "func" not in task_data:
                return None
            task_data.pop("retry", None)
            payload["status"] = TaskResultStatus.FAILED.name
            payload["finished_at"] = finished_at.isoformat()
            payload["errors"] = [
                *payload.get("errors", []),
                dataclasses.asdict(cls.create_task_error(error)),
            ]
            return cls.deserialize_task_result(json.dumps(payload))
        except AttributeError, ImportError, KeyError, TypeError, ValueError:
            return None

    def validate_task(self, task: RetryTask) -> None:
        super().validate_task(task)
        if task.retry is not None and not (
            is_module_level_function(task.retry) or hasattr(task.retry, "deconstruct")
        ):
            message = (
                "Task's retry function must be defined at a module level or be a "
                "deconstructible callable."
            )
            raise InvalidTask(message)

    @staticmethod
    def create_task_error(exception: BaseException) -> TaskError:
        exception_type = type(exception)
        return TaskError(
            exception_class_path=f"{exception_type.__module__}.{exception_type.__qualname__}",
            traceback="".join(format_exception(exception)),
        )

    @staticmethod
    def retry_delay(task_result: TaskResult) -> datetime.timedelta | None:
        if task_result.task.retry:
            try:
                return task_result.task.retry(TaskContext(task_result=task_result))
            except Exception:
                logger.exception(
                    "Retry callback failed for task '%s@%s'",
                    task_result.id,
                    task_result.task.module_path,
                )
        return None

    def acquire(
        self,
        *queue_names: str,
        count: int = 1,
        timeout: datetime.timedelta | None = None,
        worker: str = "",
    ) -> list[ThreadmillTaskResult]:
        """
        Return and lock up to `count` tasks. The tasks stay in the queue.

        The method waits up to `timeout` for the first task. It fills the
        remaining tasks without a wait.

        Args:
            queue_names: The names of the queues to acquire tasks from.
            count: The maximum number of tasks to acquire, at least 1.
            timeout: The maximum time to wait for the first task. If None, the method waits without a limit.
            worker: The name of the worker thread that acquires the tasks.

        Raises:
            TimeoutError: The first task does not arrive within the specified timeout.
            queue.Empty: The first task is unavailable and the timeout is None.

        Returns:
            Between one and `count` leased task results in lease order. The list is
            never empty. Each result is a `ThreadmillTaskResult` that carries the
            lease token for its acknowledgement.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: TaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError

    def requeue(self, task_result: TaskResult, run_after: datetime.datetime) -> None:
        """
        Re-queue a failed task result for a retry attempt after `run_after`.

        Cleans up any persisted failed result so the method works both for
        in-flight retries (task still running) and inspector-driven requeues
        of already-failed tasks.
        """
        raise NotImplementedError

    def peek(
        self,
        queue_name: str = DEFAULT_TASK_QUEUE_NAME,
        *,
        status: TaskResultStatus,
        count: int = 1,
    ) -> collections.abc.Generator[TaskResult]:
        """
        Yield up to `count` tasks from a queue in the given status segment.

        Pending work comes back in queue order, so the first READY task is the
        next one a worker picks up. Finished results come back most recently
        finished first.

        Args:
            queue_name: The name of the queue to peek into.
            status: The status of the tasks to yield.
            count: The maximum number of tasks to yield. If 0, yield all available tasks.
        """
        raise NotImplementedError

    async def queue_stats(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        """Return per-queue task counts for all configured queues."""
        raise NotImplementedError

    async def worker_telemetry(
        self,
    ) -> collections.abc.AsyncIterator[TelemetryEvent]:
        """Yield ingress/egress telemetry events from the backend's pub/sub stream."""
        raise NotImplementedError

    def dequeue(self, task_result: TaskResult) -> None:
        """Delete a single task from its current status segment."""
        raise NotImplementedError

    def purge(self, queue_name: str) -> None:
        """
        Delete every task across all segments of a queue.

        Args:
            queue_name: The queue to purge.
        """
        raise NotImplementedError
