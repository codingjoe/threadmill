import datetime
import json
import logging
import pickle
import time
import uuid

import pytest
from django.tasks import (
    TaskResult,
    TaskResultStatus,
    TaskResultStatus as TRS,
)
from django.tasks.base import TaskContext, TaskError
from django.tasks.exceptions import InvalidTask
from django.utils import (
    timezone,
    timezone as tz,
)

from tests.testapp.tasks import (
    boom_no_retry,
    boom_retry_raises,
    boom_with_retry,
    echo,
    retry_always,
)
from threadmill.backends.base import (
    Broker,
    RetryTask,
    ThreadmillTaskBackend,
    _parse_datetime,
)
from threadmill.exceptions import AcknowledgementTimeout


class BackendDouble(ThreadmillTaskBackend):
    def enqueue(self, task, args, kwargs):
        return TaskResult(
            task=task,
            id=str(uuid.uuid7()),
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            backend=self.alias,
            errors=[],
            worker_ids=[],
            args=args,
            kwargs=kwargs,
        )


class TestThreadmillTaskBackend:
    """Tests for the ThreadmillTaskBackend class."""

    def test_acquire__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acquire API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acquire(
                timeout=datetime.timedelta(seconds=1)
            )

    def test_acknowledge__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acknowledge API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acknowledge(task_result=None)

    def test_requeue__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend requeue API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).requeue(
                task_result=None,
                run_after=timezone.now(),
            )

    def test_peek__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend peek_results API."""
        with pytest.raises(NotImplementedError):
            list(
                BackendDouble(alias="default", params={}).peek(
                    "default", status=TaskResultStatus.READY
                )
            )

    async def test_queue_stats__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend queue_stats API."""
        with pytest.raises(NotImplementedError):
            await BackendDouble(alias="default", params={}).queue_stats()

    def test_dequeue__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend dequeue API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).dequeue(task_result=None)

    def test_purge__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend purge_queue API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).purge("default")

    def test_validate_task__accepts_module_level_function(self) -> None:
        """validate_task accepts a module-level retry callback."""
        backend = BackendDouble(alias="default", params={})
        backend.validate_task(boom_with_retry)

    def test_validate_task__accepts_deconstructible_callable(self) -> None:
        """validate_task accepts a retry callback with a deconstruct method."""
        backend = BackendDouble(alias="default", params={})
        task = RetryTask(func=echo.func, retry=DeconstructibleRetry())
        backend.validate_task(task)

    def test_validate_task__accepts_none_retry(self) -> None:
        """validate_task accepts a task with retry=None."""
        backend = BackendDouble(alias="default", params={})
        task = RetryTask(func=echo.func)
        backend.validate_task(task)

    def test_validate_task__rejects_lambda(self) -> None:
        """Reject a non-module-level retry callback during task creation."""
        with pytest.raises(InvalidTask):
            RetryTask(
                func=echo.func,
                retry=lambda ctx: None,
            )

    def test_create_task_error__builds_task_error(self) -> None:
        """create_task_error builds a TaskError with exception info."""
        error = ThreadmillTaskBackend.create_task_error(RuntimeError("test error"))
        assert "RuntimeError" in error.exception_class_path
        assert "test error" in error.traceback

    def test_retry_delay__none_when_task_has_no_retry(self) -> None:
        """Return None when the task has no retry callback."""
        assert ThreadmillTaskBackend.retry_delay(_task_result_with_retry(echo)) is None

    def test_retry_delay__returns_timedelta_from_callback(self) -> None:
        """Return the timedelta from the retry callback."""
        assert ThreadmillTaskBackend.retry_delay(
            _task_result_with_retry(
                boom_with_retry,
                errors=[ThreadmillTaskBackend.create_task_error(ValueError("boom"))],
            )
        ) == datetime.timedelta(seconds=1)

    def test_retry_delay__none_when_callback_returns_none(self) -> None:
        """Return None when the retry callback returns None."""
        result = _task_result_with_retry(
            boom_no_retry,
            errors=[ThreadmillTaskBackend.create_task_error(ValueError("boom"))],
        )
        assert ThreadmillTaskBackend.retry_delay(result) is None

    def test_retry_delay__none_when_callback_raises(self, caplog) -> None:
        """Return None and log when the retry callback raises an exception."""
        result = _task_result_with_retry(
            boom_retry_raises,
            errors=[ThreadmillTaskBackend.create_task_error(ValueError("boom"))],
        )
        with caplog.at_level(logging.ERROR, logger="threadmill.backends.base"):
            assert ThreadmillTaskBackend.retry_delay(result) is None
        assert "Retry callback failed" in caplog.text

    def test_retry_delay__callback_receives_errors(self) -> None:
        """Retry callback receives a context carrying the accumulated errors."""
        error = ThreadmillTaskBackend.create_task_error(ValueError("the latest error"))
        result = _task_result_with_retry(
            RetryTask(func=echo.func, retry=record_context),
            errors=[error],
        )
        ThreadmillTaskBackend.retry_delay(result)
        assert received_contexts[-1].task_result.errors == [error]

    def test_serialize_task_result__encodes_retry_as_path(self) -> None:
        """TaskResultEncoder serializes retry as a dotted path."""
        result = _task_result_with_retry(boom_with_retry)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        assert "tests.testapp.tasks.retry_always" in data

    def test_serialize_task_result__omits_retry_when_none(self) -> None:
        """TaskResultEncoder omits retry when it is None."""
        task = RetryTask(func=echo.func)
        result = TaskResult(
            task=task,
            id=str(uuid.uuid7()),
            status=TRS.READY,
            enqueued_at=tz.now(),
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            backend="default",
            errors=[],
            worker_ids=[],
            args=[],
            kwargs={},
        )
        data = ThreadmillTaskBackend.serialize_task_result(result)
        decoded = json.loads(data)
        assert decoded["task"]["retry"] is None

    def test_serialize_task_result__raises_type_error_for_unknown_object(
        self,
    ) -> None:
        """TaskResultEncoder raises TypeError for objects it cannot serialize."""
        with pytest.raises(TypeError):
            ThreadmillTaskBackend.serialize_task_result(object())

    def test_deserialize_task_result__roundtrips_retry_callback(self) -> None:
        """deserialize_task_result restores the retry callable from path."""
        result = _task_result_with_retry(boom_with_retry)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert restored.task.retry is retry_always
        assert restored.task.retry_path == "tests.testapp.tasks.retry_always"

    def test_deserialize_task_result__roundtrips_without_retry(self) -> None:
        """deserialize_task_result works when retry is None."""
        result = _task_result_with_retry(echo)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert restored.task.retry is None

    def test_deserialize_task_result__roundtrips_deconstructible_retry(self) -> None:
        """deserialize_task_result restores a deconstructible retry callable."""
        task = RetryTask(func=echo.func, retry=DeconstructibleRetry(seconds=5))
        result = _task_result_with_retry(task)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert isinstance(restored.task.retry, DeconstructibleRetry)
        assert restored.task.retry.seconds == 5

    def test_deserialize_task_result__preserves_errors(self) -> None:
        """deserialize_task_result preserves accumulated errors."""
        error = TaskError(
            exception_class_path="ValueError",
            traceback="traceback",
        )
        result = _task_result_with_retry(boom_with_retry, errors=[error])
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert len(restored.errors) == 1
        assert restored.errors[0].exception_class_path == "ValueError"

    def test_deserialize_task_result__preserves_return_value(self) -> None:
        """deserialize_task_result preserves the _return_value attribute."""
        result = _task_result_with_retry(echo)
        object.__setattr__(result, "_return_value", 42)
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert restored._return_value == 42


class TestRetryTask:
    """Tests for the RetryTask class."""

    def test_retry_path__none_when_no_retry(self) -> None:
        """Return None when retry callback is not set."""
        task = RetryTask(func=echo.func)
        assert task.retry_path is None

    def test_retry_path__returns_dotted_path(self) -> None:
        """Return the importable dotted path of the retry callback."""
        task = RetryTask(func=echo.func, retry=retry_always)
        assert task.retry_path == "tests.testapp.tasks.retry_always"

    def test_retry_path__returns_deconstruct_tuple(self) -> None:
        """Return the deconstruct() tuple for a deconstructible callable."""
        retry = DeconstructibleRetry(seconds=5)
        task = RetryTask(func=echo.func, retry=retry)
        assert task.retry_path == retry.deconstruct()

    def test_pickle__roundtrips_retry_callback(self) -> None:
        """Pickle preserves the retry callable by importable path."""
        result = _task_result_with_retry(boom_with_retry)
        restored = pickle.loads(pickle.dumps(result))  # noqa: S301
        assert restored.task.retry is retry_always

    def test_pickle__roundtrips_without_retry(self) -> None:
        """Pickle works when retry is None."""
        result = _task_result_with_retry(echo)
        restored = pickle.loads(pickle.dumps(result))  # noqa: S301
        assert restored.task.retry is None


class TestParseDateTime:
    """Tests for the _parse_datetime helper."""

    def test_parse_datetime__parses_iso_string(self) -> None:
        """Parse an ISO datetime string into a datetime object."""
        now = timezone.now()
        assert _parse_datetime(now.isoformat()) == now

    def test_parse_datetime__returns_non_string_unchanged(self) -> None:
        """Non-string values pass through unchanged."""
        assert _parse_datetime(None) is None
        assert _parse_datetime(42) == 42

    def test_parse_datetime__returns_invalid_string_unchanged(self) -> None:
        """Invalid datetime strings pass through unchanged."""
        assert _parse_datetime("not-a-date") == "not-a-date"


class TestAcknowledgementTimeout:
    """Tests for the AcknowledgementTimeout exception."""

    def test_exception_can_be_instantiated(self) -> None:
        """AcknowledgementTimeout can be instantiated."""
        exc = AcknowledgementTimeout()
        assert isinstance(exc, Exception)

    def test_exception_can_be_used_in_task_error(self) -> None:
        """AcknowledgementTimeout can be used as a TaskError's exception_class_path."""
        error = TaskError(
            exception_class_path="threadmill.exceptions.AcknowledgementTimeout",
            traceback="Task processing lease expired.",
        )
        assert (
            error.exception_class_path == "threadmill.exceptions.AcknowledgementTimeout"
        )


class FakeBroker(Broker):
    """Broker that records main() calls for testing."""

    def __init__(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=0.01)
    ) -> None:
        super().__init__(interval=interval)
        self.maintain_calls: list[float] = []

    def main(self) -> None:
        self.maintain_calls.append(time.monotonic())


class TestBroker:
    def test_main__is_noop(self) -> None:
        """Base Broker.main() is a no-op."""
        Broker(interval=datetime.timedelta(seconds=1)).main()

    def test_run__calls_maintain_then_exits_on_shutdown(self) -> None:
        """run() loops calling main() and exits after shutdown()."""
        broker = FakeBroker(interval=datetime.timedelta(seconds=0.01))
        broker.start()
        time.sleep(0.05)
        broker.shutdown()
        broker.join(timeout=1)
        assert not broker.is_alive()
        assert len(broker.maintain_calls) >= 1

    def test_interval_is_honored(self) -> None:
        """Broker waits at least interval between main() calls."""
        broker = FakeBroker(interval=datetime.timedelta(seconds=0.1))
        broker.start()
        time.sleep(0.25)
        broker.shutdown()
        broker.join(timeout=1)
        assert len(broker.maintain_calls) >= 2
        for i in range(1, len(broker.maintain_calls)):
            assert broker.maintain_calls[i] - broker.maintain_calls[i - 1] >= 0.09

    def test_shutdown__sets_event(self) -> None:
        """shutdown() sets the shutdown_requested event."""
        broker = Broker(interval=datetime.timedelta(seconds=1))
        assert not broker.shutdown_requested.is_set()
        broker.shutdown()
        assert broker.shutdown_requested.is_set()


received_contexts: list[TaskContext] = []


def record_context(context: TaskContext) -> None:
    """Retry callback that records the TaskContext it receives."""
    received_contexts.append(context)


def _task_result_with_retry(task, *, errors=None) -> TaskResult:
    """Build a FAILED TaskResult with errors for retry testing."""
    now = tz.now()
    return TaskResult(
        task=task,
        id=str(uuid.uuid7()),
        status=TRS.FAILED,
        enqueued_at=now,
        started_at=now,
        finished_at=now,
        last_attempted_at=now,
        backend="default",
        errors=errors or [],
        worker_ids=[],
        args=[],
        kwargs={},
    )


class DeconstructibleRetry:
    """A deconstructible retry callable for testing."""

    def __init__(self, *, seconds: int = 1) -> None:
        self.seconds = seconds

    def __call__(self, context) -> datetime.timedelta:
        return datetime.timedelta(seconds=self.seconds)

    def deconstruct(self):
        return (
            "tests.backends.test_base.DeconstructibleRetry",
            (),
            {"seconds": self.seconds},
        )

    def __eq__(self, other):
        return isinstance(other, DeconstructibleRetry) and self.seconds == other.seconds
