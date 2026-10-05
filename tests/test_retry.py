import datetime
import uuid

from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskContext, TaskError
from django.utils import timezone

from threadmill import retry


def _context(*, attempt: int, exception_class: type[Exception]) -> TaskContext:
    """Build a TaskContext with a single error of the given exception class."""
    result = TaskResult(
        task=print,
        id=str(uuid.uuid7()),
        status=TaskResultStatus.FAILED,
        enqueued_at=timezone.now(),
        started_at=timezone.now(),
        finished_at=timezone.now(),
        last_attempted_at=timezone.now(),
        backend="default",
        errors=[
            TaskError(
                exception_class_path=f"{exception_class.__module__}.{exception_class.__qualname__}",
                traceback="",
            )
        ],
        worker_ids=[f"w{i}" for i in range(attempt)],
        args=[],
        kwargs={},
    )
    return TaskContext(task_result=result)


class TestExponentialBackoff:
    """Tests for the ExponentialBackoff retry strategy."""

    def test_call__returns_exponential_delay(self) -> None:
        """Return exponentially increasing delay for each retry attempt."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=60),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        for attempt in range(5):
            context = _context(attempt=attempt, exception_class=ValueError)
            delay = backoff(context)
            expected_delay = backoff.base_delay * (backoff.factor**attempt)
            assert delay == expected_delay

    def test_call__caps_delay_at_max_delay(self) -> None:
        """Return max_delay when the exponential delay exceeds it."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=10,
            expected_exceptions=(ValueError,),
        )
        context = _context(attempt=8, exception_class=ValueError)
        delay = backoff(context)
        assert delay == backoff.max_delay

    def test_call__returns_max_delay_for_large_attempt(self) -> None:
        """Return max_delay instead of raising OverflowError above attempt 46."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(hours=1),
            factor=2.0,
            max_retries=720,
            expected_exceptions=(ValueError,),
        )
        context = _context(attempt=47, exception_class=ValueError)
        assert backoff(context) == backoff.max_delay

    def test_call__returns_none_after_max_retries(self) -> None:
        """Return None when the attempt count reaches max_retries."""
        backoff = retry.ExponentialBackoff(
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        context = _context(attempt=5, exception_class=ValueError)
        assert backoff(context) is None

    def test_call__returns_none_for_unexpected_exception(self) -> None:
        """Return None when the error is not in expected_exceptions."""
        backoff = retry.ExponentialBackoff(
            expected_exceptions=(ValueError,),
        )
        context = _context(attempt=0, exception_class=KeyError)
        assert backoff(context) is None

    def test_deconstruct__returns_path_args_kwargs(self) -> None:
        """Deconstruct returns the importable path, empty args, and field kwargs."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        class_path, args, kwargs = backoff.deconstruct()
        assert class_path == "threadmill.retry.ExponentialBackoff"
        assert args == ()
        assert kwargs == {
            "base_delay": 1.0,
            "max_delay": 10.0,
            "factor": 2.0,
            "max_retries": 5,
            "expected_exceptions": ("builtins.ValueError",),
        }

    def test_deconstruct__reconstructs_roundtrip(self) -> None:
        """Reconstructing from deconstruct output yields an equal object."""
        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=1),
            max_delay=datetime.timedelta(seconds=10),
            factor=2.0,
            max_retries=5,
            expected_exceptions=(ValueError,),
        )
        _, _, kwargs = backoff.deconstruct()
        reconstructed = retry.ExponentialBackoff(**kwargs)
        assert backoff == reconstructed

    def test_serialize__roundtrips_through_task_result(self) -> None:
        """ExponentialBackoff survives JSON serialization as a retry callback."""
        import uuid

        from django.tasks import TaskResult, TaskResultStatus
        from django.utils import timezone

        from threadmill.backends.base import RetryTask, ThreadmillTaskBackend

        backoff = retry.ExponentialBackoff(
            base_delay=datetime.timedelta(seconds=2),
            max_delay=datetime.timedelta(minutes=5),
            max_retries=3,
        )
        from tests.testapp.tasks import echo

        task = RetryTask(func=echo.func, retry=backoff)
        result = TaskResult(
            task=task,
            id=str(uuid.uuid7()),
            status=TaskResultStatus.FAILED,
            enqueued_at=timezone.now(),
            started_at=timezone.now(),
            finished_at=timezone.now(),
            last_attempted_at=timezone.now(),
            backend="default",
            errors=[],
            worker_ids=[],
            args=[],
            kwargs={},
        )
        data = ThreadmillTaskBackend.serialize_task_result(result)
        restored = ThreadmillTaskBackend.deserialize_task_result(data)
        assert isinstance(restored.task.retry, retry.ExponentialBackoff)
        assert restored.task.retry == backoff
