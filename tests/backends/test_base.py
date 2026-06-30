from __future__ import annotations

import datetime
import time
import uuid

import pytest
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.utils import timezone

from threadmill.backends.base import Broker, ThreadmillTaskBackend
from threadmill.exceptions import AcknowledgementTimeout


class BackendDouble(ThreadmillTaskBackend):
    def enqueue(self, task, args, kwargs):
        return TaskResult(
            task=task,
            id=str(uuid.uuid4()),
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


class TestAcknowledgeableTaskBackend:
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

    def test_peek__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend peek_results API."""
        with pytest.raises(NotImplementedError):
            list(
                BackendDouble(alias="default", params={}).peek(
                    "default", status=TaskResultStatus.READY
                )
            )

    def test_telemetry__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend telemetry API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).telemetry()


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
