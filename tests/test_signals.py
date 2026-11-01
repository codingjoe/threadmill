import dataclasses
import datetime
import time
import uuid
from unittest import mock

import pytest
from django.db import DEFAULT_DB_ALIAS, connections
from django.tasks import TaskResult, TaskResultStatus, task_backends
from django.tasks.backends.immediate import ImmediateBackend
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone

from tests.testapp.tasks import echo
from threadmill import signals
from threadmill.backends.base import ThreadmillTaskBackend
from threadmill.executor import (
    JsonFormatter,
    TaskExecutor,
    WorkerProcess,
    WorkerThread,
)


def _task_result(task, *args) -> TaskResult:
    """Build a READY `TaskResult` without touching the backend."""
    return TaskResult(
        task=task,
        id=str(uuid.uuid4()),
        status=TaskResultStatus.READY,
        enqueued_at=timezone.now(),
        started_at=None,
        finished_at=None,
        last_attempted_at=None,
        args=list(args),
        kwargs={},
        backend="default",
        errors=[],
        worker_ids=[],
    )


def _finished_task_result(task, *args) -> TaskResult:
    """Build a SUCCESSFUL `TaskResult` with attempt timestamps."""
    started_at = timezone.now()
    return dataclasses.replace(
        _task_result(task, *args),
        status=TaskResultStatus.SUCCESSFUL,
        started_at=started_at,
        last_attempted_at=started_at,
        finished_at=timezone.now(),
    )


@pytest.fixture
def stale_connection():
    """Open a database connection and mark it as expired."""
    connection = connections[DEFAULT_DB_ALIAS]
    connection.ensure_connection()
    connection.close_at = time.monotonic() - 1
    yield connection
    connection.close_at = None
    connection.close()


@pytest.mark.django_db(transaction=True)
class TestCloseTaskDatabaseConnection:
    def test_task_started__closes_stale_connection(self, stale_connection):
        """A stale connection is closed when a task starts."""
        with mock.patch.object(
            stale_connection, "close", wraps=stale_connection.close
        ) as spy:
            task_started.send(TaskExecutor, task_result=_task_result(echo))

        spy.assert_called_once()

    def test_task_finished__closes_stale_connection(self, stale_connection):
        """A stale connection is closed when a task finished."""
        with mock.patch.object(
            stale_connection, "close", wraps=stale_connection.close
        ) as spy:
            task_finished.send(TaskExecutor, task_result=_finished_task_result(echo))

        spy.assert_called_once()

    def test_task_started__ignores_other_senders(self, stale_connection):
        """Signals from other executors do not close the connection."""
        with mock.patch.object(stale_connection, "close") as spy:
            task_started.send(ImmediateBackend, task_result=_task_result(echo))

        spy.assert_not_called()

    def test_execute_task_result__closes_stale_connection(self, stale_connection):
        """Executing a task closes stale connections via lifecycle signals."""
        with mock.patch.object(
            stale_connection, "close", wraps=stale_connection.close
        ) as spy:
            worker = WorkerProcess(
                thread_count=1,
                backend_alias="default",
                queues=("default",),
                log_formatter=JsonFormatter(),
            )
            thread = WorkerThread(worker=worker, index=0, backend=None)

            result = thread.execute_task_result(_task_result(echo, 42))

        assert result.status is TaskResultStatus.SUCCESSFUL
        spy.assert_called()


class StubSentryMetrics:
    """Record Sentry metric calls without an initialized SDK."""

    def __init__(self) -> None:
        self.counts: list[tuple] = []
        self.distributions: list[tuple] = []

    def count(self, name, value, *, attributes=None) -> None:
        """Record one counter value."""
        self.counts.append((name, value, attributes))

    def distribution(self, name, value, *, unit=None, attributes=None) -> None:
        """Record one distribution observation."""
        self.distributions.append((name, value, unit, attributes))


@pytest.fixture
def sentry_metrics(monkeypatch):
    """Replace the Sentry metrics module with a recording stub."""
    stub = StubSentryMetrics()
    monkeypatch.setattr(signals, "sentry_metrics", stub)
    return stub


@pytest.mark.django_db(transaction=True)
class TestTaskMetrics:
    """Tests for Sentry metrics emitted on task lifecycle signals."""

    def test_task_enqueued__records_ingress(self, sentry_metrics):
        """Record ingress for tasks enqueued on a Threadmill backend."""
        task_result = _task_result(echo)

        task_enqueued.send(ThreadmillTaskBackend, task_result=task_result)

        assert sentry_metrics.counts == [
            ("threadmill.queue.ingress", 1, {"queue": task_result.task.queue_name})
        ]

    def test_task_enqueued__ignores_non_threadmill_backends(self, sentry_metrics):
        """Ignore tasks enqueued on backends that Threadmill does not run."""
        task_enqueued.send(ImmediateBackend, task_result=_task_result(echo))

        assert sentry_metrics.counts == []

    def test_task_enqueued__ignores_instance_senders(self, sentry_metrics):
        """Ignore senders that are not backend classes."""
        task_enqueued.send(task_backends["immediate"], task_result=_task_result(echo))

        assert sentry_metrics.counts == []

    def test_task_finished__records_egress_and_duration(self, sentry_metrics):
        """Record egress and the attempt duration for finished tasks."""
        finished_at = timezone.now()
        task_result = dataclasses.replace(
            _finished_task_result(echo),
            last_attempted_at=finished_at - datetime.timedelta(seconds=1.5),
            finished_at=finished_at,
        )

        task_finished.send(TaskExecutor, task_result=task_result)

        assert sentry_metrics.counts == [
            ("threadmill.queue.egress", 1, {"queue": task_result.task.queue_name})
        ]
        [(name, value, unit, attributes)] = sentry_metrics.distributions
        assert name == "threadmill.task.duration"
        assert value == pytest.approx(1500)
        assert unit == "millisecond"
        assert attributes == {
            "task": task_result.task.module_path,
            "outcome": "successful",
            "attempt": task_result.attempts,
        }
