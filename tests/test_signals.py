import dataclasses
import time
import uuid
from unittest import mock

import pytest
from django.db import DEFAULT_DB_ALIAS, connections
from django.tasks import TaskResult, TaskResultStatus
from django.tasks.backends.immediate import ImmediateBackend
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from tests.testapp.tasks import echo
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
