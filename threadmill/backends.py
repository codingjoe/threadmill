from __future__ import annotations

import datetime
from abc import ABC

from django.tasks import TaskResult
from django.tasks.backends.base import BaseTaskBackend


class AcknowledgeableTaskBackend(BaseTaskBackend, ABC):
    """Provide an interface for tasks queues to be processed by the executor."""

    supports_async_task = True
    supports_get_result = True

    def acquire(
        self, *queue_names: str, timeout: datetime.timedelta | None = None
    ) -> TaskResult:
        """
        Return and lock the next task to be processed without removing it from the queue.

        Args:
            queue_names: The names of the queues to acquire tasks from.
            timeout: The maximum time to wait for a task. If None, wait indefinitely.

        Raises:
            TimeoutError: If no task is available within the specified timeout.
            queue.Empty: If no task is available and timeout is None.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: TaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError
