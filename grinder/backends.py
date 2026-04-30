from __future__ import annotations

import datetime
from abc import ABC

from django.tasks import TaskResult
from django.tasks.backends.base import BaseTaskBackend


class AcknowledgeableTaskBackend(BaseTaskBackend, ABC):
    """Provide an interface for tasks queues to be processed by the executor."""

    def acquire(self, timeout: datetime.timedelta | None = None) -> TaskResult:
        """
        Return and lock the next task to be processed without removing it from the queue.

        Args:
            timeout: The maximum time to wait for a task. If None, wait indefinitely.

        Raises:
            TimeoutError: If no task is available within the specified timeout.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: TaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError
