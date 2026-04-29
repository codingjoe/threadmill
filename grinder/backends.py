from __future__ import annotations

import datetime
from abc import ABC

from django.tasks import TaskResult
from django.tasks.backends.base import BaseTaskBackend
from django.utils.module_loading import import_string


class SerializableTaskResult(TaskResult):
    """A serializable representation of a TaskResult for use in task backends."""

    task_path: str

    def __init__(self, task_path: str, **kwargs):
        super().__init__(**kwargs)
        object.__setattr__(self, "task_path", task_path)

    @property
    def task(self):
        return import_string(self.task_path)


class AcknowledgeableTaskBackend(BaseTaskBackend, ABC):
    """Provide an interface for tasks queues to be processed by the executor."""

    def acquire(
        self, timeout: datetime.timedelta | None = None
    ) -> SerializableTaskResult:
        """
        Return and lock the next task to be processed without removing it from the queue.

        Args:
            timeout: The maximum time to wait for a task. If None, wait indefinitely.

        Raises:
            TimeoutError: If no task is available within the specified timeout.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: SerializableTaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError
