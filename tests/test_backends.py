from __future__ import annotations

import datetime

import pytest
from grinder.backends import AcknowledgeableTaskBackend


class BackendDouble(AcknowledgeableTaskBackend):
    def enqueue(self, task):
        return task


class TestAcknowledgeableTaskBackend:
    def test_acquire__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acquire API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acquire(
                datetime.timedelta(seconds=1)
            )

    def test_acknowledge__raise_not_implemented_error(self) -> None:
        """Raise NotImplementedError for backend acknowledge API."""
        with pytest.raises(NotImplementedError):
            BackendDouble(alias="default", params={}).acknowledge(task_result=None)
