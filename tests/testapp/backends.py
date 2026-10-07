"""Test support task backends without a broker."""

from threadmill.backends.base import ThreadmillTaskBackend


class StubTaskBackend(ThreadmillTaskBackend):
    """Backend whose acquire always fails, to exercise prefetch error paths."""

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    def acquire(self, *queue_names, count=1, timeout=None, worker=""):
        """Raise a fetch failure for every call."""
        raise RuntimeError("backend unavailable")
