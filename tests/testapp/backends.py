"""Test support task backends without a broker."""

import collections.abc
import datetime

from threadmill.backends.base import (
    BackendTelemetry,
    Broker,
    TelemetryEvent,
    ThreadmillTaskBackend,
)


class StubTaskBackend(ThreadmillTaskBackend):
    """Backend whose acquire always fails, to exercise prefetch error paths."""

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    def acquire(self, *queue_names, count=1, timeout=None, worker=""):
        """Raise a fetch failure for every call."""
        message = "backend unavailable"
        raise RuntimeError(message)


class RecordingBroker(Broker):
    """Broker double recording lifecycle calls without running maintenance."""

    instances: list[Broker] = []
    events: list[str] = []
    alive_after_shutdown = True
    fail_shutdown = False
    fail_join = False

    def __init__(
        self,
        backend=None,
        *,
        interval: datetime.timedelta = datetime.timedelta(seconds=1),
    ) -> None:
        super().__init__(backend, interval=interval)
        self.alive = False
        self.started = False
        self.shutdown_called = False
        self.join_called = False
        RecordingBroker.instances.append(self)

    def start(self) -> None:
        """Record the start without spawning a thread."""
        self.started = True
        self.alive = True
        RecordingBroker.events.append("broker.start")

    def is_alive(self) -> bool:
        """Report whether shutdown left the broker running."""
        return self.alive

    def shutdown(self) -> None:
        """Record shutdown and optionally fail or stay alive for the join check."""
        self.shutdown_called = True
        RecordingBroker.events.append("broker.shutdown")
        if self.fail_shutdown:
            message = "broker shutdown failed"
            raise RuntimeError(message)
        self.alive = self.alive_after_shutdown

    def join(self, timeout=None) -> None:
        """Record the join and optionally fail."""
        self.join_called = True
        RecordingBroker.events.append("broker.join")
        if self.fail_join:
            message = "broker join failed"
            raise RuntimeError(message)
        self.alive = False


class TelemetryTaskBackend(ThreadmillTaskBackend):
    """Backend double serving scripted queue telemetry to exporter tests."""

    broker_class = RecordingBroker

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.telemetry: list[BackendTelemetry | BaseException] = []
        self.events: list[list[TelemetryEvent] | BaseException] = []
        self.queue_stats_calls = 0
        self.telemetry_calls = 0

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    async def queue_stats(
        self, *, interval=datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        self.queue_stats_calls += 1
        item = self.telemetry.pop(0) if self.telemetry else BackendTelemetry(queues={})
        if isinstance(item, BaseException):
            raise item
        return item

    async def worker_telemetry(
        self,
    ) -> collections.abc.AsyncIterator[TelemetryEvent]:
        self.telemetry_calls += 1
        item = self.events.pop(0) if self.events else []
        if isinstance(item, BaseException):
            raise item
        for event in item:
            yield event


class TelemetryTaskBackendWithoutBroker(TelemetryTaskBackend):
    """Telemetry backend whose broker is disabled."""

    broker_class = None
