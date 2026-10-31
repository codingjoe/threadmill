import datetime
import json

import pytest
from django.http import Http404
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS
from django.test import RequestFactory, override_settings
from django.urls import resolve

from tests.testapp.backends import StubTaskBackend
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
)
from threadmill.views import QueueStatsView


def _stats(*, ready: int) -> QueueStats:
    return QueueStats(
        counts=QueueCounts(
            ready=ready,
            running=0,
            deferred=0,
            successful=0,
            failed=0,
        ),
        rates=QueueRates(
            interval=datetime.timedelta(seconds=60),
            ingress=0,
            egress=0,
        ),
    )


class TelemetryBackend(StubTaskBackend):
    """Backend that reports fixed queue telemetry."""

    async def queue_stats(self):
        return BackendTelemetry(
            queues={
                "emails": _stats(ready=3),
                "reports": _stats(ready=4),
            }
        )


@pytest.fixture
def telemetry_backend(monkeypatch):
    backend = TelemetryBackend("telemetry", {"QUEUES": ["emails", "reports"]})
    monkeypatch.setattr("threadmill.views.task_backends", {"telemetry": backend})
    return backend


def _request(**params):
    return RequestFactory().get("/queue-stats/", params)


async def test_get__sums_ready_counts_for_requested_queues(telemetry_backend):
    """The endpoint sums the ready counts of the requested queues."""
    response = await QueueStatsView.as_view()(
        _request(backend="telemetry", queue=["emails", "reports"])
    )

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 7}


async def test_get__defaults_to_all_backend_queues(telemetry_backend):
    """The endpoint uses every configured queue when none is requested."""
    response = await QueueStatsView.as_view()(_request(backend="telemetry"))

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 7}


async def test_get__defaults_to_default_backend(monkeypatch, telemetry_backend):
    """The endpoint uses the default backend alias when none is requested."""
    monkeypatch.setattr(
        "threadmill.views.task_backends",
        {DEFAULT_TASK_BACKEND_ALIAS: telemetry_backend},
    )

    response = await QueueStatsView.as_view()(_request())

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 7}


async def test_get__unknown_backend_raises_404():
    """An unregistered backend alias is a 404."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="missing"))


async def test_get__non_threadmill_backend_raises_404(monkeypatch):
    """Backends that do not implement Threadmill telemetry are a 404."""
    monkeypatch.setattr("threadmill.views.task_backends", {"other": object()})

    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="other"))


async def test_get__backend_without_queue_stats_raises_404(monkeypatch):
    """Threadmill backends without queue stats are a 404."""
    backend = StubTaskBackend("stub", {"QUEUES": ["default"]})
    monkeypatch.setattr("threadmill.views.task_backends", {"stub": backend})

    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="stub"))


async def test_get__non_threadmill_queue_stats_raises_404(monkeypatch):
    """A non-Threadmill queue_stats result is a 404."""

    class OtherBackend:
        async def queue_stats(self):
            return object()

    monkeypatch.setattr("threadmill.views.task_backends", {"other": OtherBackend()})

    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="other"))


async def test_get__unknown_queue_raises_404(telemetry_backend):
    """An unknown queue name is a 404."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="telemetry", queue="missing"))


@override_settings(ROOT_URLCONF="threadmill.urls")
def test_urls__queue_stats():
    """The queue stats URL resolves to the view."""
    match = resolve("/queue-stats/")

    assert match.func.view_class is QueueStatsView
