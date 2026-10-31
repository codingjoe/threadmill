import dataclasses
import json

import pytest
from django.http import Http404
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS
from django.test import RequestFactory, override_settings
from django.urls import resolve

from tests.testapp.tasks import echo
from threadmill.views import QueueStatsView


def _create_request(**params):
    return RequestFactory().get("/queue-stats/", params)


def _enqueue(*, queue_name: str, count: int) -> None:
    task = dataclasses.replace(echo, queue_name=queue_name)
    for value in range(count):
        task.enqueue(value)


class TestQueueStatsView:
    """Tests for the queue stats view."""

    async def test_get__sums_ready_counts_for_requested_queues(self):
        """Sum the ready counts of the requested queues."""
        _enqueue(queue_name="default", count=5)
        _enqueue(queue_name="compute", count=1)
        _enqueue(queue_name="io", count=2)

        response = await QueueStatsView.as_view()(
            _create_request(backend=DEFAULT_TASK_BACKEND_ALIAS, queue=["compute", "io"])
        )

        assert response.status_code == 200
        assert json.loads(response.content) == {"ready": 3}

    async def test_get__defaults_to_all_backend_queues(self):
        """Use every configured queue when none is requested."""
        _enqueue(queue_name="compute", count=1)
        _enqueue(queue_name="io", count=2)

        response = await QueueStatsView.as_view()(
            _create_request(backend=DEFAULT_TASK_BACKEND_ALIAS)
        )

        assert response.status_code == 200
        assert json.loads(response.content) == {"ready": 3}

    async def test_get__defaults_to_default_backend(self):
        """Use the default backend alias when none is requested."""
        _enqueue(queue_name="compute", count=3)

        response = await QueueStatsView.as_view()(_create_request())

        assert response.status_code == 200
        assert json.loads(response.content) == {"ready": 3}

    async def test_get__unknown_backend_raises_404(self):
        """Raise 404 for an unregistered backend alias."""
        with pytest.raises(Http404):
            await QueueStatsView.as_view()(_create_request(backend="missing"))

    async def test_get__non_threadmill_backend_raises_404(self):
        """Raise 404 for registered backends outside Threadmill."""
        with pytest.raises(Http404):
            await QueueStatsView.as_view()(_create_request(backend="immediate"))

    async def test_get__backend_without_queue_stats_raises_404(self):
        """Raise 404 for Threadmill backends without queue stats."""
        with pytest.raises(Http404):
            await QueueStatsView.as_view()(_create_request(backend="stub"))

    async def test_get__unknown_queue_raises_404(self):
        """Raise 404 for an unknown queue name."""
        with pytest.raises(Http404):
            await QueueStatsView.as_view()(_create_request(queue="missing"))


class TestQueueStatsUrl:
    """Tests for the queue stats URL."""

    @override_settings(ROOT_URLCONF="threadmill.urls")
    def test_urlpatterns__queue_stats(self):
        """Resolve the queue stats URL to the view."""
        assert resolve("/queue-stats/").func.view_class is QueueStatsView
