import dataclasses
import json

import pytest
from django.http import Http404
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS
from django.test import RequestFactory, override_settings
from django.urls import resolve

from tests.testapp.tasks import echo
from threadmill.views import QueueStatsView


def _request(**params):
    return RequestFactory().get("/queue-stats/", params)


def _enqueue(*, queue_name: str, count: int) -> None:
    task = dataclasses.replace(echo, queue_name=queue_name)
    for value in range(count):
        task.enqueue(value)


async def test_get__sums_ready_counts_for_requested_queues():
    """The endpoint sums the ready counts of the requested queues."""
    _enqueue(queue_name="compute", count=1)
    _enqueue(queue_name="io", count=2)

    response = await QueueStatsView.as_view()(
        _request(backend=DEFAULT_TASK_BACKEND_ALIAS, queue=["compute", "io"])
    )

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 3}


async def test_get__defaults_to_all_backend_queues():
    """The endpoint uses every configured queue when none is requested."""
    _enqueue(queue_name="compute", count=1)
    _enqueue(queue_name="io", count=2)

    response = await QueueStatsView.as_view()(
        _request(backend=DEFAULT_TASK_BACKEND_ALIAS)
    )

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 3}


async def test_get__defaults_to_default_backend():
    """The endpoint uses the default backend alias when none is requested."""
    _enqueue(queue_name="compute", count=3)

    response = await QueueStatsView.as_view()(_request())

    assert response.status_code == 200
    assert json.loads(response.content) == {"ready": 3}


async def test_get__unknown_backend_raises_404():
    """An unregistered backend alias is a 404."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="missing"))


async def test_get__non_threadmill_backend_raises_404():
    """Registered backends outside Threadmill expose no queue stats."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="immediate"))


async def test_get__backend_without_queue_stats_raises_404():
    """Threadmill backends without queue stats are a 404."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(backend="stub"))


async def test_get__unknown_queue_raises_404():
    """An unknown queue name is a 404."""
    with pytest.raises(Http404):
        await QueueStatsView.as_view()(_request(queue="missing"))


@override_settings(ROOT_URLCONF="threadmill.urls")
def test_urls__queue_stats():
    """The queue stats URL resolves to the view."""
    match = resolve("/queue-stats/")

    assert match.func.view_class is QueueStatsView
