from django.http import Http404, HttpRequest, JsonResponse
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    InvalidTaskBackend,
    task_backends,
)
from django.views import View

from .backends.base import ThreadmillTaskBackend


class QueueStatsView(View):
    async def get(self, request: HttpRequest) -> JsonResponse:
        try:
            backend = task_backends[
                request.GET.get("backend", DEFAULT_TASK_BACKEND_ALIAS)
            ]
        except InvalidTaskBackend as exception:
            raise Http404 from exception
        if not isinstance(backend, ThreadmillTaskBackend):
            raise Http404
        telemetry = await backend.queue_stats()
        try:
            return JsonResponse(
                {
                    "ready": sum(
                        telemetry.queues[queue_name].counts.ready
                        for queue_name in request.GET.getlist("queue", backend.queues)
                    )
                }
            )
        except KeyError as exception:
            raise Http404 from exception
