from django.http import Http404, HttpRequest, JsonResponse
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    InvalidTaskBackend,
    task_backends,
)
from django.views import View


class QueueStatsView(View):
    async def get(self, request: HttpRequest) -> JsonResponse:
        try:
            backend = task_backends[
                request.GET.get("backend", DEFAULT_TASK_BACKEND_ALIAS)
            ]
        except InvalidTaskBackend as exception:
            raise Http404 from exception
        try:
            telemetry = await backend.queue_stats()
        except (AttributeError, NotImplementedError) as exception:
            raise Http404 from exception
        try:
            return JsonResponse(
                {
                    "ready": sum(
                        telemetry.queues[queue_name].counts.ready
                        for queue_name in request.GET.getlist("queue", backend.queues)
                    )
                }
            )
        except (AttributeError, KeyError) as exception:
            raise Http404 from exception
