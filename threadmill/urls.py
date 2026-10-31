from django.urls import path

from .views import QueueStatsView

app_name = "threadmill"

urlpatterns = [
    path(
        "queue-stats/",
        QueueStatsView.as_view(),
        name="queue-stats",
    ),
]
