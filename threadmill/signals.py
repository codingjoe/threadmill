from django.db import close_old_connections
from django.dispatch import receiver
from django.tasks.signals import task_enqueued, task_finished, task_started

from .backends.base import ThreadmillTaskBackend
from .executor import TaskExecutor, _import_sentry_metrics

sentry_metrics = _import_sentry_metrics()


@receiver([task_started, task_finished], sender=TaskExecutor)
def close_task_database_connection(sender, task_result, **kwargs):
    close_old_connections()


@receiver(task_enqueued)
def record_task_enqueued(sender, task_result, **kwargs):
    """Record one task entering its queue."""
    if (
        sentry_metrics is not None
        and isinstance(sender, type)
        and issubclass(sender, ThreadmillTaskBackend)
    ):
        sentry_metrics.count(
            "threadmill.queue.ingress",
            1,
            attributes={"queue": task_result.task.queue_name},
        )


@receiver(task_finished, sender=TaskExecutor)
def record_task_finished(sender, task_result, **kwargs):
    """Emit one egress count and one duration observation per attempt."""
    if sentry_metrics is not None:
        sentry_metrics.count(
            "threadmill.queue.egress",
            1,
            attributes={"queue": task_result.task.queue_name},
        )
        duration = task_result.finished_at - task_result.last_attempted_at
        sentry_metrics.distribution(
            "threadmill.task.duration",
            duration.total_seconds() * 1e3,
            unit="millisecond",
            attributes={
                "task": task_result.task.module_path,
                "outcome": task_result.status.value.lower(),
                "attempt": task_result.attempts,
            },
        )
