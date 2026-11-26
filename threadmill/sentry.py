"""Sentry reporting for task attempts that raise."""

import contextlib
import logging

from django.tasks import TaskResult

logger = logging.getLogger(__name__)

sentry_sdk = None
with contextlib.suppress(ImportError):
    import sentry_sdk
    from sentry_sdk.integrations import logging as sentry_logging
    from sentry_sdk.utils import event_from_exception


def report_failure(
    exception: BaseException,
    task_result: TaskResult,
    *,
    handled: bool,
) -> None:
    """
    Send one attempt's error to Sentry as its own event.

    The event arrives with a ``threadmill`` mechanism, tags for task, queue,
    and attempt, and the task ID as an extra. Local variable values stay out
    of the stack trace. Mark an attempt handled when a retry follows it and
    unhandled when it is the last one, so the issue reflects that the task
    gave up.

    Args:
        exception: Error raised by the attempt.
        task_result: Result of the attempt that raised it.
        handled: Whether another attempt follows.
    """
    if sentry_sdk is not None:
        try:
            event, hint = event_from_exception(
                exception, mechanism={"type": "threadmill", "handled": handled}
            )
            for value in event["exception"]["values"]:
                if isinstance(stacktrace := value.get("stacktrace"), dict):
                    for frame in stacktrace["frames"]:
                        frame.pop("vars", None)
            sentry_sdk.capture_event(
                event,
                hint=hint,
                tags={
                    "task": task_result.task.module_path,
                    "queue": task_result.task.queue_name,
                    "attempt": str(task_result.attempts),
                },
                extras={"task_id": task_result.id},
            )
        except Exception:
            logger.exception("Failed to report task failure to Sentry")


def ignore_django_tasks_events() -> None:
    """
    Mute the framework logger that repeats each raised attempt.

    The task framework logs each failed attempt with its traceback, and
    Sentry's logging integration would turn that record into a second copy,
    always marked handled. The worker mutes it once per process.
    """
    if sentry_sdk is not None:
        getattr(
            sentry_logging, "ignore_logger_for_events", sentry_logging.ignore_logger
        )("django.tasks")
