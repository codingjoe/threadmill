"""Tests for the optional Sentry failure reporter."""

import logging
import uuid

import pytest
from django.tasks import TaskResult, TaskResultStatus
from django.utils import timezone

from tests.testapp.tasks import boom
from threadmill import sentry


def _task_result(*, attempts: int = 2) -> TaskResult:
    """Build a FAILED task result without touching a backend."""
    return TaskResult(
        task=boom,
        id=str(uuid.uuid7()),
        status=TaskResultStatus.FAILED,
        enqueued_at=timezone.now(),
        started_at=timezone.now(),
        finished_at=timezone.now(),
        last_attempted_at=timezone.now(),
        args=[],
        kwargs={},
        backend="default",
        errors=[],
        worker_ids=[f"worker-{index}" for index in range(attempts)],
    )


def _raise_value_error() -> None:
    """Raise a ValueError from a helper frame."""
    raise ValueError("boom")


def _raised_value_error() -> ValueError:
    """Return a ValueError carrying its traceback."""
    try:
        _raise_value_error()
    except ValueError as exception:
        return exception


def _raise_chained_value_error() -> None:
    """Raise a ValueError whose cause was never raised itself."""
    raise ValueError("effect") from KeyError("unraised cause")


def _raised_chained_value_error() -> ValueError:
    """Return a chained exception with an untraced cause."""
    try:
        _raise_chained_value_error()
    except ValueError as exception:
        return exception


class StubSentrySdk:
    """Record captured events instead of delivering them to Sentry."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def capture_event(self, event, **kwargs) -> None:
        """Record one event and its scope kwargs."""
        self.calls.append((event, kwargs))


@pytest.fixture
def sentry_sdk(monkeypatch):
    """Replace the SDK with a recording stub."""
    stub = StubSentrySdk()
    monkeypatch.setattr(sentry, "sentry_sdk", stub)
    return stub


@pytest.mark.parametrize("handled", [False, True])
def test_report_failure__marks_handled_state(sentry_sdk, handled):
    """Mark retryable failures handled and terminal failures unhandled."""
    sentry.report_failure(_raised_value_error(), _task_result(), handled=handled)

    [(event, _)] = sentry_sdk.calls
    mechanism = event["exception"]["values"][-1]["mechanism"]
    assert mechanism["type"] == "threadmill"
    assert mechanism["handled"] is handled


def test_report_failure__attaches_task_scope(sentry_sdk):
    """Tag the event with task, queue, attempt and the task id."""
    task_result = _task_result(attempts=2)

    sentry.report_failure(_raised_value_error(), task_result, handled=False)

    [(_, kwargs)] = sentry_sdk.calls
    assert kwargs["tags"] == {
        "task": boom.module_path,
        "queue": boom.queue_name,
        "attempt": "2",
    }
    assert kwargs["extras"] == {"task_id": task_result.id}
    assert kwargs["hint"]["exc_info"]


def test_report_failure__strips_frame_variables(sentry_sdk):
    """Keep local variables out of the event frames."""
    sentry.report_failure(_raised_value_error(), _task_result(), handled=False)

    [(event, _)] = sentry_sdk.calls
    frames = [
        frame
        for value in event["exception"]["values"]
        for frame in value["stacktrace"]["frames"]
    ]
    assert frames
    assert all("vars" not in frame for frame in frames)


def test_report_failure__skips_values_without_stacktrace(sentry_sdk):
    """Report chained exceptions whose cause never had a traceback."""
    sentry.report_failure(_raised_chained_value_error(), _task_result(), handled=False)

    [(event, _)] = sentry_sdk.calls
    values = event["exception"]["values"]
    assert [value["type"] for value in values] == ["KeyError", "ValueError"]
    assert "stacktrace" not in values[0]
    frames = values[1]["stacktrace"]["frames"]
    assert frames
    assert all("vars" not in frame for frame in frames)


def test_report_failure__logs_capture_errors(monkeypatch, caplog):
    """Log and swallow errors raised while reporting."""

    class FailingSentrySdk:
        @staticmethod
        def capture_event(event, **kwargs) -> None:
            message = "sentry unavailable"
            raise RuntimeError(message)

    monkeypatch.setattr(sentry, "sentry_sdk", FailingSentrySdk)

    with caplog.at_level(logging.ERROR, logger="threadmill.sentry"):
        sentry.report_failure(_raised_value_error(), _task_result(), handled=False)

    [record] = [
        record for record in caplog.records if record.name == "threadmill.sentry"
    ]
    assert record.getMessage() == "Failed to report task failure to Sentry"
    assert record.exc_info is not None


def test_report_failure__skips_without_sdk(monkeypatch):
    """Do nothing when the optional SDK is not installed."""

    def fail(*args, **kwargs) -> None:
        pytest.fail("event_from_exception called without the SDK")

    monkeypatch.setattr(sentry, "sentry_sdk", None)
    monkeypatch.setattr(sentry, "event_from_exception", fail)

    sentry.report_failure(_raised_value_error(), _task_result(), handled=False)


def test_ignore_django_tasks_events__mutes_django_tasks_logger(monkeypatch):
    """Mute Django's duplicate task-failure events through the current API."""
    muted = []

    class StubSentryLogging:
        @staticmethod
        def ignore_logger_for_events(name) -> None:
            muted.append(("events", name))

        @staticmethod
        def ignore_logger(name) -> None:
            muted.append(("logs", name))

    monkeypatch.setattr(sentry, "sentry_sdk", object())
    monkeypatch.setattr(sentry, "sentry_logging", StubSentryLogging)

    sentry.ignore_django_tasks_events()

    assert muted == [("events", "django.tasks")]


def test_ignore_django_tasks_events__falls_back_for_older_sdk(monkeypatch):
    """Fall back to ignore_logger on SDKs without the event-only name."""
    muted = []

    class StubSentryLogging:
        @staticmethod
        def ignore_logger(name) -> None:
            muted.append(name)

    monkeypatch.setattr(sentry, "sentry_sdk", object())
    monkeypatch.setattr(sentry, "sentry_logging", StubSentryLogging)

    sentry.ignore_django_tasks_events()

    assert muted == ["django.tasks"]


def test_ignore_django_tasks_events__skips_without_sdk(monkeypatch):
    """Do nothing when the optional SDK is not installed."""
    monkeypatch.setattr(sentry, "sentry_sdk", None)
    monkeypatch.setattr(sentry, "sentry_logging", None)

    sentry.ignore_django_tasks_events()
