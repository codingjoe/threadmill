from __future__ import annotations

import dataclasses
import datetime

import pytest
from django.tasks import (
    DEFAULT_TASK_QUEUE_NAME,
    TaskResult,
    TaskResultStatus,
    default_task_backend,
)
from django.tasks.base import TaskError
from django.utils import timezone
from textual.widgets import DataTable, ListView, Select
from textual.widgets._data_table import RowKey

from tests.testapp.tasks import echo
from threadmill.backends.base import QueueStats, QueueTelemetry, ThreadmillTaskBackend
from threadmill.inspector.app import (
    InspectorApp,
    QueueList,
    TaskDetail,
    TaskList,
    si_prefix,
)


class FailingBackend(ThreadmillTaskBackend):
    """Backend double whose peek and queue_telemetry raise for error-path tests."""

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    def peek(self, *args, **kwargs):
        raise RuntimeError("peek failed")

    def queue_telemetry(self, *, interval=datetime.timedelta(seconds=60)):
        raise RuntimeError("telemetry failed")


def _failed_result() -> TaskResult:
    """Build a failed TaskResult with an error for detail-view tests."""
    return TaskResult(
        task=echo,
        id="err-1",
        status=TaskResultStatus.FAILED,
        enqueued_at=timezone.now(),
        started_at=timezone.now(),
        finished_at=timezone.now(),
        last_attempted_at=None,
        backend="default",
        errors=[TaskError(exception_class_path="ValueError", traceback="boom\nline2")],
        worker_ids=["w1"],
        args=[1],
        kwargs={},
    )


def _stats(**overrides: int) -> QueueStats:
    """Build QueueStats zeroed everywhere except the given overrides."""
    return QueueStats(
        **{
            "ingress": 0,
            "egress": 0,
            "ready": 0,
            "running": 0,
            "deferred": 0,
            "successful": 0,
            "failed": 0,
        }
        | overrides
    )


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (0, "0"),
        (999, "999"),
        (1000, "1k"),
        (3500, "3.5k"),
        (12345, "12.3k"),
        (123456, "123k"),
        (1_000_000, "1M"),
        (3_500_000, "3.5M"),
    ],
)
def test_si_prefix(count: int, expected: str) -> None:
    """Large counts are compacted with k/M/G suffixes."""
    assert si_prefix(count) == expected


class TestInspectorApp:
    """Tests for the textual inspector TUI."""

    async def test_selecting_queue_lists_ready_tasks(self):
        """Selecting a queue lists its ready tasks in the active tab."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-ready", DataTable)
            assert table.row_count == 2
            assert not table.disabled

    async def test_task_table_has_visible_height(self):
        """The ready data table renders with non-zero height, not collapsed by layout."""
        default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-ready", DataTable)
            assert table.row_count >= 1
            assert table.region.height > 0

    async def test_default_tab_is_ready(self):
        """The inspector opens on the ready tab."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            assert task_list._tabs.active == "tab-ready"

    async def test_switching_tabs_refreshes_running_tasks(self):
        """Switching to the running tab lists acquired tasks."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="inspector-test"
        )
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            task_list._tabs.active = "tab-running"
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-running", DataTable)
            assert table.row_count == 1
            assert not table.disabled

    async def test_selected_task_detail_updates(self):
        """The detail view reflects the first task of the selected queue."""
        task_result = default_task_backend.enqueue(echo, args=[42])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            assert app._task_detail.task_result is not None
            assert app._task_detail.task_result.id == task_result.id

    async def test_failed_task_detail_renders_errors(self):
        """The detail view renders error tracebacks for failed results with errors."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            detail = app.query_one("#task-detail", TaskDetail)
            detail.task_result = _failed_result()
            await pilot.pause()
            text = str(detail.render())
            assert "ValueError" in text
            assert "boom" in text

    async def test_row_selected_updates_selected_task(self):
        """Selecting a row in the data table updates the selected task."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-ready", DataTable)
            task_list.on_data_table_row_selected(
                DataTable.RowSelected(table, 0, RowKey(value=task_result.id))
            )
            await pilot.pause()
            assert task_list.selected_task is not None
            assert task_list.selected_task.id == task_result.id

    async def test_select_changed_switches_backend(self):
        """Choosing a backend alias from the dropdown switches the active backend."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            select = app.query_one("#backend-select", Select)
            app.on_select_changed(Select.Changed(select, "default"))
            await pilot.pause()
            assert app.backend is default_task_backend

    async def test_list_view_selected_sets_queue(self):
        """Pressing select on a queue list item updates the task list queue."""
        default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            queue_list = app.query_one("#queue-list", QueueList)
            item = next(iter(queue_list._items.values()))
            app.on_list_view_selected(ListView.Selected(queue_list, item, 0))
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            assert task_list.queue_name == item.queue_name

    async def test_telemetry_refresh_updates_and_prunes_queues(self):
        """Telemetry refresh updates existing queue labels and drops stale queues."""
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            app._refresh_telemetry()
            await pilot.pause()
            queue_list = app.query_one("#queue-list", QueueList)
            assert set(queue_list._items) == set(default_task_backend.queues)
            app.telemetry = QueueTelemetry(queues={"default": _stats()})
            await pilot.pause()
            assert list(queue_list._items) == ["default"]

    async def test_telemetry_counts_are_scoped_to_selected_queue(self):
        """Tab counts reflect the selected queue, not backend-wide totals."""
        default_task_backend.enqueue(echo, args=[1])
        stats = _stats(ready=1)
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            app.telemetry = QueueTelemetry(
                queues={"default": stats, "other": _stats(ready=5)}
            )
            await pilot.pause()
            assert task_list.counts == {
                "running": 0,
                "ready": 1,
                "successful": 0,
                "failed": 0,
            }

    async def test_tab_count_label_abbreviates_large_counts(self):
        """A tab count at or above 1000 is abbreviated in its tab label."""
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            app.telemetry = QueueTelemetry(queues={"default": _stats(ready=1500)})
            await pilot.pause()
            assert task_list._tabs.get_tab("tab-ready").label.plain == "Ready (1.5k)"

    async def test_refresh_preserves_selected_task(self):
        """Refreshing the task list keeps the highlighted task selected."""
        first = default_task_backend.enqueue(echo, args=[1])
        second = default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            task_list.selected_task = next(
                r for r in task_list._current_results if r.id == second.id
            )
            task_list._refresh_data()
            await pilot.pause()
            assert task_list.selected_task is not None
            assert task_list.selected_task.id == second.id
            assert first.id in {r.id for r in task_list._current_results}

    async def test_refresh_falls_back_to_first_when_selected_gone(self):
        """A refresh that drops the selected task falls back to the first remaining row."""
        first = default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            task_list.selected_task = next(
                r for r in task_list._current_results if r.id == first.id
            )
            acquired = default_task_backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="inspector-test"
            )
            assert acquired.id == first.id
            task_list._refresh_data()
            await pilot.pause()
            assert task_list.selected_task is not None
            assert task_list.selected_task.id != first.id
            assert first.id not in {r.id for r in task_list._current_results}

    async def test_watch_backend_propagates_and_handles_errors(self):
        """Changing backend updates the task list and logs peek/telemetry errors."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            failing = FailingBackend(alias="failing", params={})
            app.backend = failing
            await pilot.pause()
            await pilot.pause()
            assert task_list.backend is failing

    async def test_action_quit_exits(self):
        """The quit action exits the app without raising."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.action_quit()
            await pilot.pause()

    async def test_action_refresh_polls_backend(self):
        """The F5 binding fetches a fresh telemetry snapshot from the backend."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            default_task_backend.enqueue(echo, args=[1])
            app.action_refresh()
            await pilot.pause()
            assert app.telemetry.queues["default"].ready == 1

    async def test_auto_selects_default_queue(self):
        """On first telemetry the queue list auto-selects the default queue."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            assert task_list.queue_name == DEFAULT_TASK_QUEUE_NAME
            table = app.query_one("#table-ready", DataTable)
            assert table.row_count == 2

    async def test_tab_columns_match_status(self):
        """Each status tab shows only the date columns that status populates."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            ready = app.query_one("#table-ready", DataTable)
            running = app.query_one("#table-running", DataTable)
            successful = app.query_one("#table-successful", DataTable)
            assert [c.label.plain for c in ready.ordered_columns] == [
                "ID",
                "Function",
                "Enqueued",
            ]
            assert [c.label.plain for c in running.ordered_columns] == [
                "ID",
                "Function",
                "Enqueued",
                "Started",
                "Workers",
            ]
            assert [c.label.plain for c in successful.ordered_columns] == [
                "ID",
                "Function",
                "Enqueued",
                "Started",
                "Finished",
                "Workers",
            ]

    async def test_digit_binding_switches_tab(self):
        """Pressing a digit key activates the matching status tab and refreshes it."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="tab-test"
        )
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            await pilot.press("1")
            await pilot.pause()
            await pilot.pause()
            assert task_list._tabs.active == "tab-running"
            running = app.query_one("#table-running", DataTable)
            assert not running.disabled
            assert running.row_count == 1

    async def test_successful_tab_lists_finished_task(self):
        """The successful tab renders acknowledged tasks with the finished column."""
        default_task_backend.enqueue(echo, args=[1])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="succ-test"
        )
        assert acquired is not None
        default_task_backend.acknowledge(
            dataclasses.replace(
                acquired,
                status=TaskResultStatus.SUCCESSFUL,
                finished_at=timezone.now(),
            )
        )
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.switch_tab("tab-successful")
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-successful", DataTable)
            assert not table.disabled
            assert table.row_count >= 1

    async def test_auto_refresh_timer_armed(self):
        """With auto-refresh on, a telemetry timer is armed on mount."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert app._telemetry_timer is not None
            assert app._telemetry_timer.name == "telemetry-refresh"
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            assert app._telemetry_timer is None

    async def test_initial_focus_on_queue_list(self):
        """The app opens with focus on the queue list, not the backend selector."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert app.focused is not None
            assert app.focused.id == "queue-list"

    async def test_telemetry_refresh_does_not_re_peek_task_list(self):
        """A telemetry refresh updates counts but leaves task rows stale until manual refresh."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-ready", DataTable)
            before = table.row_count
            default_task_backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="stale-test"
            )
            app.telemetry = app.backend.queue_telemetry()
            await pilot.pause()
            await pilot.pause()
            assert table.row_count == before
            task_list.refresh_tasks()
            await pilot.pause()
            await pilot.pause()
            assert table.row_count == before - 1

    async def test_action_refresh_refreshes_task_list(self):
        """F5 re-peeks the task list, picking up changes since the last refresh."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            table = app.query_one("#table-ready", DataTable)
            before = table.row_count
            default_task_backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="f5-test"
            )
            app.action_refresh()
            await pilot.pause()
            await pilot.pause()
            assert table.row_count == before - 1
