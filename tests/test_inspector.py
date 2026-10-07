import dataclasses
import datetime

import pytest

pytest.importorskip("textual.widgets")

from django.conf import settings
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
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
    ThreadmillTaskBackend,
)
from threadmill.backends.redis import RedisTaskBackend
from threadmill.inspector.app import (
    InspectorApp,
    QueueList,
    TaskDetail,
    TaskList,
    si_prefix,
)
from threadmill.inspector.screens import ConfirmScreen, PurgeScreen


class FailingBackend(ThreadmillTaskBackend):
    """Backend double whose peek and queue_stats raise for error-path tests."""

    def enqueue(self, task, args, kwargs):
        raise NotImplementedError

    def peek(self, *args, **kwargs):
        raise RuntimeError("peek failed")

    async def queue_stats(self, *, interval=None):
        raise RuntimeError("queue_stats failed")


class ErroringBackend(RedisTaskBackend):
    """Redis backend whose requeue/dequeue/purge raise for error-path tests."""

    def requeue(self, task_result, run_after):
        raise RuntimeError("boom")

    def dequeue(self, task_result):
        raise RuntimeError("boom")

    def purge(self, queue_name):
        raise RuntimeError("boom")


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


def _stats(**overrides: int | datetime.timedelta) -> QueueStats:
    """Build QueueStats zeroed everywhere except the given overrides.

    Count overrides (ready/running/deferred/successful/failed) populate
    `counts`; rate overrides (ingress/egress) populate `rates`. An
    optional `interval` override sets the rates window.
    """
    interval = overrides.pop("interval", datetime.timedelta(seconds=60))
    counts = QueueCounts(
        ready=overrides.get("ready", 0),
        running=overrides.get("running", 0),
        deferred=overrides.get("deferred", 0),
        successful=overrides.get("successful", 0),
        failed=overrides.get("failed", 0),
    )
    rates = QueueRates(
        interval=interval,
        ingress=overrides.get("ingress", 0),
        egress=overrides.get("egress", 0),
    )
    return QueueStats(counts=counts, rates=rates)


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

    async def test_queue_stats_refresh_updates_and_prunes_queues(self):
        """Telemetry refresh updates existing queue labels and drops stale queues."""
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.telemetry = await app.backend.queue_stats()
            await pilot.pause()
            queue_list = app.query_one("#queue-list", QueueList)
            assert set(queue_list._items) == set(default_task_backend.queues)
            app.telemetry = BackendTelemetry(queues={"default": _stats()})
            await pilot.pause()
            assert list(queue_list._items) == ["default"]

    async def test_queue_stats_counts_are_scoped_to_selected_queue(self):
        """Tab counts reflect the selected queue, not backend-wide totals."""
        default_task_backend.enqueue(echo, args=[1])
        stats = _stats(ready=1)
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            app.telemetry = BackendTelemetry(
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
            app.telemetry = BackendTelemetry(queues={"default": _stats(ready=1500)})
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
            (acquired,) = default_task_backend.acquire(
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
            app.query_one("#task-list", TaskList).action_refresh()
            await pilot.pause()
            assert app.telemetry.queues["default"].counts.ready == 1

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
                "Priority",
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
        """The digit binding activates the matching status tab and refreshes it."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="tab-test"
        )
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            app.action_switch_tab("tab-running")
            await pilot.pause()
            await pilot.pause()
            assert task_list._tabs.active == "tab-running"
            running = app.query_one("#table-running", DataTable)
            assert not running.disabled
            assert running.row_count == 1

    async def test_successful_tab_lists_finished_task(self):
        """The successful tab renders acknowledged tasks with the finished column."""
        default_task_backend.enqueue(echo, args=[1])
        (acquired,) = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="succ-test"
        )
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
        """With auto-refresh on, the sparkline timer is armed on mount."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert app._sparkline_timer is not None
            assert app._sparkline_timer.name == "sparkline-refresh"
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            assert app._sparkline_timer is None

    async def test_initial_focus_on_queue_list(self):
        """The app opens with focus on the queue list, not the backend selector."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            assert app.focused is not None
            assert app.focused.id == "queue-list"

    async def test_queue_stats_refresh_does_not_re_peek_task_list(self):
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
            app.telemetry = await app.backend.queue_stats()
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
            app.query_one("#task-list", TaskList).action_refresh()
            await pilot.pause()
            await pilot.pause()
            assert table.row_count == before - 1


def _acknowledge_failed() -> str:
    """Enqueue, acquire, and acknowledge a task as FAILED. Return its ID."""
    task_result = default_task_backend.enqueue(echo, args=[1])
    (acquired,) = default_task_backend.acquire(
        timeout=datetime.timedelta(seconds=1), worker="inspector-test"
    )
    default_task_backend.acknowledge(
        dataclasses.replace(
            acquired, status=TaskResultStatus.FAILED, finished_at=timezone.now()
        )
    )
    return task_result.id


def _acknowledge_successful() -> str:
    """Enqueue, acquire, and acknowledge a task as SUCCESSFUL. Return its ID."""
    task_result = default_task_backend.enqueue(echo, args=[1])
    (acquired,) = default_task_backend.acquire(
        timeout=datetime.timedelta(seconds=1), worker="inspector-test"
    )
    default_task_backend.acknowledge(
        dataclasses.replace(
            acquired, status=TaskResultStatus.SUCCESSFUL, finished_at=timezone.now()
        )
    )
    return task_result.id


async def _select_failed_task(pilot, app) -> tuple[TaskList, TaskResult]:
    """Set up the task list with a failed task selected on the Failed tab."""
    task_id = _acknowledge_failed()
    task_list = app.query_one("#task-list", TaskList)
    task_list.queue_name = "default"
    await pilot.pause()
    task_list.switch_tab("tab-failed")
    await pilot.pause()
    await pilot.pause()
    failed_task = next(
        (r for r in task_list._current_results if r.id == task_id),
        None,
    )
    assert failed_task is not None, "Failed task not found in task list"
    task_list.selected_task = failed_task
    return task_list, failed_task


class TestCheckAction:
    """Tests for TaskList.check_action conditional binding visibility."""

    async def test_check_action__requeue_visible_only_on_failed_tab(self):
        """check_action shows requeue only when the Failed tab is active."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.switch_tab("tab-failed")
            await pilot.pause()
            assert task_list.check_action("requeue", ()) is True
            task_list.switch_tab("tab-ready")
            await pilot.pause()
            assert task_list.check_action("requeue", ()) is False

    async def test_check_action__dequeue_hidden_on_running_tab(self):
        """check_action hides dequeue on the Running tab."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.switch_tab("tab-running")
            await pilot.pause()
            assert task_list.check_action("dequeue", ()) is False

    async def test_check_action__dequeue_visible_on_non_running_tabs(self):
        """check_action shows dequeue on Ready, Successful, and Failed tabs."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            for tab in ("tab-ready", "tab-successful", "tab-failed"):
                task_list.switch_tab(tab)
                await pilot.pause()
                assert task_list.check_action("dequeue", ()) is True, tab

    async def test_check_action__other_actions_always_visible(self):
        """check_action always shows refresh binding."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            assert task_list.check_action("refresh", ()) is True


class TestRequeueAction:
    """Tests for TaskList.action_requeue and _do_requeue."""

    async def test_action_requeue__noop_without_selected_task(self):
        """Requeue action does nothing when no task is selected."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.switch_tab("tab-failed")
            await pilot.pause()
            task_list.action_requeue()
            await pilot.pause()
            assert not isinstance(app.screen, ConfirmScreen)

    async def test_action_requeue__noop_on_non_failed_tab(self):
        """Requeue action does nothing when not on the Failed tab."""
        _acknowledge_failed()
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            task_list.switch_tab("tab-ready")
            await pilot.pause()
            task_list.selected_task = next(
                (r for r in task_list._current_results),
                None,
            )
            task_list.action_requeue()
            await pilot.pause()
            assert not isinstance(app.screen, ConfirmScreen)

    async def test_action_requeue__pushes_confirm_screen(self):
        """Requeue action pushes a confirmation screen for a failed task."""
        task_id = _acknowledge_failed()
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            task_list.switch_tab("tab-failed")
            await pilot.pause()
            await pilot.pause()
            task_list.selected_task = next(
                (r for r in task_list._current_results if r.id == task_id),
                None,
            )
            task_list.action_requeue()
            await pilot.pause()
            assert isinstance(app.screen, ConfirmScreen)

    async def test_do_requeue__calls_backend_requeue(self):
        """_do_requeue requeues the failed task after confirmation."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list, failed_task = await _select_failed_task(pilot, app)
            task_list._do_requeue(True, failed_task)
            await pilot.pause()
            failed = list(
                default_task_backend.peek("default", status=TaskResultStatus.FAILED)
            )
            assert failed_task.id not in {r.id for r in failed}

    async def test_do_requeue__noop_when_not_confirmed(self):
        """_do_requeue leaves the failed task in place when not confirmed."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list, failed_task = await _select_failed_task(pilot, app)
            task_list._do_requeue(False, failed_task)
            await pilot.pause()
            failed = list(
                default_task_backend.peek("default", status=TaskResultStatus.FAILED)
            )
            assert failed_task.id in {r.id for r in failed}

    async def test_do_requeue__notifies_on_error(self):
        """_do_requeue logs and notifies when backend.requeue raises."""
        params = {k: v for k, v in settings.TASKS["default"].items() if k != "BACKEND"}
        erroring = ErroringBackend(alias="default", params=params)
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.backend = erroring
            await pilot.pause()
            task_list, failed_task = await _select_failed_task(pilot, app)
            task_list._do_requeue(True, failed_task)
            await pilot.pause()
            failed = list(
                default_task_backend.peek("default", status=TaskResultStatus.FAILED)
            )
            assert failed_task.id in {r.id for r in failed}


class TestDequeueAction:
    """Tests for TaskList.action_dequeue and _do_dequeue."""

    async def test_action_dequeue__noop_without_selected_task(self):
        """Dequeue action does nothing when no task is selected."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.switch_tab("tab-ready")
            await pilot.pause()
            task_list.action_dequeue()
            await pilot.pause()
            assert not isinstance(app.screen, ConfirmScreen)

    async def test_action_dequeue__pushes_confirm_screen(self):
        """Dequeue action pushes a confirmation screen for a selected task."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            task_list.selected_task = next(
                (r for r in task_list._current_results if r.id == task_result.id),
                None,
            )
            task_list.action_dequeue()
            await pilot.pause()
            assert isinstance(app.screen, ConfirmScreen)

    async def test_do_dequeue__calls_backend_dequeue(self):
        """_do_dequeue removes the task from the ready queue after confirmation."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            ready_task = next(
                (r for r in task_list._current_results if r.id == task_result.id),
                None,
            )
            task_list._do_dequeue(True, ready_task)
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert task_result.id not in {r.id for r in ready}

    async def test_do_dequeue__noop_when_not_confirmed(self):
        """_do_dequeue leaves the task in the ready queue when not confirmed."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            ready_task = next(
                (r for r in task_list._current_results if r.id == task_result.id),
                None,
            )
            task_list._do_dequeue(False, ready_task)
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert task_result.id in {r.id for r in ready}

    async def test_do_dequeue__notifies_on_error(self):
        """_do_dequeue logs and notifies when backend.dequeue raises."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        params = {k: v for k, v in settings.TASKS["default"].items() if k != "BACKEND"}
        erroring = ErroringBackend(alias="default", params=params)
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.backend = erroring
            await pilot.pause()
            task_list = app.query_one("#task-list", TaskList)
            task_list.queue_name = "default"
            await pilot.pause()
            await pilot.pause()
            ready_task = next(
                (r for r in task_list._current_results if r.id == task_result.id),
                None,
            )
            task_list._do_dequeue(True, ready_task)
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert task_result.id in {r.id for r in ready}


class TestPurgeAction:
    """Tests for QueueList.action_purge_queue and _do_purge."""

    async def test_action_purge_queue__noop_without_highlighted_item(self):
        """Purge action does nothing when no queue item is highlighted."""
        app = InspectorApp(backend=default_task_backend, auto_refresh=False)
        async with app.run_test() as pilot:
            await pilot.pause()
            queue_list = app.query_one("#queue-list", QueueList)
            queue_list.clear()
            queue_list._items.clear()
            await pilot.pause()
            assert queue_list.highlighted_child is None
            queue_list.action_purge()
            await pilot.pause()
            assert not isinstance(app.screen, PurgeScreen)

    async def test_action_purge_queue__pushes_purge_screen(self):
        """Purge action pushes a PurgeScreen for the highlighted queue."""
        default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            queue_list = app.query_one("#queue-list", QueueList)
            queue_list.action_purge()
            await pilot.pause()
            assert isinstance(app.screen, PurgeScreen)

    async def test_do_purge__calls_backend_purge_queue(self):
        """_do_purge empties the queue after confirmation."""
        default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            app.query_one("#queue-list", QueueList)._do_purge(True, "default")
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert ready == []

    async def test_do_purge__noop_when_not_confirmed(self):
        """_do_purge leaves tasks in the queue when not confirmed."""
        default_task_backend.enqueue(echo, args=[1])
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            await pilot.pause()
            app.query_one("#queue-list", QueueList)._do_purge(False, "default")
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert len(ready) == 1

    async def test_do_purge__notifies_on_error(self):
        """_do_purge logs and notifies when backend.purge_queue raises."""
        default_task_backend.enqueue(echo, args=[1])
        params = {k: v for k, v in settings.TASKS["default"].items() if k != "BACKEND"}
        erroring = ErroringBackend(alias="default", params=params)
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            app.backend = erroring
            await pilot.pause()
            await pilot.pause()
            app.query_one("#queue-list", QueueList)._do_purge(True, "default")
            await pilot.pause()
            ready = list(
                default_task_backend.peek("default", status=TaskResultStatus.READY)
            )
            assert len(ready) == 1


class TestConfirmScreen:
    """Tests for the ConfirmScreen modal dialog."""

    async def test_confirm_screen__enter_confirms(self):
        """Pressing Enter confirms and dismisses with True."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            result: list[bool] = []
            app.push_screen(ConfirmScreen("Confirm?"), lambda r: result.append(r))
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()
            assert result == [True]

    async def test_confirm_screen__escape_cancels(self):
        """Pressing Escape cancels and dismisses with False."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            result: list[bool] = []
            app.push_screen(ConfirmScreen("Confirm?"), lambda r: result.append(r))
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            assert result == [False]


class TestPurgeScreen:
    """Tests for the PurgeScreen modal dialog."""

    async def test_purge_screen__correct_name_confirms(self):
        """Typing the correct queue name and pressing Enter confirms."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            result: list[bool] = []
            app.push_screen(PurgeScreen("default"), lambda r: result.append(r))
            await pilot.pause()
            await pilot.press("d", "e", "f", "a", "u", "l", "t")
            await pilot.press("enter")
            await pilot.pause()
            assert result == [True]

    async def test_purge_screen__wrong_name_does_not_confirm(self):
        """Typing a wrong queue name does not dismiss the screen."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            result: list[bool] = []
            app.push_screen(PurgeScreen("default"), lambda r: result.append(r))
            await pilot.pause()
            await pilot.press("x", "y", "z")
            await pilot.press("enter")
            await pilot.pause()
            assert result == []

    async def test_purge_screen__escape_cancels(self):
        """Pressing Escape cancels and dismisses with False."""
        app = InspectorApp(backend=default_task_backend)
        async with app.run_test() as pilot:
            await pilot.pause()
            result: list[bool] = []
            app.push_screen(PurgeScreen("default"), lambda r: result.append(r))
            await pilot.pause()
            await pilot.press("escape")
            await pilot.pause()
            assert result == [False]
