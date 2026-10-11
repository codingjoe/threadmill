"""Textual app for the inspector TUI."""

import asyncio
import dataclasses
import datetime
import logging
import math
import time
import typing
from typing import Any

from django.contrib.humanize.templatetags.humanize import naturaltime
from django.tasks import (
    DEFAULT_TASK_QUEUE_NAME,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.theme import BUILTIN_THEMES
from textual.widgets import (
    DataTable,
    Footer,
    ListItem,
    ListView,
    Select,
    Sparkline,
    Static,
    TabbedContent,
    TabPane,
)

from ..backends.base import (
    BackendTelemetry,
    TelemetryDirection,
    ThreadmillTaskBackend,
)
from .screens import ConfirmScreen, PurgeScreen
from .telemetry import TelemetryBuffer

logger = logging.getLogger(__name__)

TELEMETRY_INTERVAL = datetime.timedelta(seconds=2)
"""Between automatic queue-stat refreshes; the task list stays manual."""

SPARKLINE_INTERVAL = datetime.timedelta(seconds=1)
"""Between sparkline refreshes from the rolling telemetry buffer."""

TAB_STATUSES: list[tuple[str, TaskResultStatus | None]] = [
    ("Running", TaskResultStatus.RUNNING),
    ("Ready", TaskResultStatus.READY),
    ("Successful", TaskResultStatus.SUCCESSFUL),
    ("Failed", TaskResultStatus.FAILED),
]


class QueueItem(ListItem):
    """A list item that exposes its queue name."""

    def __init__(self, queue_name: str, label: str, **kwargs: Any) -> None:
        super().__init__(Static(label), id=f"queue-{queue_name}", **kwargs)
        self.queue_name = queue_name


def _supported_aliases() -> typing.Generator[tuple[str, str]]:
    for alias in task_backends:
        if isinstance(task_backends[alias], ThreadmillTaskBackend):
            yield alias, alias


def _format_dt(value: datetime.datetime | None) -> str:
    return value.isoformat() if value else ""


def si_prefix(n: int) -> str:
    """Round and shorten a number with an SI prefix (k, M, G, etc.)."""
    if n < 1000:
        return str(n)
    prefixes = ["", "k", "M", "G", "T", "P", "E", "Z", "Y"]
    m = min(int(math.log10(abs(n)) // 3), len(prefixes) - 1)
    if (value := n / 1000**m) and value >= 100:
        return f"{int(value)}{prefixes[m]}"
    return f"{value:.1f}".rstrip("0").rstrip(".") + prefixes[m]


COLUMN_LABELS: dict[str, str] = {
    "id": "ID",
    "priority": "Priority",
    "function": "Function",
    "enqueued": "Enqueued",
    "started": "Started",
    "finished": "Finished",
    "workers": "Workers",
}

TAB_COLUMNS: dict[str, tuple[str, ...]] = {
    "running": ("id", "function", "enqueued", "started", "workers"),
    "ready": ("id", "function", "priority", "enqueued"),
    "successful": ("id", "function", "enqueued", "started", "finished", "workers"),
    "failed": ("id", "function", "enqueued", "started", "finished", "workers"),
}

TAB_KEYS = {
    label.lower(): str(index + 1) for index, (label, _) in enumerate(TAB_STATUSES)
}


def _cell(result: TaskResult, column: str) -> str:
    match column:
        case "id":
            return result.id[:8]
        case "function":
            return result.task.module_path
        case "priority":
            return str(result.task.priority)
        case "enqueued":
            return naturaltime(result.enqueued_at)
        case "started":
            return naturaltime(result.started_at)
        case "finished":
            return naturaltime(result.finished_at)
        case "workers":
            return ", ".join(result.worker_ids) or "-"


class TaskDetail(Static):
    """Read-only detail view for the selected task result."""

    task_result: reactive[TaskResult | None] = reactive(None)

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Detail"

    def watch_task_result(self) -> None:
        self.update(self._detail_content())

    def _detail_content(self) -> str:
        """Build the detail text for the current task."""
        result = self.task_result
        if result is None:
            return "Select a task to view details."
        lines = [
            f"[b]Task:[/b] {result.task.module_path}",
            f"[b]ID:[/b] {result.id}",
            f"[b]Status:[/b] {result.status.name}",
            f"[b]Queue:[/b] {result.task.queue_name}",
            f"[b]Priority:[/b] {result.task.priority}",
            f"[b]Enqueued:[/b] {_format_dt(result.enqueued_at)}",
            f"[b]Started:[/b] {_format_dt(result.started_at)}",
            f"[b]Finished:[/b] {_format_dt(result.finished_at)}",
            f"[b]Last attempted:[/b] {_format_dt(result.last_attempted_at)}",
            f"[b]Worker IDs:[/b] {', '.join(result.worker_ids) or '-'}",
            f"[b]Args:[/b] {result.args!r}",
            f"[b]Kwargs:[/b] {result.kwargs!r}",
        ]
        if result.errors:
            lines.append("")
            lines.append("[b]Errors:[/b]")
            for error in result.errors:
                lines.append(f"  {error.exception_class_path}")
                lines.extend(f"    {line}" for line in error.traceback.splitlines())
        return "\n".join(lines)


class TaskList(Vertical):
    """Tabbed list of tasks for the selected queue."""

    backend: reactive[ThreadmillTaskBackend | None] = reactive(None)
    queue_name: reactive[str] = reactive("")
    telemetry: reactive[BackendTelemetry | None] = reactive(None)
    counts: reactive[dict[str, int]] = reactive({})
    selected_task: reactive[TaskResult | None] = reactive(None)

    BINDINGS = [
        Binding("f5", "refresh", "Refresh"),
        Binding("r", "requeue", "Requeue"),
        Binding("d", "dequeue", "Drop"),
    ]

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Show requeue only on the Failed tab, dequeue on non-Running tabs."""
        status = self.active_status()
        if action == "requeue":
            return status == TaskResultStatus.FAILED
        if action == "dequeue":
            return status is not None and status != TaskResultStatus.RUNNING
        return True

    def compose(self) -> ComposeResult:
        with TabbedContent(initial="tab-ready") as tabs:
            for label, _ in TAB_STATUSES:
                with TabPane(label, id=f"tab-{label.lower()}"):
                    yield DataTable(id=f"table-{label.lower()}", cursor_type="row")
        self._tabs = tabs
        self.border_title = "Tasks"

    def watch_backend(self) -> None:
        self._refresh_data()

    def watch_queue_name(self) -> None:
        self._refresh_data()

    def refresh_tasks(self) -> None:
        """Re-fetch and render tasks for the current queue and tab."""
        self._refresh_data()

    def compute_counts(self) -> dict[str, int]:
        if self.telemetry is None or not self.queue_name:
            return {label.lower(): 0 for label, _ in TAB_STATUSES}
        counts = self.telemetry.queues[self.queue_name].counts
        return {
            label.lower(): getattr(counts, label.lower()) for label, _ in TAB_STATUSES
        }

    def watch_counts(self, counts: dict[str, int]) -> None:
        for label, _status in TAB_STATUSES:
            tab = self._tabs.get_tab(f"tab-{label.lower()}")
            tab.label = f"{label} ({si_prefix(counts.get(label.lower(), 0))})"

    def watch_selected_task(self, task: TaskResult | None) -> None:
        if self.app._task_detail is not None:
            self.app._task_detail.task_result = task

    def on_mount(self) -> None:
        """Configure per-status columns on first mount."""
        for label, _ in TAB_STATUSES:
            tab_id = label.lower()
            table = self.query_one(f"#table-{tab_id}", DataTable)
            table.add_columns(
                *(COLUMN_LABELS[column] for column in TAB_COLUMNS[tab_id])
            )
            table.disabled = tab_id != "ready"

    def switch_tab(self, tab_id: str) -> None:
        """Activate the tab with the given id."""
        self._tabs.active = tab_id

    def action_refresh(self) -> None:
        """Refresh telemetry and re-fetch the task list."""
        self.app._poll_queue_stats()
        self.refresh_tasks()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Notify the app when a task row is selected."""
        if isinstance(event.row_key.value, str):
            self._select_task_by_id(event.row_key.value)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        """Update the detail preview when a row is highlighted."""
        if isinstance(event.row_key.value, str):
            self._select_task_by_id(event.row_key.value)

    def on_tabbed_content_tab_activated(
        self, _event: TabbedContent.TabActivated
    ) -> None:
        """Refresh the visible table when the user switches tabs."""
        self._refresh_data()
        self.refresh_bindings()

    def _select_task_by_id(self, task_id: str) -> None:
        """Find the task result matching the row key in the current results."""
        self.selected_task = next(
            (result for result in self._current_results if result.id == task_id),
            None,
        )

    def active_status(self) -> TaskResultStatus | None:
        """Return the status segment of the currently active task tab."""
        tab_id = self._tabs.active.removeprefix("tab-")
        return next(
            (status for label, status in TAB_STATUSES if label.lower() == tab_id),
            None,
        )

    def action_requeue(self) -> None:
        """Requeue the selected failed task after confirmation."""
        task = self.selected_task
        if task and self.active_status() == TaskResultStatus.FAILED:
            self.app.push_screen(
                ConfirmScreen(f"Requeue task {task.id[:8]}?"),
                lambda confirmed: self._do_requeue(confirmed, task),
            )

    def _do_requeue(self, confirmed: bool, task: TaskResult) -> None:
        if confirmed:
            try:
                self.backend.requeue(task, datetime.datetime.now(datetime.UTC))
            except Exception:
                logger.exception("Requeue failed for task %s", task.id)
                self.app.notify("Requeue failed.", severity="error")
            else:
                self.app._after_action("Task requeued.")

    def action_dequeue(self) -> None:
        """Remove the selected task from its current segment after confirmation."""
        task = self.selected_task
        if task and self.active_status() is not None:
            self.app.push_screen(
                ConfirmScreen(
                    f"Delete task {task.task.func.__name__} ({task.id[:8]})?",
                    danger=True,
                ),
                lambda confirmed: self._do_dequeue(confirmed, task),
            )

    def _do_dequeue(self, confirmed: bool, task: TaskResult) -> None:
        if confirmed:
            try:
                self.backend.dequeue(task)
            except Exception:
                logger.exception("Delete failed for task %s", task.id)
                self.app.notify("Delete failed.", severity="error")
            else:
                self.app._after_action("Task deleted.")

    def _refresh_data(self) -> None:
        """Fetch and display tasks for the current queue and active tab."""
        backend = self.backend
        queue_name = self.queue_name
        if backend is None or not queue_name:
            self._current_results = []
            return
        tab_id = self._tabs.active.removeprefix("tab-")
        status = next(
            (status for label, status in TAB_STATUSES if label.lower() == tab_id),
            None,
        )
        for label, _ in TAB_STATUSES:
            table = self.query_one(f"#table-{label.lower()}", DataTable)
            table.clear()
            table.disabled = label.lower() != tab_id
        table = self.query_one(f"#table-{tab_id}", DataTable)
        table.disabled = False
        try:
            self._current_results = list(
                backend.peek(queue_name=queue_name, status=status, count=100)
            )
        except Exception:
            logger.exception("Failed to peek tasks for %r", queue_name)
            self._current_results = []
        columns = TAB_COLUMNS[tab_id]
        for result in self._current_results:
            table.add_row(
                *(_cell(result, column) for column in columns),
                key=result.id,
            )
        self._restore_cursor(table)

    def _restore_cursor(self, table: DataTable) -> None:
        """Keep the highlighted task selected across a refresh, falling back to the first row."""
        previous_id = self.selected_task.id if self.selected_task else None
        row = next(
            (
                index
                for index, result in enumerate(self._current_results)
                if result.id == previous_id
            ),
            None,
        )
        if row is None:
            row = 0 if self._current_results else None
        if row is not None:
            table.move_cursor(row=row)
        self.selected_task = self._current_results[row] if row is not None else None


class QueueList(ListView):
    """List of queues for the selected backend with ingress/egress deltas."""

    telemetry: reactive[BackendTelemetry | None] = reactive(None)

    BINDINGS = [
        Binding("p", "purge", "Purge"),
    ]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: dict[str, QueueItem] = {}

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Queues"

    def watch_telemetry(self, telemetry: BackendTelemetry | None) -> None:
        if telemetry is not None:
            self.update_telemetry(telemetry)

    def update_telemetry(self, telemetry: BackendTelemetry) -> None:
        queues = telemetry.queues
        for queue_name, stats in sorted(queues.items()):
            theme = BUILTIN_THEMES[self.app.theme]
            rates = stats.rates
            label = (
                f"{queue_name:24}  "
                f"[{theme.success}]+{si_prefix(rates.ingress):3}[/]  "
                f"[{theme.error}]-{si_prefix(rates.egress):3}[/]"
            )
            if queue_name in self._items:
                self._items[queue_name].children[0].update(label)
            else:
                item = QueueItem(queue_name, label)
                self._items[queue_name] = item
                self.append(item)
        stale = [name for name in self._items if name not in queues]
        for queue_name in stale:
            self._items.pop(queue_name).remove()
        if self.index is None and self._items:
            target = (
                DEFAULT_TASK_QUEUE_NAME
                if DEFAULT_TASK_QUEUE_NAME in self._items
                else next(iter(self._items))
            )
            self.index = list(self._items.keys()).index(target)
            self._notify_selection(target)

    def _notify_selection(self, queue_name: str) -> None:
        self.app._task_list.queue_name = queue_name

    def action_purge(self) -> None:
        item = self.highlighted_child
        if isinstance(item, QueueItem):
            queue_name = item.queue_name
            self.app.push_screen(
                PurgeScreen(queue_name),
                lambda confirmed: self._do_purge(confirmed, queue_name),
            )

    def _do_purge(self, confirmed: bool, queue_name: str) -> None:
        if confirmed:
            try:
                self.app.backend.purge(queue_name)
            except Exception:
                logger.exception("Purge failed for %r", queue_name)
                self.app.notify("Purge failed.", severity="error")
            else:
                self.app._after_action(f"Purged queue {queue_name}.")


class InspectorApp(App):
    """Threadmill TUI inspector with backend/queue/task panes."""

    CSS_PATH = "inspector.scss"
    BINDINGS = [
        *(
            Binding(key, f"switch_tab('tab-{tab_id}')", tab_id.capitalize())
            for tab_id, key in TAB_KEYS.items()
        ),
        Binding("q", "quit", "Quit"),
    ]

    backend: reactive[ThreadmillTaskBackend] = reactive(None)
    telemetry: reactive[BackendTelemetry] = reactive(None, always_update=True)

    def __init__(
        self,
        backend: ThreadmillTaskBackend,
        *,
        auto_refresh: bool = True,
    ) -> None:
        super().__init__()
        self._queue_list: QueueList | None = None
        self._task_list: TaskList | None = None
        self._task_detail: TaskDetail | None = None
        self._options_static: Static | None = None
        self._sparkline_timer = None
        self._telemetry_timer = None
        self._auto_refresh = auto_refresh
        self.telemetry_buffer = TelemetryBuffer()
        self.set_reactive(InspectorApp.backend, backend)

    def compose(self) -> ComposeResult:
        self.title = "Threadmill"
        self.sub_title = "Inspector"
        with Vertical(id="split-view"):
            with Horizontal(id="backend-row"):
                yield Select(
                    _supported_aliases(),
                    id="backend-select",
                    value=self.backend.alias,
                    allow_blank=False,
                )
                yield Static(id="backend-options")
            with Horizontal():
                with Vertical(id="left-pane"):
                    yield QueueList(id="queue-list").data_bind(
                        telemetry=InspectorApp.telemetry
                    )
                with Vertical(id="right-pane"):
                    with Horizontal(id="sparkline-row"):
                        yield Sparkline(id="ingress-sparkline")
                        yield Sparkline(id="egress-sparkline")
                    yield TaskList(id="task-list").data_bind(
                        backend=InspectorApp.backend,
                        telemetry=InspectorApp.telemetry,
                    )
                    yield TaskDetail(id="task-detail", name="Task Detail")
        yield Footer(show_command_palette=True)

    def on_mount(self) -> None:
        """Register the theme, render the initial snapshot, and arm auto-refresh."""
        self.theme = "monokai"
        self._queue_list = self.query_one("#queue-list", QueueList)
        self._task_list = self.query_one("#task-list", TaskList)
        self._task_detail = self.query_one("#task-detail", TaskDetail)
        self._options_static = self.query_one("#backend-options", Static)
        self._refresh_options()
        self.query_one("#ingress-sparkline", Sparkline).border_title = "Ingress"
        self.query_one("#egress-sparkline", Sparkline).border_title = "Egress"
        self._poll_queue_stats()
        self._listen_telemetry()
        if self._auto_refresh:
            self._telemetry_timer = self.set_interval(
                TELEMETRY_INTERVAL.total_seconds(),
                self._poll_queue_stats,
                name="telemetry-refresh",
            )
            self._sparkline_timer = self.set_interval(
                SPARKLINE_INTERVAL.total_seconds(),
                self._refresh_sparklines,
                name="sparkline-refresh",
            )
        self._queue_list.focus()

    def action_quit(self) -> None:
        """Exit the TUI."""
        self.exit()

    def _after_action(self, message: str) -> None:
        """Refresh telemetry and tasks after a mutating queue action."""
        self.notify(message)
        self._poll_queue_stats()
        self._task_list.refresh_tasks()

    def action_switch_tab(self, tab_id: str) -> None:
        """Activate the task status tab matching the given id."""
        self._task_list.switch_tab(tab_id)

    def watch_backend(self) -> None:
        self._refresh_options()
        self.telemetry_buffer = TelemetryBuffer()
        self._poll_queue_stats()
        self._listen_telemetry()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Switch backend when the user selects a different alias."""
        self.backend = task_backends[event.value]

    def _queue_from_item(self, item: ListItem | None) -> str | None:
        """Extract queue name from a queue list item id."""
        return item.id.removeprefix("queue-") if item is not None else None

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Update the task list when a queue is selected."""
        if queue_name := self._queue_from_item(event.item):
            self._task_list.queue_name = queue_name

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        """Preview the selected queue without requiring Enter."""
        if queue_name := self._queue_from_item(event.item):
            self._task_list.queue_name = queue_name

    def _refresh_options(self) -> None:
        """Show the selected backend's constructor options."""
        parts = [
            f"{key}={value!r}" for key, value in sorted(self.backend.options.items())
        ]
        self._options_static.update(" ".join(parts) or "No options")

    @work(exclusive=True, group="queue-stats")
    async def _poll_queue_stats(self) -> None:
        """Fetch queue counts and merge live pub/sub rates into the telemetry."""
        try:
            telemetry = await self.backend.queue_stats()
        except Exception:
            logger.exception("Failed to refresh queue stats")
        else:
            for queue_name, stats in telemetry.queues.items():
                live_rates = self.telemetry_buffer.rates_for(queue_name)
                telemetry.queues[queue_name] = dataclasses.replace(
                    stats, rates=live_rates
                )
            self.telemetry = telemetry

    def _refresh_sparklines(self) -> None:
        """Push the latest 60-second rolling series into the ingress/egress sparklines."""
        queue_name = self._task_list.queue_name
        ingress = self.query_one("#ingress-sparkline", Sparkline)
        egress = self.query_one("#egress-sparkline", Sparkline)
        now = time.monotonic()
        ingress.data = (
            self.telemetry_buffer.series(
                queue_name, TelemetryDirection.INGRESS, now=now
            )
            if queue_name
            else [0.0] * 60
        )
        egress.data = (
            self.telemetry_buffer.series(queue_name, TelemetryDirection.EGRESS, now=now)
            if queue_name
            else [0.0] * 60
        )

    @work(exclusive=True, group="telemetry")
    async def _listen_telemetry(self) -> None:
        """Feed the telemetry buffer from the backend's pub/sub stream."""
        try:
            stream = self.backend.worker_telemetry()
            if asyncio.iscoroutine(stream):
                stream = await stream
        except NotImplementedError:
            pass
        else:
            if stream is not None:
                async for event in stream:
                    self.telemetry_buffer.record(event, now=time.monotonic())
