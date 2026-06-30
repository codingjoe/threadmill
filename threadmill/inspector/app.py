"""Textual app for the inspector TUI."""

from __future__ import annotations

import dataclasses
import datetime
import logging
import math
import typing
from typing import Any

from django.contrib.humanize.templatetags.humanize import naturaltime
from django.tasks import (
    DEFAULT_TASK_QUEUE_NAME,
    TaskResult,
    TaskResultStatus,
    task_backends,
)
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
    Tree,
)
from textual.widgets.tree import TreeNode

from ..backends.base import (
    BackendTelemetry,
    ThreadmillTaskBackend,
    WorkerTelemetry,
)

logger = logging.getLogger(__name__)

TELEMETRY_INTERVAL_SECONDS = 2.0
"""Seconds between automatic queue-stat refreshes; the task list stays manual."""

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
                for line in error.traceback.splitlines():
                    lines.append(f"    {line}")
        return "\n".join(lines)


class TaskList(Vertical):
    """Tabbed list of tasks for the selected queue."""

    backend: reactive[ThreadmillTaskBackend | None] = reactive(None)
    queue_name: reactive[str] = reactive("")
    telemetry: reactive[BackendTelemetry | None] = reactive(None)
    counts: reactive[dict[str, int]] = reactive({})
    selected_task: reactive[TaskResult | None] = reactive(None)

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

    def _select_task_by_id(self, task_id: str) -> None:
        """Find the task result matching the row key in the current results."""
        self.selected_task = next(
            (result for result in self._current_results if result.id == task_id),
            None,
        )

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
        except Exception:  # noqa: BLE001
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

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._items: dict[str, QueueItem] = {}

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Queues"

    def watch_telemetry(self, telemetry: BackendTelemetry | None) -> None:
        """Refresh queue labels when a new telemetry snapshot arrives."""
        if telemetry is not None:
            self.update_telemetry(telemetry)

    def update_telemetry(self, telemetry: BackendTelemetry) -> None:
        """Refresh queue labels from a new telemetry snapshot."""
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
        """Tell the app which queue to display."""
        self.app._task_list.queue_name = queue_name


@dataclasses.dataclass
class WorkerTreeNode:
    """A node in the Queue -> Node -> Worker selection tree."""

    kind: str
    label: str
    queue_name: str = ""
    hostname: str = ""
    worker_name: str = ""


class SelectionTree(Tree[WorkerTreeNode]):
    """Queue -> Node -> Worker hierarchy built from worker telemetry."""

    telemetry: reactive[WorkerTelemetry | None] = reactive(None)

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Selection"
        self.show_root = False

    def watch_telemetry(self, telemetry: WorkerTelemetry | None) -> None:
        """Rebuild the tree when a new telemetry snapshot arrives."""
        if telemetry is not None:
            self.update_telemetry(telemetry)

    def update_telemetry(self, telemetry: WorkerTelemetry) -> None:
        """Rebuild the tree from a telemetry snapshot, preserving the cursor."""
        previous_data = self.cursor_node.data if self.cursor_node else None
        self.clear()
        for queue_name in sorted(telemetry.queues):
            queue_node = self.root.add(
                queue_name,
                WorkerTreeNode(
                    kind="queue",
                    label=queue_name,
                    queue_name=queue_name,
                ),
                expand=True,
            )
            for hostname in telemetry.queues[queue_name]:
                node_telemetry = telemetry.nodes.get(hostname)
                if node_telemetry is None:
                    continue
                host_label = f"{hostname}  cpu {node_telemetry.cpu_percent:.0f}%  mem {si_prefix(node_telemetry.memory_bytes)}B"
                host_node = queue_node.add(
                    host_label,
                    WorkerTreeNode(
                        kind="node",
                        label=host_label,
                        queue_name=queue_name,
                        hostname=hostname,
                    ),
                    expand=True,
                )
                for worker_name, worker in sorted(node_telemetry.workers.items()):
                    worker_label = (
                        f"{worker_name}  "
                        f"cpu {worker.cpu_percent:.0f}%  "
                        f"mem {si_prefix(worker.memory_bytes)}B  "
                        f"{worker.tasks_per_minute:.0f}/min"
                    )
                    host_node.add_leaf(
                        worker_label,
                        WorkerTreeNode(
                            kind="worker",
                            label=worker_label,
                            queue_name=queue_name,
                            hostname=hostname,
                            worker_name=worker_name,
                        ),
                    )
        self._restore_cursor(previous_data)

    def _restore_cursor(self, previous_data: WorkerTreeNode | None) -> None:
        """Move the cursor to the previously selected node, if still present."""
        if previous_data is None or not self.root.children:
            return
        node = self._find_node_by_data(self.root, previous_data)
        if node is not None:
            self.call_after_refresh(self.select_node, node)

    @staticmethod
    def _find_node_by_data(
        root: TreeNode[WorkerTreeNode], target: WorkerTreeNode
    ) -> TreeNode[WorkerTreeNode] | None:
        """Depth-first search for a tree node whose data matches *target*."""
        for child in root.children:
            if child.data == target:
                return child
            result = SelectionTree._find_node_by_data(child, target)
            if result is not None:
                return result
        return None


class WorkerGraphs(Static):
    """Fixed-position throughput/CPU/memory graphs for the worker view."""

    telemetry: reactive[WorkerTelemetry | None] = reactive(None)
    selection: reactive[WorkerTreeNode | None] = reactive(None)

    GRAPH_HISTORY_SIZE = 60

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._throughput_history: list[float] = []
        self._cpu_history: list[float] = []
        self._memory_history: list[float] = []

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.border_title = "Worker Graphs"
        yield Sparkline(id="worker-throughput-graph", data=[])
        yield Sparkline(id="worker-cpu-graph", data=[])
        yield Sparkline(id="worker-memory-graph", data=[])

    def watch_selection(self) -> None:
        """Reset histories when the selection changes."""
        self._throughput_history = []
        self._cpu_history = []
        self._memory_history = []
        self._refresh_graphs()

    def watch_telemetry(self) -> None:
        self._refresh_graphs()

    def _refresh_graphs(self) -> None:
        """Append the current sample to each graph and redraw."""
        telemetry = self.telemetry
        selection = self.selection
        if telemetry is None or selection is None:
            return
        node = telemetry.nodes.get(selection.hostname)
        if node is None:
            return
        if selection.kind == "node":
            throughput = node.tasks_per_minute
            cpu = node.cpu_percent
            memory = node.memory_percent
        elif selection.kind == "worker":
            worker = node.workers.get(selection.worker_name)
            if worker is None:
                return
            throughput = worker.tasks_per_minute
            cpu = worker.cpu_percent
            memory = worker.memory_bytes
        else:
            return
        self._throughput_history.append(throughput)
        self._cpu_history.append(cpu)
        self._memory_history.append(memory)
        self._throughput_history = self._throughput_history[-self.GRAPH_HISTORY_SIZE :]
        self._cpu_history = self._cpu_history[-self.GRAPH_HISTORY_SIZE :]
        self._memory_history = self._memory_history[-self.GRAPH_HISTORY_SIZE :]
        try:
            self.query_one("#worker-throughput-graph", Sparkline).data = list(
                self._throughput_history
            )
            self.query_one("#worker-cpu-graph", Sparkline).data = list(
                self._cpu_history
            )
            self.query_one("#worker-memory-graph", Sparkline).data = list(
                self._memory_history
            )
        except Exception:  # noqa: BLE001
            logger.debug("Worker graph widgets not yet mounted")


class InspectorApp(App):
    """Threadmill TUI inspector with backend/queue/task panes."""

    CSS_PATH = "inspector.scss"
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("f5", "refresh", "Refresh"),
        Binding("v", "toggle_view", "Toggle View"),
        *(
            Binding(key, f"switch_tab('tab-{tab_id}')", tab_id.capitalize())
            for tab_id, key in TAB_KEYS.items()
        ),
    ]

    backend: reactive[ThreadmillTaskBackend] = reactive(None)
    telemetry: reactive[BackendTelemetry] = reactive(None, always_update=True)
    worker_telemetry: reactive[WorkerTelemetry | None] = reactive(
        None, always_update=True
    )
    worker_view_enabled: reactive[bool] = reactive(False)

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
        self._selection_tree: SelectionTree | None = None
        self._worker_graphs: WorkerGraphs | None = None
        self._telemetry_timer = None
        self._auto_refresh = auto_refresh
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
                    yield SelectionTree(
                        "Worker Telemetry", id="selection-tree"
                    ).data_bind(telemetry=InspectorApp.worker_telemetry)
                with Vertical(id="right-pane"):
                    yield TaskList(id="task-list").data_bind(
                        backend=InspectorApp.backend,
                        telemetry=InspectorApp.telemetry,
                    )
                    yield TaskDetail(id="task-detail", name="Task Detail")
                    yield WorkerGraphs(id="worker-graphs").data_bind(
                        telemetry=InspectorApp.worker_telemetry,
                    )
        yield Footer(show_command_palette=True)

    def on_mount(self) -> None:
        """Register the theme, render the initial snapshot, and arm auto-refresh."""
        self.theme = "monokai"
        self._queue_list = self.query_one("#queue-list", QueueList)
        self._task_list = self.query_one("#task-list", TaskList)
        self._task_detail = self.query_one("#task-detail", TaskDetail)
        self._options_static = self.query_one("#backend-options", Static)
        self._selection_tree = self.query_one("#selection-tree", SelectionTree)
        self._worker_graphs = self.query_one("#worker-graphs", WorkerGraphs)
        self._refresh_options()
        self._refresh_telemetry()
        if self._auto_refresh:
            self._telemetry_timer = self.set_interval(
                TELEMETRY_INTERVAL_SECONDS,
                self._refresh_telemetry,
                name="telemetry-refresh",
            )
        self._apply_worker_view_visibility()
        self._queue_list.focus()

    def action_quit(self) -> None:
        """Exit the TUI."""
        self.exit()

    def action_refresh(self) -> None:
        """Refresh queue stats and re-fetch the task list on demand."""
        self._refresh_telemetry()
        self._task_list.refresh_tasks()

    def action_switch_tab(self, tab_id: str) -> None:
        """Activate the task status tab matching the given id."""
        self._task_list.switch_tab(tab_id)

    def action_toggle_view(self) -> None:
        """Switch between queue view and worker view."""
        self.worker_view_enabled = not self.worker_view_enabled

    def watch_worker_view_enabled(self, enabled: bool) -> None:
        """Show/hide widgets when the view mode changes."""
        self._apply_worker_view_visibility()

    def _apply_worker_view_visibility(self) -> None:
        """Toggle display of queue vs worker widgets."""
        if self.worker_view_enabled:
            self._queue_list.display = False
            self._task_list.display = False
            self._task_detail.display = False
            self._selection_tree.display = True
            self._worker_graphs.display = True
            self._selection_tree.focus()
        else:
            self._queue_list.display = True
            self._task_list.display = True
            self._task_detail.display = True
            self._selection_tree.display = False
            self._worker_graphs.display = False
            self._queue_list.focus()

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Update worker graphs when a tree node is selected."""
        if event.node.data is not None and isinstance(event.node.data, WorkerTreeNode):
            self._worker_graphs.selection = event.node.data

    def watch_backend(self) -> None:
        self._refresh_options()
        self._refresh_telemetry()

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

    def _refresh_telemetry(self) -> None:
        """Poll the backend for fresh queue and worker telemetry."""
        try:
            self.telemetry = self.backend.telemetry()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to refresh telemetry")
        try:
            self.worker_telemetry = self.backend.worker_telemetry()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to refresh worker telemetry")
