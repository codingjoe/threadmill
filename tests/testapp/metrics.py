"""Recording metrics double for tests."""

from threadmill.metrics import Metrics


class RecordingMetrics(Metrics):
    """Record every recorder call in memory for assertions."""

    def __init__(self, *, events: list[str] | None = None) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.shutdown_called = False
        self.events = events

    def _record(self, name: str, **payload) -> None:
        self.calls.append((name, payload))

    def calls_named(self, name: str) -> list[dict]:
        """Return the payloads of every call with the given name."""
        return [payload for call_name, payload in self.calls if call_name == name]

    def record_task_started(self, *, queue_name, task_module_path):
        self._record(
            "task_started",
            queue_name=queue_name,
            task_module_path=task_module_path,
        )

    def record_task_finished(
        self,
        *,
        queue_name,
        task_module_path,
        status,
        duration_s,
    ):
        self._record(
            "task_finished",
            queue_name=queue_name,
            task_module_path=task_module_path,
            status=status,
            duration_s=duration_s,
        )

    def record_busy_threads(self, *, busy_threads, thread_count):
        self._record(
            "busy_threads",
            busy_threads=busy_threads,
            thread_count=thread_count,
        )

    def record_queue_tasks(self, *, queue_name, status, count):
        self._record(
            "queue_tasks",
            queue_name=queue_name,
            status=status,
            count=count,
        )

    def record_queue_backlog(self, *, queue_name, count):
        self._record("queue_backlog", queue_name=queue_name, count=count)

    def record_queue_ingress(self, *, queue_name):
        self._record("queue_ingress", queue_name=queue_name)

    def record_queue_egress(self, *, queue_name):
        self._record("queue_egress", queue_name=queue_name)

    def shutdown(self):
        self.shutdown_called = True
        if self.events is not None:
            self.events.append("metrics.shutdown")
