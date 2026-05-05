from queue import Empty

from django.tasks import TaskResult, TaskResultStatus
from django.utils import timezone
from django.utils.module_loading import import_string
from threadmill.backends import AcknowledgeableTaskBackend


class GeneratingTaskBackend(AcknowledgeableTaskBackend):
    solved_task_count = 0
    issued_task_count = 0
    target_task_count = 100
    supports_async_task = True

    def __init__(self, alias, params):
        super().__init__(alias=alias, params=params)
        self._queues = None

    def reset(self, task_count=100):
        GeneratingTaskBackend.solved_task_count = 0
        GeneratingTaskBackend.issued_task_count = 0
        self._queues = {
            "default": [
                TaskResult(
                    task=import_string("tests.testapp.tasks.random_crash"),
                    enqueued_at=timezone.now(),
                    status=TaskResultStatus.READY,
                    id=f"default-{i + 1}",
                    args=[],
                    kwargs={},
                    worker_ids=[],
                    started_at=None,
                    finished_at=None,
                    errors=[],
                    backend=self.alias,
                    last_attempted_at=None,
                )
                for i in range(task_count)
            ],
            "compute": [
                TaskResult(
                    task=import_string("tests.testapp.tasks.compute_workload"),
                    enqueued_at=timezone.now(),
                    status=TaskResultStatus.READY,
                    id=f"compute-{i + 1}",
                    args=[],
                    kwargs={},
                    worker_ids=[],
                    started_at=None,
                    finished_at=None,
                    errors=[],
                    backend=self.alias,
                    last_attempted_at=None,
                )
                for i in range(task_count)
            ],
            "io": [
                TaskResult(
                    task=import_string("tests.testapp.tasks.io_workload"),
                    enqueued_at=timezone.now(),
                    status=TaskResultStatus.READY,
                    id=f"io-{i + 1}",
                    args=[],
                    kwargs={},
                    worker_ids=[],
                    started_at=None,
                    finished_at=None,
                    errors=[],
                    backend=self.alias,
                    last_attempted_at=None,
                )
                for i in range(task_count)
            ],
            "memory": [
                TaskResult(
                    task=import_string("tests.testapp.tasks.memory_workload"),
                    enqueued_at=timezone.now(),
                    status=TaskResultStatus.READY,
                    id=f"memory-{i + 1}",
                    args=[],
                    kwargs={},
                    worker_ids=[],
                    started_at=None,
                    finished_at=None,
                    errors=[],
                    backend=self.alias,
                    last_attempted_at=None,
                )
                for i in range(task_count)
            ],
        }

    def enqueue(self, task):
        return task

    def acquire(self, *queue_names, timeout=None):
        if self._queues is None:
            self.reset()
        GeneratingTaskBackend.issued_task_count += 1
        queues = [self._queues[queue_name] for queue_name in queue_names]
        try:
            # pop from the longest queue first to simulate a more realistic scenario
            for queue in sorted(queues, key=len, reverse=True):
                return queue.pop(0)
        except IndexError as e:
            GeneratingTaskBackend.issued_task_count -= 1
            raise Empty("No more tasks to solve.") from e
        raise Empty("No more tasks to solve.")

    def acknowledge(self, task_result: TaskResult) -> None:
        GeneratingTaskBackend.solved_task_count += 1
