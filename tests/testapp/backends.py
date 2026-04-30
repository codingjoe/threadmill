from queue import Empty

from django.tasks import TaskResult, TaskResultStatus
from django.utils import timezone
from django.utils.module_loading import import_string
from grinder.backends import AcknowledgeableTaskBackend


class CPUHeavyTaskBackend(AcknowledgeableTaskBackend):
    solved_task_count = 0
    issued_task_count = 0
    target_task_count = 100

    def __init__(self, alias, params):
        super().__init__(alias=alias, params=params)
        self._task_generator = None

    def reset(self):
        CPUHeavyTaskBackend.solved_task_count = 0
        CPUHeavyTaskBackend.issued_task_count = 0
        self._task_generator = (
            TaskResult(
                task=import_string("tests.testapp.tasks.cpu_heavy_task"),
                enqueued_at=timezone.now(),
                status=TaskResultStatus.READY,
                id=str(i + 1),
                args=[],
                kwargs={},
                worker_ids=[],
                started_at=None,
                finished_at=None,
                errors=[],
                backend=self.alias,
                last_attempted_at=None,
            )
            for i in range(CPUHeavyTaskBackend.target_task_count)
        )

    def enqueue(self, task):
        return task

    def acquire(self, timeout=None):
        if self._task_generator is None:
            self.reset()
        CPUHeavyTaskBackend.issued_task_count += 1
        try:
            return next(self._task_generator)
        except StopIteration as e:
            raise Empty("No more tasks to solve.") from e

    def acknowledge(self, task_result: TaskResult) -> None:
        CPUHeavyTaskBackend.solved_task_count += 1
