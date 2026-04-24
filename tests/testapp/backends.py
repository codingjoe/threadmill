import uuid

from django.tasks import TaskResult, TaskResultStatus
from django.utils import timezone
from grinder.backends import AcknowledgeableTaskBackend


class CPUHeavyTaskBackend(AcknowledgeableTaskBackend):
    solved_task_count = 0
    issued_task_count = 0
    target_task_count = 1000

    def __init__(self, alias, params):
        super().__init__(alias=alias, params=params)
        self.reset()

    def reset(self):
        from .tasks import cpu_heavy_task

        CPUHeavyTaskBackend.solved_task_count = 0
        CPUHeavyTaskBackend.issued_task_count = 0
        self._task_generator = (
            TaskResult(
                task=cpu_heavy_task,
                enqueued_at=timezone.now(),
                status=TaskResultStatus.READY,
                id=str(uuid.uuid4()),
                args=[],
                kwargs={},
                worker_ids=[],
                started_at=None,
                finished_at=None,
                errors=[],
                backend=self.alias,
                last_attempted_at=None,
            )
            for _ in range(CPUHeavyTaskBackend.target_task_count)
        )

    def enqueue(self, task):
        return task

    def acquire(self, timeout=None):
        CPUHeavyTaskBackend.issued_task_count += 1
        try:
            task_result = next(self._task_generator)
        except StopIteration:
            raise TimeoutError("No tasks available within the specified timeout.")
        return task_result

    def acknowledge(self, task_result: TaskResult) -> None:
        CPUHeavyTaskBackend.solved_task_count += 1
