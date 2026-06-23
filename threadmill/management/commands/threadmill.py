import signal
import sys

from django.core.management import BaseCommand, CommandError
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    InvalidTaskBackend,
    task_backends,
)

from ...executor import TaskExecutor


def kill_softly(signum, frame):
    """Raise a KeyboardInterrupt to stop the worker gracefully."""
    signame = signal.Signals(signum).name
    raise KeyboardInterrupt(f"Received {signame} ({signum}), shutting down…")


class Command(BaseCommand):
    """Run task workers to process enqueued tasks from the specified backends and queues."""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-b",
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Alias of the tasks backend to use.",
        )
        parser.add_argument(
            "-q",
            "--queues",
            nargs="+",
            default=[DEFAULT_TASK_QUEUE_NAME],
            help="Queue names to listen to and process tasks from.",
        )
        parser.add_argument(
            "-w",
            "--workers",
            type=int,
            help="Number of worker processes to use. Defaults to the number of CPU cores minus one.",
        )
        parser.add_argument(
            "-t",
            "--threads",
            type=int,
            default=1,
            help="Number of threads to use. Defaults to 1. ",
        )
        parser.add_argument(
            "--max-tasks",
            type=int,
            default=0,
            help=(
                "Number of the maximum number of tasks to run until a worker is recycled."
                " Defaults to 0, which means no limit."
            ),
        )
        parser.add_argument(
            "--max-tasks-jitter",
            type=int,
            default=0,
            help="Maximum random jitter to add to the max-tasks value by randint(0, max_tasks_jitter).",
        )
        parser.add_argument(
            "--task-backlog-size",
            type=int,
            default=1,
            help="The number of tasks to prefetch from the message queue while all workers are busy. Defaults to 1.",
        )
        parser.add_argument(
            "--exit-empty",
            action="store_true",
            help="Drain the task queue and exit with 0.",
        )

    def handle(
        self,
        *,
        verbosity,
        backend,
        queues,
        workers,
        threads,
        max_tasks,
        max_tasks_jitter,
        exit_empty,
        **options,
    ):
        match sys.platform:
            case "win32":
                signal.signal(signal.SIGBREAK, kill_softly)
            case _:
                signal.signal(signal.SIGHUP, kill_softly)
        signal.signal(signal.SIGTERM, kill_softly)
        signal.signal(signal.SIGINT, kill_softly)
        self.stdout.write(self.style.SUCCESS("Starting workers…"))
        try:
            backend = task_backends[backend]
        except InvalidTaskBackend:
            raise CommandError(f"Invalid backend: {backend!r}")
        if _non_queues := set(queues) - set(backend.queues):
            raise CommandError(
                f"Backend does not support all specified queues: {_non_queues!r}"
            )
        exe = TaskExecutor(
            backend=backend,
            workers=workers,
            threads=threads,
            max_tasks=max_tasks,
            max_tasks_jitter=max_tasks_jitter,
            exit_empty=exit_empty,
            queues=queues,
        )
        try:
            exe.run()
        except KeyboardInterrupt as e:
            self.stdout.write(self.style.WARNING(str(e)))
            self.stdout.write(self.style.NOTICE("Shutting down workers…"))
            exe.shutdown()
