import datetime
import signal
import sys

from django.core.management import BaseCommand
from django.tasks import task_backends

from ...executor import TaskExecutor


def kill_softly(signum, frame):
    """Raise a KeyboardInterrupt to stop the worker gracefully."""
    signame = signal.Signals(signum).name
    raise KeyboardInterrupt(f"Received {signame} ({signum}), shutting down…")


class Command(BaseCommand):
    """Run task worker for all tasks with the `cron` decorator."""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-b",
            "--backends",
            nargs="+",
            default="default",
            help="Alias of the tasks backend to use.",
        )
        parser.add_argument(
            "-q",
            "--queues",
            nargs="+",
            default="default",
            help="Queue names to listen too and process tasks from.",
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
            help="Number of threads to use. Defaults to the number of CPU cores minus one. ",
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
            "--task-timeout",
            type=float,
            default=3600.0,
            help="Kill hung tasks after timeout seconds. Defaults to one hour.",
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
        backends,
        queues,
        workers,
        threads,
        max_tasks,
        max_tasks_jitter,
        task_timeout,
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
        backend_alias = backends[0] if isinstance(backends, list) else backends
        backend = task_backends[backend_alias]
        if not set(queues).issubset(backend.queues):
            self.stderr.write(
                self.style.ERROR("Backend does not support all specified queues.")
            )
            exit(1)
        exe = TaskExecutor(
            backend=backend,
            workers=workers,
            threads=threads,
            max_tasks=max_tasks,
            max_tasks_jitter=max_tasks_jitter,
            task_timeout=datetime.timedelta(seconds=task_timeout),
            exit_empty=exit_empty,
            queues=queues,
        )
        try:
            exe.run()
        except KeyboardInterrupt as e:
            self.stdout.write(self.style.WARNING(str(e)))
            self.stdout.write(self.style.NOTICE("Shutting down workers…"))
            exe.shutdown()
