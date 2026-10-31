import argparse
import datetime
import logging
import signal
import sys

from django.core.management import BaseCommand, CommandError
from django.core.management.base import BaseCommand as DjangoBaseCommand
from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    InvalidTaskBackend,
    task_backends,
)

from ...backends.base import ThreadmillTaskBackend
from ...exceptions import MissingExtraError
from ...executor import JsonFormatter, TaskExecutor


def kill_softly(signum, frame):
    """Raise a KeyboardInterrupt to stop the worker gracefully."""
    signame = signal.Signals(signum).name
    message = f"Received {signame} ({signum}), shutting down…"
    raise KeyboardInterrupt(message)


class ThreadmillCommand(DjangoBaseCommand):
    feature = ""

    def add_arguments(self, parser):
        parser.add_argument(
            "-b",
            "--backend",
            type=self.parse_backend,
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Alias of the tasks backend.",
        )

    def parse_backend(self, alias: str) -> ThreadmillTaskBackend:
        try:
            backend = task_backends[alias]
        except InvalidTaskBackend as e:
            message = f"Invalid backend: {alias!r}"
            raise argparse.ArgumentTypeError(message) from e
        if not isinstance(backend, ThreadmillTaskBackend):
            message = f"Backend {backend.alias!r} does not support {self.feature}."
            raise argparse.ArgumentTypeError(message)
        return backend

    @staticmethod
    def parse_duration(value: str) -> datetime.timedelta:
        return datetime.timedelta(seconds=float(value))

    def install_signal_handlers(self) -> None:
        match sys.platform:
            case "win32":
                signal.signal(signal.SIGBREAK, kill_softly)
            case _:
                signal.signal(signal.SIGHUP, kill_softly)
        signal.signal(signal.SIGTERM, kill_softly)
        signal.signal(signal.SIGINT, kill_softly)


class WorkerCommand(ThreadmillCommand):
    """Run task workers to process enqueued tasks from the specified backends and queues."""

    feature = "task processing"
    help = __doc__

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            "-q",
            "--queues",
            nargs="+",
            default=[DEFAULT_TASK_QUEUE_NAME],
            help="Queue names to listen to and process tasks from (round-robin).",
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
            "--prefetch-count",
            type=int,
            default=None,
            help=(
                "Number of tasks to prefetch per worker process."
                " Defaults to 4 × the number of threads. Use 1 to disable batching."
            ),
        )
        parser.add_argument(
            "--poll-interval",
            type=self.parse_duration,
            default=datetime.timedelta(seconds=0.01),
            help="Base wait between idle acquire attempts in seconds.",
        )
        parser.add_argument(
            "--poll-max-interval",
            type=self.parse_duration,
            default=datetime.timedelta(seconds=1),
            help="Maximum wait between idle acquire attempts in seconds.",
        )
        parser.add_argument(
            "--exit-empty",
            action="store_true",
            help="Drain the task queue and exit with 0.",
        )
        parser.add_argument(
            "--log-format",
            help=(
                "Logging format string for all log records of the worker process,"
                " e.g. '%%(levelname)s %%(message)s'. Defaults to JSON."
            ),
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
        prefetch_count,
        poll_interval,
        poll_max_interval,
        exit_empty,
        log_format,
        **options,
    ):
        self.install_signal_handlers()
        self.stdout.write(self.style.SUCCESS("Starting workers…"))
        if _non_queues := set(queues) - set(backend.queues):
            message = f"Backend does not support all specified queues: {_non_queues!r}"
            raise CommandError(message)
        try:
            log_formatter = (
                JsonFormatter() if log_format is None else logging.Formatter(log_format)
            )
            log_formatter.format(
                logging.LogRecord(
                    "threadmill", logging.INFO, __file__, 1, "Ready", (), None
                )
            )
        except (TypeError, ValueError) as e:
            message = f"Invalid log format: {log_format!r}"
            raise CommandError(message) from e
        if prefetch_count is not None and prefetch_count < 0:
            message = f"Invalid prefetch count: {prefetch_count!r}"
            raise CommandError(message)
        exe = TaskExecutor(
            backend=backend,
            workers=workers,
            threads=threads,
            max_tasks=max_tasks,
            max_tasks_jitter=max_tasks_jitter,
            prefetch_count=prefetch_count,
            poll_interval=poll_interval,
            poll_max_interval=poll_max_interval,
            exit_empty=exit_empty,
            queues=queues,
            log_formatter=log_formatter,
        )
        try:
            exe.run()
        except KeyboardInterrupt as e:
            self.stdout.write(self.style.WARNING(str(e)))
            self.stdout.write(self.style.NOTICE("Shutting down workers…"))
            exe.shutdown()


class InspectorCommand(ThreadmillCommand):
    """Launch the textual TUI inspector for task backends and queues."""

    feature = "inspection"
    help = __doc__

    def handle(self, *, backend, **options):
        try:
            from ...inspector.app import InspectorApp
        except ImportError as e:
            raise MissingExtraError("inspector") from e
        InspectorApp(backend=backend).run()


class Command(BaseCommand):
    """Dispatcher for the worker and inspector subcommands."""

    help = "Run threadmill workers or inspect queues."

    subcommands = {
        "worker": WorkerCommand,
        "inspector": InspectorCommand,
    }

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", required=True)
        for name, command_class in self.subcommands.items():
            subparser = subparsers.add_parser(name, help=command_class.help)
            command_class().add_arguments(subparser)

    def handle(self, *args, **options):
        subcommand_name = options.pop("subcommand")
        try:
            self.subcommands[subcommand_name]().execute(*args, **options)
        except MissingExtraError as e:
            raise CommandError(str(e)) from e
