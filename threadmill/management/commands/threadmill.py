import asyncio
import datetime
import logging
import math
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
from ...executor import JsonFormatter, TaskExecutor
from ...metrics import (
    configure_metrics,
    is_metrics_export_enabled,
    is_metrics_sdk_available,
)
from ...queue_metrics import DEFAULT_QUEUE_SAMPLE_INTERVAL_S, QueueMetricsExporter


def kill_softly(signum, frame):
    """Raise a KeyboardInterrupt to stop the worker gracefully."""
    signame = signal.Signals(signum).name
    message = f"Received {signame} ({signum}), shutting down…"
    raise KeyboardInterrupt(message)


def require_threadmill_backend(alias: str, *, feature: str) -> ThreadmillTaskBackend:
    """Return the configured threadmill backend or raise a command error."""
    try:
        backend = task_backends[alias]
    except InvalidTaskBackend as e:
        message = f"Invalid backend: {alias!r}"
        raise CommandError(message) from e
    if not isinstance(backend, ThreadmillTaskBackend):
        message = f"Backend {backend.alias!r} does not support {feature}."
        raise CommandError(message)
    return backend


def install_signal_handlers() -> None:
    """Stop gracefully on the platform's shutdown signals."""
    match sys.platform:
        case "win32":
            signal.signal(signal.SIGBREAK, kill_softly)
        case _:
            signal.signal(signal.SIGHUP, kill_softly)
    signal.signal(signal.SIGTERM, kill_softly)
    signal.signal(signal.SIGINT, kill_softly)


class WorkerCommand(DjangoBaseCommand):
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
            type=float,
            default=0.01,
            help="Base wait between idle acquire attempts in seconds.",
        )
        parser.add_argument(
            "--poll-max-interval",
            type=float,
            default=1,
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
        install_signal_handlers()
        self.stdout.write(self.style.SUCCESS("Starting workers…"))
        backend = require_threadmill_backend(backend, feature="task processing")
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
            poll_interval=datetime.timedelta(seconds=poll_interval),
            poll_max_interval=datetime.timedelta(seconds=poll_max_interval),
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


class InspectorCommand(DjangoBaseCommand):
    """Launch the textual TUI inspector for task backends and queues."""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-b",
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Alias of the tasks backend to inspect.",
        )

    def handle(self, *, backend, **options):
        try:
            from ...inspector.app import InspectorApp
        except ImportError:
            message = "Optional dependency missing. Install: threadmill[inspector]"
            raise CommandError(message)
        backend = require_threadmill_backend(backend, feature="inspection")
        InspectorApp(backend=backend).run()


class OpenTelemetryCommand(DjangoBaseCommand):
    """Export queue counts, backlog, ingress, and egress over OTLP or the console."""

    help = __doc__

    def add_arguments(self, parser):
        parser.add_argument(
            "-b",
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Alias of the tasks backend to export metrics for.",
        )
        parser.add_argument(
            "--interval",
            type=float,
            default=DEFAULT_QUEUE_SAMPLE_INTERVAL_S,
            help="Queue sampling interval in seconds.",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Sample the queue once, export, and exit.",
        )

    def handle(self, *, backend, interval, once, **options):
        backend = require_threadmill_backend(backend, feature="queue telemetry")
        if not math.isfinite(interval) or interval <= 0:
            message = f"Invalid interval: {interval!r}"
            raise CommandError(message)
        if not is_metrics_sdk_available():
            message = (
                "OpenTelemetry SDK is not installed. Install: threadmill[opentelemetry]"
            )
            raise CommandError(message)
        if not is_metrics_export_enabled():
            message = (
                "Metrics export is disabled. Unset OTEL_SDK_DISABLED and set "
                "OTEL_METRICS_EXPORTER to otlp or console."
            )
            raise CommandError(message)

        try:
            QueueMetricsExporter.require_telemetry_support(backend)
        except NotImplementedError as e:
            raise CommandError(str(e)) from e
        if not once:
            install_signal_handlers()
        metrics = configure_metrics(backend_alias=backend.alias)
        exporter = QueueMetricsExporter(
            backend=backend, metrics=metrics, interval=interval
        )
        broker = None
        try:
            if backend.broker_class is not None and not once:
                broker = backend.broker_class(backend)
                broker.start()
            asyncio.run(exporter.sample_once() if once else exporter.run())
        except KeyboardInterrupt as e:
            self.stdout.write(self.style.WARNING(str(e)))
        finally:
            try:
                if broker is not None:
                    broker.shutdown()
                    if broker.is_alive():
                        broker.join()
            finally:
                metrics.shutdown()


class Command(BaseCommand):
    """Dispatcher for the worker, inspector, and otel subcommands."""

    help = "Run threadmill workers, inspect queues, or export OpenTelemetry metrics."

    subcommands = {
        "worker": WorkerCommand,
        "inspector": InspectorCommand,
        "otel": OpenTelemetryCommand,
    }

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", required=True)
        for name, command_class in self.subcommands.items():
            subparser = subparsers.add_parser(name, help=command_class.help)
            command_class().add_arguments(subparser)

    def handle(self, *args, **options):
        subcommand_name = options.pop("subcommand")
        self.subcommands[subcommand_name]().execute(*args, **options)
