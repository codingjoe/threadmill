"""Reusable callables for retrying tasks."""

import dataclasses
import datetime
import typing

if typing.TYPE_CHECKING:
    from django.tasks import TaskContext


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class ExponentialBackoff:
    """Delay retries exponentially, capped at ``max_delay`` and limited to ``max_retries`` attempts.

    Only retry exceptions listed in ``expected_exceptions``.

    Usage:

        @task(retry=ExponentialBackoff(base_delay=datetime.timedelta(seconds=1), max_delay=datetime.timedelta(minutes=5), factor=2.0, max_retries=5))
        def my_task():
            ...
    """

    base_delay: datetime.timedelta = dataclasses.field(
        default=datetime.timedelta(seconds=1), doc="Base delay in seconds"
    )
    max_delay: datetime.timedelta = dataclasses.field(
        default=datetime.timedelta(minutes=60), doc="Maximum delay in seconds"
    )
    factor: float = dataclasses.field(default=2.0, doc="Exponential factor for backoff")
    max_retries: int = dataclasses.field(default=5, doc="Maximum number of retries")
    expected_exceptions: tuple[type[Exception], ...] = dataclasses.field(
        default=(Exception,), doc="Tuple of exception classes that trigger a retry"
    )

    def __post_init__(self) -> None:
        """Resolve deserialized fields back to their original types."""
        for field_name in ("base_delay", "max_delay"):
            value = getattr(self, field_name)
            if isinstance(value, (int, float)):
                object.__setattr__(self, field_name, datetime.timedelta(seconds=value))
        resolved = []
        for exc in self.expected_exceptions:
            if isinstance(exc, str):
                from django.utils.module_loading import import_string

                exc = import_string(exc)
            resolved.append(exc)
        object.__setattr__(self, "expected_exceptions", tuple(resolved))

    def __call__(self, context: TaskContext) -> datetime.timedelta | None:
        if context.attempt < self.max_retries and issubclass(
            context.task_result.errors[-1].exception_class, self.expected_exceptions
        ):
            delay_secs = min(
                self.base_delay.total_seconds() * self.factor**context.attempt,
                self.max_delay.total_seconds(),
            )
            return datetime.timedelta(seconds=delay_secs)

    def deconstruct(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            (),
            {
                "base_delay": self.base_delay.total_seconds(),
                "max_delay": self.max_delay.total_seconds(),
                "factor": self.factor,
                "max_retries": self.max_retries,
                "expected_exceptions": tuple(
                    f"{exc.__module__}.{exc.__qualname__}"
                    for exc in self.expected_exceptions
                ),
            },
        )
