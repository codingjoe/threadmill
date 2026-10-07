import asyncio
import datetime
import logging
import random
import uuid

from django.tasks import task
from django.tasks.base import TaskContext

from threadmill.exceptions import AcknowledgementTimeout

logger = logging.getLogger(__name__)


@task()
def echo(value):
    """Return the given value (fast, deterministic, for tests)."""
    return value


@task()
def boom():
    """Raise ValueError (deterministic failure, for tests)."""
    raise ValueError("boom")


@task()
def log_message(message):
    """Log the given message at INFO level (tests task log routing)."""
    logger.info(message)
    return message


@task()
def record_execution(path, value):
    """Append one line per execution so tests can detect duplicate processing."""
    with open(path, "a", encoding="utf-8") as recorded:
        recorded.write(f"{value}\n")


@task()
def count_users():
    """Count all users in the database (tests model access in workers)."""
    from django.contrib.auth.models import User  # noqa


@task(queue_name="compute")
def compute_workload():
    """Calculate the first 1000 prime numbers."""

    def is_prime(number: int) -> bool:
        if number < 2:
            return False
        if number in (2, 3):
            return True
        if number % 2 == 0:
            return False
        for divisor in range(3, int(number**0.5) + 1, 2):
            if number % divisor == 0:
                return False
        return True

    prime_count = 0
    number = 2
    while prime_count < 100_000:
        if is_prime(number):
            prime_count += 1
        number += 1
    return prime_count


@task(queue_name="io")
async def io_workload():
    """Sleep for a random amount of time."""
    await asyncio.sleep(random.uniform(0.1, 0.5))  # noqa: S311


leak = {}


@task(queue_name="memory")
def memory_workload():
    """Allocate and leak 100MB of memory."""
    leak[uuid.uuid7()] = "x" * 1024 * 1024 * 100


@task()
def random_crash():
    """Raise a random exception."""
    if random.random() < 0.75:  # noqa: S311
        exit(1)


def retry_always(context: TaskContext) -> datetime.timedelta:
    """Retry with a fixed 1-second delay."""
    return datetime.timedelta(seconds=1)


def retry_never(context: TaskContext) -> datetime.timedelta | None:
    """Never retry — always return None."""
    return None


def retry_raise(context: TaskContext) -> datetime.timedelta | None:
    """Raise an exception to test retry callback error handling."""
    raise RuntimeError("retry callback crashed")


def retry_on_lease_expiry(context: TaskContext) -> datetime.timedelta | None:
    """Retry with a fixed 1-second delay only when the lease expired."""
    if context.task_result.errors[-1].exception_class is AcknowledgementTimeout:
        return datetime.timedelta(seconds=1)


@task(retry=retry_always)
def boom_with_retry():
    """Raise ValueError, but always schedule a retry."""
    raise ValueError("boom")


@task(retry=retry_never)
def boom_no_retry():
    """Raise ValueError, retry callback returns None."""
    raise ValueError("boom")


@task(retry=retry_raise)
def boom_retry_raises():
    """Raise ValueError, retry callback itself raises."""
    raise ValueError("boom")


@task(retry=retry_on_lease_expiry)
def echo_retry_on_lease_expiry(value):
    """Return the given value, retrying only when the lease expired."""
    return value
