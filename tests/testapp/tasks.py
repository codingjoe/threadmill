import asyncio
import logging
import random
import uuid

from django.tasks import task

logger = logging.getLogger(__name__)


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
    await asyncio.sleep(random.uniform(0.1, 0.5))


leak = {}


@task(queue_name="memory")
def memory_workload():
    """Allocate and leak 100MB of memory."""
    leak[uuid.uuid4()] = "x" * 1024 * 1024 * 100
