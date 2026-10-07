"""The shared contract every framework in the parallelism benchmark follows.

The Celery and dramatiq worker CLIs import their applications without setting up
Django, so this module must not import Django or any Django application. The work
matches ``tests.testapp.tasks.compute_workload``, which is the equivalent Django
task the threadmill arm of the benchmark runs.
"""

PRIME_TARGET = 100_000
"""How many primes to count, about one second of work on a modern core."""

COMPLETION_KEY = "benchmark:cpu_done"
"""Key each CPU task increments when it finishes.

A sentinel task cannot prove a drain on a pool with more than one thread: a free
thread can take the sentinel before its predecessors finish. Counting completions
proves it whatever the completion order.
"""

CPU_QUEUE = "cpu_benchmark"
"""Queue carrying only the CPU workload.

Defined once here because both framework applications must publish to it and the
benchmark must listen on it. A queue of its own keeps the measurement honest: a
worker process left behind by another test or worktree sharing the same Redis
instance listens on the framework's default queue and would otherwise steal tasks
and report a drain that never did the work.
"""


def count_primes(target: int = PRIME_TARGET) -> int:
    """Count the first ``target`` primes and return the count."""

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
    while prime_count < target:
        if is_prime(number):
            prime_count += 1
        number += 1
    return prime_count
