"""The huey app the comparison benchmarks hand tasks to.

The huey consumer CLI imports this module without setting up Django, so it must
not import Django or any Django application. The Redis storage keeps task
results in the ``huey.results.<name>`` hash, the way the Celery app's result
backend keeps them in Redis. The benchmark cleanup's ``huey.*`` pattern deletes
the queue, the results, the schedule and the counters.
"""

import os

import redis
from huey import RedisHuey

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PROCESSED_KEY = "benchmark:processed"
"""Key the sentinel task increments once every earlier task was processed."""

client = redis.Redis.from_url(REDIS_URL)

huey_app = RedisHuey("threadmill_benchmark", results=True, url=REDIS_URL)


@huey_app.task()
def huey_echo(value):
    """Return the given value."""
    return value


@huey_app.task()
def huey_mark_processed():
    """Record that every earlier task in the queue has been processed."""
    client.incr(PROCESSED_KEY)
