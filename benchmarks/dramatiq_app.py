"""The dramatiq broker the comparison benchmarks hand tasks to.

The dramatiq worker CLI imports this module without setting up Django, so it
must not import Django or any Django application.
"""

import os

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PROCESSED_KEY = "benchmark:processed"
"""Key the sentinel task increments once every earlier task was processed."""

client = redis.Redis.from_url(REDIS_URL)

# joe: the echo value is discarded, one warning per task; celery stores its
# results in Redis, so add the Results middleware to compare result storage too.
redis_broker = RedisBroker(url=REDIS_URL)


@dramatiq.actor(broker=redis_broker)
def dramatiq_echo(value):
    """Return the given value."""
    return value


@dramatiq.actor(broker=redis_broker)
def dramatiq_mark_processed():
    """Record that every earlier task in the queue has been processed."""
    client.incr(PROCESSED_KEY)
