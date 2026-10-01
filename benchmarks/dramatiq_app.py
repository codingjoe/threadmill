"""The dramatiq broker the comparison benchmarks hand tasks to.

The dramatiq worker CLI imports this module without setting up Django, so it
must not import Django or any Django application. The broker carries the Results
middleware so the echo actor stores its return value in Redis, the way the Celery
app's result backend does.
"""

import os

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results import Results
from dramatiq.results.backends.redis import RedisBackend

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PROCESSED_KEY = "benchmark:processed"
"""Key the sentinel task increments once every earlier task was processed."""

client = redis.Redis.from_url(REDIS_URL)

redis_broker = RedisBroker(url=REDIS_URL)
redis_broker.add_middleware(Results(backend=RedisBackend(client=client)))


@dramatiq.actor(broker=redis_broker, store_results=True)
def dramatiq_echo(value):
    """Return the given value."""
    return value


@dramatiq.actor(broker=redis_broker)
def dramatiq_mark_processed():
    """Record that every earlier task in the queue has been processed."""
    client.incr(PROCESSED_KEY)
