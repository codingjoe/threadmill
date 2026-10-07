"""The dramatiq broker the comparison benchmarks hand tasks to.

The dramatiq worker CLI imports this module without setting up Django, so it
must not import Django or any Django application. The broker carries the Results
middleware so the echo actor stores its return value in Redis, the way the Celery
app's result backend does. The result backend names its keys
``dramatiq:results:<queue>:<actor>:<message_id>`` rather than the default bare MD5
hash, so the benchmark cleanup's ``dramatiq:*`` pattern deletes them.
"""

import os

import dramatiq
import redis
from dramatiq.brokers.redis import RedisBroker
from dramatiq.results import Results
from dramatiq.results.backends.redis import RedisBackend

from benchmarks import cpu_work

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PROCESSED_KEY = "benchmark:processed"
"""Key the sentinel task increments once every earlier task was processed."""

client = redis.Redis.from_url(REDIS_URL)

redis_broker = RedisBroker(url=REDIS_URL)
# A greppable namespace the benchmark cleanup's "dramatiq:*" pattern matches;
# the backend's default key is a bare MD5 hash no pattern can name.
redis_broker.add_middleware(
    Results(
        backend=RedisBackend(
            client=client,
            namespace="dramatiq:results",
            use_namespace_prefix_keys=True,
        ),
    ),
)


@dramatiq.actor(broker=redis_broker, store_results=True)
def dramatiq_echo(value):
    """Return the given value."""
    return value


@dramatiq.actor(broker=redis_broker)
def dramatiq_mark_processed():
    """Record that every earlier task in the queue has been processed."""
    client.incr(PROCESSED_KEY)


@dramatiq.actor(broker=redis_broker, queue_name=cpu_work.CPU_QUEUE)
def dramatiq_compute(value):
    """Consume CPU, then record that this task finished."""
    cpu_work.count_primes()
    client.incr(cpu_work.COMPLETION_KEY)
