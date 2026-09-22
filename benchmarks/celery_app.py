"""The Celery application the comparison benchmarks hand tasks to.

The Celery worker CLI imports this module without setting up Django, so it must
not import Django or any Django application.
"""

import os

import redis
from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

PROCESSED_KEY = "benchmark:processed"
"""Key the sentinel task increments once every earlier task was processed."""

client = redis.Redis.from_url(REDIS_URL)

celery_app = Celery("threadmill-benchmark", broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.update(
    broker_connection_retry_on_startup=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)


@celery_app.task
def celery_echo(value):
    """Return the given value."""
    return value


@celery_app.task
def celery_mark_processed():
    """Record that every earlier task in the queue has been processed."""
    client.incr(PROCESSED_KEY)
