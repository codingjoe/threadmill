<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/codingjoe/threadmill/raw/main/docs/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/codingjoe/threadmill/raw/main/docs/images/logo-light.svg">
    <img alt="Threadmill: Durable high-performance backend for Django's task framework." src="https://github.com/codingjoe/threadmill/raw/main/docs/images/logo-light.svg">
  </picture>
<br>
  <a href="https://github.com/codingjoe/threadmill/">Documentation</a> |
  <a href="https://github.com/codingjoe/threadmill/issues/new/choose">Issues</a> |
  <a href="https://github.com/codingjoe/threadmill/releases">Changelog</a> |
  <a href="https://github.com/sponsors/codingjoe">Funding</a> 💚
</p>

# Threadmill [![PyPi Version](https://img.shields.io/pypi/v/threadmill.svg)](https://pypi.python.org/pypi/threadmill/) [![Test Coverage](https://codecov.io/gh/codingjoe/threadmill/branch/main/graph/badge.svg)](https://codecov.io/gh/codingjoe/threadmill) [![GitHub License](https://img.shields.io/github/license/codingjoe/threadmill)](https://raw.githubusercontent.com/codingjoe/threadmill/master/LICENSE)

**Durable high-performance backend for Django's task framework.**

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/codingjoe/threadmill/raw/main/docs/images/backend-comparison-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/codingjoe/threadmill/raw/main/docs/images/backend-comparison-light.svg">
    <img alt="Tasks per second with one worker: threadmill 4,989, celery 2,363, django-tasks-db 2,179, django-tasks-redis 1,712." src="https://github.com/codingjoe/threadmill/raw/main/docs/images/backend-comparison-light.svg">
  </picture>
</p>

## Design Principles

- **Durability** – Recover from any failures, even poorly written tasks.
- **Consistency** – Never lose data, even if someone unplugs the power or network.
- **Utilization** – Keep the CPU saturated with tasks, not with idle time or waiting for locks.

## Setup

You need to have [Django's Task framework][django-tasks] set up properly.

```console
uv add threadmill[redis]
```

Add `threadmill` to your `INSTALLED_APPS` in `settings.py`
and configure the task backend:

```python
# settings.py
import os

INSTALLED_APPS = [
    "threadmill",
    # ...
]

TASKS = {
    "default": {
        "BACKEND": "threadmill.backends.redis.RedisTaskBackend",
        "REDIS_URL": os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    },
    # ...
}
```

Optionally, install the inspector dependency if you want the TUI:

```console
uv add threadmill[inspector]
```

Then launch the worker pool:

```console
uv run manage.py threadmill worker
```

## Usage

### Workers

The workers are inspired by Gunicorn, and the CLI is very similar.

#### Utilization

Depending on your workload, you can tweak the number of processes and threads.
Processes allow for parallel compute (no GIL) while threads are great for low-memory concurrent IO.

```console
uv run manage.py threadmill worker --processes 4 --threads 2
```

#### Health

If your tasks leak memory, you can recycle (restart) the workers after a certain number of tasks have been processed:

```console
uv run manage.py threadmill worker --max-tasks 1000 --max-tasks-jitter 100
```

This will restart the workers after 1000 tasks have been processed, with a random jitter of up to 100 tasks to avoid all workers restarting at the same time.

Should a worker crash or be killed, the pool will automatically restart it.

#### Shutdown

A graceful shutdown is possible with the `SIGTERM` or a keyboard interrupt.
All workers will finish the tasks they acquired and acknowledge them.

You can use `--exit-empty` to exit immediately after all tasks have been processed,
which might be useful for draining a one-off queue.

### Inspector

![Inspector TUI screenshot](https://github.com/codingjoe/threadmill/raw/main/docs/images/TUI-screenshot.svg)

The optional TUI inspector lets you watch queues, tasks, and task details in real-time.
Install it with the `inspector` extra and launch it from a separate terminal:

```console
uv add threadmill[inspector]
uv run manage.py threadmill inspector
```

### Redis Backend Options

The `RedisTaskBackend` accepts the following options under `OPTIONS` in your
`TASKS` configuration:

| Option              | Default                   | Description                                                             |
| ------------------- | ------------------------- | ----------------------------------------------------------------------- |
| `lease_ttl`         | `timedelta(hours=1)`      | Max processing time before a started task is marked FAILED.             |
| `result_ttl`        | `timedelta(days=1)`       | How long task results are retained before automatic removal.            |
| `broker_interval`   | `timedelta(seconds=1)`    | Interval between background broker maintenance passes.                  |
| `batch_size`        | `100`                     | Max tasks to move or requeue per broker pass.                           |
| `poll_interval`     | `timedelta(seconds=0.01)` | Base wait between idle acquire attempts, doubled after each empty poll. |
| `poll_max_interval` | `timedelta(seconds=1)`    | Max wait between idle acquire attempts.                                 |

A task that is started but never acknowledged (lease expired) is marked FAILED
with an `AcknowledgementTimeout` error. Set `lease_ttl` comfortably above your
worst-case task runtime.

All keys for one backend alias share a Redis Cluster hash tag (`{alias}`), so
every multi-key operation — including the cross-queue acquire — runs on a single
shard. Scale horizontally by running additional backend aliases, not by relying
on cross-slot operations.

### Retrying failed tasks

Pass a `retry` callback to `@task()` to retry failed tasks with a delay.
The callback receives a `TaskContext` — use `context.attempt` for the current
attempt count and `context.task_result.errors[-1]` for the latest error.
Return a `timedelta` to schedule the next attempt, or `None` to stop retrying.

The worker re-queues the failed task, preserving its ID and error history;
the broker promotes it back to the ready queue once the delay elapses.

#### Built-in `ExponentialBackoff`

`threadmill.retry.ExponentialBackoff` provides a serializable exponential
backoff strategy out of the box. It caps the delay at `max_delay`, stops
after `max_retries` attempts, and only retries exceptions listed in
`expected_exceptions`.

```python
import datetime

from django.tasks import task
from requests import HTTPError

from threadmill.retry import ExponentialBackoff


@task(
    retry=ExponentialBackoff(
        base_delay=datetime.timedelta(seconds=1),
        max_delay=datetime.timedelta(minutes=5),
        factor=2.0,
        max_retries=5,
        expected_exceptions=(HTTPError,),
    )
)
def fetch_github_api(url: str): ...
```

#### Custom retry callbacks

For cases that need logic beyond what `ExponentialBackoff` supports,
write a callable that accepts a `TaskContext` and returns a `timedelta`
or `None`. Use `TaskError.exception_class` to filter by exception type:

```python
import datetime

from django.tasks import task
from django.tasks.base import TaskContext
from requests import HTTPError


def retry_on_rate_limit(context: TaskContext) -> datetime.timedelta | None:
    """Retry HTTP 429 responses with exponential backoff, up to 5 attempts."""
    if context.attempt >= 5:
        return None
    error = context.task_result.errors[-1]
    if not issubclass(error.exception_class, HTTPError):
        return None
    return min(
        datetime.timedelta(seconds=2**context.attempt),
        datetime.timedelta(seconds=60),
    )


@task(retry=retry_on_rate_limit)
def fetch_github_api(url: str): ...
```

## Sponsors

[![Sponsors](https://django.the-box.sh/sponsors/codingjoe/threadmill.svg)](https://github.com/sponsors/codingjoe)

[django-tasks]: https://docs.djangoproject.com/en/stable/topics/tasks/
