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

| Option            | Default                | Description                                                  |
| ----------------- | ---------------------- | ------------------------------------------------------------ |
| `lease_ttl`       | `timedelta(hours=1)`   | Max processing time before a started task is marked FAILED.  |
| `result_ttl`      | `timedelta(days=1)`    | How long task results are retained before automatic removal. |
| `broker_interval` | `timedelta(seconds=1)` | Interval between background broker maintenance passes.       |
| `batch_size`      | `100`                  | Max tasks to move or requeue per broker pass.                |

A task that is started but never acknowledged (lease expired) is marked FAILED
with an `AcknowledgementTimeout` error. Set `lease_ttl` comfortably above your
worst-case task runtime.

All keys for one backend alias share a Redis Cluster hash tag (`{alias}`), so
every multi-key operation — including the cross-queue acquire — runs on a single
shard. Scale horizontally by running additional backend aliases, not by relying
on cross-slot operations.

## Sponsors

[![Sponsors](https://django.the-box.sh/sponsors/codingjoe/threadmill.svg)](https://github.com/sponsors/codingjoe)

[django-tasks]: https://docs.djangoproject.com/en/stable/topics/tasks/
