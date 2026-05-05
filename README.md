# Threadmill

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/codingjoe/threadmill/raw/main/images/logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/codingjoe/threadmill/raw/main/images/logo-light.svg">
    <img alt="Django Grinder: A queue agnostic worker for Django's task framework." src="https://github.com/codingjoe/threadmill/raw/main/images/logo-light.svg">
  </picture>
<br>
  <a href="https://github.com/codingjoe/threadmill/">Documentation</a> |
  <a href="https://github.com/codingjoe/threadmill/issues/new/choose">Issues</a> |
  <a href="https://github.com/codingjoe/threadmill/releases">Changelog</a> |
  <a href="https://github.com/sponsors/codingjoe">Funding</a> 💚
</p>

**A queue agnostic worker for Django's task framework.**

## Design Principles

- **Durability** – We recover from any failures, even poorly written tasks.
- **Consistency** – We never lose data, even if someone unplugs the power or network.
- **Utilization** – We keep the CPU saturated with tasks, not with idle time or waiting for locks.

[![PyPi Version](https://img.shields.io/pypi/v/threadmill.svg)](https://pypi.python.org/pypi/threadmill/)
[![Test Coverage](https://codecov.io/gh/codingjoe/threadmill/branch/main/graph/badge.svg)](https://codecov.io/gh/codingjoe/threadmill)
[![GitHub License](https://img.shields.io/github/license/codingjoe/threadmill)](https://raw.githubusercontent.com/codingjoe/threadmill/master/LICENSE)

## Setup

You need to have [Django's Task framework][django-tasks] setup properly.

```console
uv add threadmill
```

Add `threadmill` to your `INSTALLED_APPS` in `settings.py`:

```python
# settings.py
INSTALLED_APPS = [
    "threadmill",
    # ...
]
```

Finally, you launch the worker pool:

```console
uv run manage.py threadmill
```

## Usage

The workers are inspired by Gunicorn, and the CLI is very similar.

### Utilization

Depending on your workload, you can tweak the number of processes and threads.
Processes allow for parallel compute (no GIL) while threads are great for low-memory concurrent IO.

```console
uv run manage.py threadmill --processes 4 --threads 2
```

### Health

If your tasks leak memory, you can recycle (restart) the workers after a certain number of tasks have been processed:

```console
uv run manage.py threadmill --max-tasks 1000 --max-tasks-jitter 100
```

This will restart the workers after 1000 tasks have been processed, with a random jitter of up to 100 tasks to avoid all workers restarting at the same time.

Should a worker crash or be killed, the pool will automatically restart it.

### Shutdown

A graceful shutdown is possible with the `SIGTERM` or a keyboard interrupt.
All workers will finish the tasks they acquired and publish them.

You can use `--exit-empty` to exit immediately after all tasks have been processed,
which might be useful for draining a one-off queue.

### Task Backlog

You can prefetch tasks from a queue to avoid IO latency bottlenecks.
However, this will increase the memory usage of the worker pool.

```console
uv run manage.py threadmill --prefetch 100
```

### Task Timeouts

Task timeouts are important to ensure the long-term health of your pool.
However, they need to be aligned with your queueing system's timeout settings.
The message queue needs to requeue a task that hasn't been acknowledged within the timeout.

## Integration

> [!NOTE]
> This section is for people who want to integrate Threadmill into their queueing system.

Threadmill is designed to be durable and requires a queueing system to support late acknowledgement.

To use Threadmill, your backend will need to inherit from `threadmill.backends.AcknowledgeableTaskBackend` and implement the following methods:

```python
class AcknowledgeableTaskBackend(BaseTaskBackend, ABC):
    """Provide an interface for tasks queues to be processed by the executor."""

    def acquire(
        self, *queue_names: str, timeout: datetime.timedelta | None = None
    ) -> TaskResult:
        """
        Return and lock the next task to be processed without removing it from the queue.

        Args:
            queue_names: The names of the queues to acquire tasks from.
            timeout: The maximum time to wait for a task. If None, wait indefinitely.

        Raises:
            TimeoutError: If no task is available within the specified timeout.
        """
        raise NotImplementedError

    def acknowledge(self, task_result: TaskResult) -> None:
        """Remove the task from the queue and publish the result."""
        raise NotImplementedError
```
