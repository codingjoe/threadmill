"""Redis-backed durable priority queue backend for Django's task framework."""

from __future__ import annotations

import datetime
import logging
import queue
import time
import uuid
from collections.abc import Generator, Sequence
from pathlib import Path

import redis
from django.tasks import DEFAULT_TASK_QUEUE_NAME, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone

from threadmill.backends.base import (
    Broker,
    QueueStats,
    QueueTelemetry,
    ThreadmillTaskBackend,
)

logger = logging.getLogger(__name__)

_LUA_DIR = Path(__file__).resolve().parent / "lua"


def _load_lua(name: str) -> str:
    """Load a Lua script from the lua directory."""
    return (_LUA_DIR / f"{name}.lua").read_text()


class RedisBroker(Broker):
    """Background maintenance broker for the Redis backend."""

    backend: RedisTaskBackend

    MOVER_SCRIPT = _load_lua("mover")
    """Move tasks whose scheduled time has passed from the deferred to the active queue."""
    REAPER_SCRIPT = _load_lua("reaper")
    """Fail tasks whose processing lease has expired from the running set."""

    def __init__(self, backend: RedisTaskBackend) -> None:
        interval = backend.options.get("broker_interval", datetime.timedelta(seconds=1))
        super().__init__(backend, interval=interval)
        self._mover_script = self.backend.client.register_script(self.MOVER_SCRIPT)
        self._reaper_script = self.backend.client.register_script(self.REAPER_SCRIPT)

    def _move_queue(self, queue_name: str) -> None:
        """Move due deferred tasks from a single deferred set."""
        deferred_key = self.backend.DEFERRED_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        queue_key = self.backend.QUEUE_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        self._mover_script(
            keys=[deferred_key, queue_key],
            args=[
                str(time.time() * 1000),
                self.backend.key_prefix + ":task:",
                str(self.backend.batch_size),
            ],
        )

    def _reap_running_queue(self, queue_name: str) -> None:
        """Fail tasks whose processing lease has expired from the running set."""
        now = timezone.now()
        now_ms = now.timestamp() * 1000
        finished_at_iso = now.isoformat()
        running_key = self.backend.RUNNING_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        results_key = self.backend.RESULTS_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        egress_key = self.backend.EGRESS_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        failed_key = self.backend.FAILED_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        self._reaper_script(
            keys=[running_key, results_key, egress_key, failed_key],
            args=[
                str(now_ms),
                self.backend.key_prefix + ":task:",
                self.backend.key_prefix + ":result:",
                str(self.backend.batch_size),
                str(int(self.backend.result_ttl.total_seconds())),
                finished_at_iso,
            ],
        )

    def main(self) -> None:
        """Run mover and running reaper passes for all queues."""
        now_ms = time.time() * 1000
        for queue_name in self.backend.queues:
            try:
                self._move_queue(queue_name)
            except Exception:  # noqa: BLE001
                logger.exception("Mover error for queue %r", queue_name)

            try:
                self._reap_running_queue(queue_name)
            except Exception:  # noqa: BLE001
                logger.exception("Running reaper error for queue %r", queue_name)

            try:
                self.backend._trim_telemetry(queue_name, now_ms=now_ms)
            except Exception:  # noqa: BLE001
                logger.exception("Telemetry trim error for queue %r", queue_name)


class RedisTaskBackend(ThreadmillTaskBackend):
    """Redis-backed durable priority queue backend.

    Uses sorted sets for priority ordering, a running set for in-flight
    tracking, and a deferred set for scheduled tasks. All multi-step operations
    are atomic via Lua scripts.
    """

    supports_async_task = True
    supports_get_result = True
    supports_priority = True
    supports_defer = True

    broker_class = RedisBroker

    QUEUE_KEY = "{prefix}:queue:{queue_name}"
    RUNNING_KEY = "{prefix}:running:{queue_name}"
    DEFERRED_KEY = "{prefix}:deferred:{queue_name}"
    TASK_KEY = "{prefix}:task:{task_id}"
    RESULT_KEY = "{prefix}:result:{result_id}"
    RESULTS_KEY = "{prefix}:results:{queue_name}"
    INGRESS_KEY = "{prefix}:ingress:{queue_name}:min"
    EGRESS_KEY = "{prefix}:egress:{queue_name}:min"
    SUCCESSFUL_KEY = "{prefix}:successful:{queue_name}"
    FAILED_KEY = "{prefix}:failed:{queue_name}"

    ACQUIRE_SCRIPT = _load_lua("acquire")
    """Pop the next task from a priority queue and move it directly to the running set."""
    ACKNOWLEDGE_SCRIPT = _load_lua("acknowledge")
    """Remove from running, persist the result, and clean up."""

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias=alias, params=params)

        try:
            redis_url = params["REDIS_URL"]
        except KeyError as e:
            raise ValueError(
                f"REDIS_URL must be specified in your settings for the {type(self).__name__}."
            ) from e
        self.client = redis.from_url(redis_url)
        self.key_prefix = f"threadmill:{{{alias}}}"
        self.lease_ttl = self.options.get("lease_ttl", datetime.timedelta(hours=1))
        self.result_ttl = self.options.get("result_ttl", datetime.timedelta(days=1))
        self.batch_size = self.options.get("batch_size", 100)
        self.telemetry_ttl = self.options.get(
            "telemetry_ttl", datetime.timedelta(seconds=60)
        )
        self._acquire_script = self.client.register_script(self.ACQUIRE_SCRIPT)
        self._acknowledge_script = self.client.register_script(self.ACKNOWLEDGE_SCRIPT)

    def _compute_score(self, priority: int, enqueued_at: datetime.datetime) -> float:
        """Compute a ZSET score for priority-ordered FIFO queueing.

        Higher priority (more positive) tasks are popped first. Within the same
        priority, earlier enqueued tasks are popped first.
        """
        enqueued_at_ms = enqueued_at.timestamp() * 1e3
        return -priority * 1e13 + enqueued_at_ms

    def enqueue(
        self,
        task,
        args: Sequence | None = None,
        kwargs: dict | None = None,
    ) -> TaskResult:
        """Enqueue a task for execution.

        If the task has a run_after datetime, it is stored in the deferred set
        instead of the active priority queue.
        """
        self.validate_task(task)

        enqueued_at = timezone.now()
        task_result = TaskResult(
            task=task,
            id=str(uuid.uuid4()),
            status=TaskResultStatus.READY,
            enqueued_at=enqueued_at,
            started_at=None,
            finished_at=None,
            last_attempted_at=None,
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        score = self._compute_score(task.priority, enqueued_at)
        enqueued_at_ms = enqueued_at.timestamp() * 1000
        serialized = self.serialize_task_result(task_result)
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_result.id)
        task_data_ttl = int(
            self.lease_ttl.total_seconds() * 3 + self.result_ttl.total_seconds()
        )
        ingress_key = self.INGRESS_KEY.format(
            prefix=self.key_prefix, queue_name=task.queue_name
        )

        pipe = self.client.pipeline()
        pipe.hset(
            task_key,
            mapping={
                "data": serialized,
                "score": str(score),
                "queue_name": task.queue_name,
            },
        )
        pipe.expire(task_key, task_data_ttl)
        pipe.zadd(ingress_key, {task_result.id: enqueued_at_ms})

        if task.run_after is not None:
            deferred_key = self.DEFERRED_KEY.format(
                prefix=self.key_prefix, queue_name=task.queue_name
            )
            run_after_ms = task.run_after.timestamp() * 1000
            pipe.zadd(deferred_key, {task_result.id: run_after_ms})
        else:
            queue_key = self.QUEUE_KEY.format(
                prefix=self.key_prefix, queue_name=task.queue_name
            )
            pipe.zadd(queue_key, {task_result.id: score})

        pipe.execute()

        task_enqueued.send(self.__class__, task_result=task_result)
        return task_result

    def acquire(
        self,
        *queue_names: str,
        timeout: datetime.timedelta | None = None,
        worker: str = "",
    ) -> TaskResult:
        queue_names = queue_names or tuple(self.queues)
        deadline = time.monotonic() + timeout.total_seconds() if timeout else None
        keys = [
            key
            for queue_name in queue_names
            for key in (
                self.RUNNING_KEY.format(prefix=self.key_prefix, queue_name=queue_name),
                self.QUEUE_KEY.format(prefix=self.key_prefix, queue_name=queue_name),
            )
        ]

        while True:
            now = timezone.now()
            now_ms = now.timestamp() * 1000
            now_iso = now.isoformat()

            if data := self._acquire_script(
                keys=keys,
                args=[
                    str(now_ms),
                    now_iso,
                    self.key_prefix + ":task:",
                    str(len(queue_names)),
                    worker,
                    str(int(self.lease_ttl.total_seconds() * 1000)),
                ],
            ):
                return self.deserialize_task_result(data)

            try:
                if deadline - time.monotonic() <= 0:
                    raise TimeoutError(
                        "No task available within the specified timeout."
                    )
            except TypeError:
                raise queue.Empty("No task available.")
            else:
                time.sleep(0.01)

    def acknowledge(self, task_result: TaskResult) -> None:
        serialized = self.serialize_task_result(task_result)
        running_key = self.RUNNING_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        result_key = self.RESULT_KEY.format(
            prefix=self.key_prefix, result_id=task_result.id
        )
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_result.id)
        results_key = self.RESULTS_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        egress_key = self.EGRESS_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        successful_key = self.SUCCESSFUL_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        failed_key = self.FAILED_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        finished_at = task_result.finished_at or timezone.now()
        finish_score = finished_at.timestamp() * 1000

        self._acknowledge_script(
            keys=[
                running_key,
                result_key,
                task_key,
                results_key,
                egress_key,
                successful_key,
                failed_key,
            ],
            args=[
                task_result.id,
                serialized,
                str(int(self.result_ttl.total_seconds())),
                str(finish_score),
                task_result.status.name,
            ],
        )

    def peek(
        self,
        queue_name: str = DEFAULT_TASK_QUEUE_NAME,
        *,
        status: TaskResultStatus | None = None,
        count: int = 1,
    ) -> Generator[TaskResult]:
        match status:
            case TaskResultStatus.READY:
                yield from self._peek_zset(self.QUEUE_KEY, queue_name, count)
            case TaskResultStatus.RUNNING:
                yield from self._peek_zset(self.RUNNING_KEY, queue_name, count)
            case TaskResultStatus.SUCCESSFUL | TaskResultStatus.FAILED:
                yield from self._peek_results(queue_name, count, status)
            case None:
                yield from self._peek_zset(self.QUEUE_KEY, queue_name, count)
                yield from self._peek_zset(self.RUNNING_KEY, queue_name, count)
                yield from self._peek_results(queue_name, count, None)

    def _peek_zset(
        self, key_template: str, queue_name: str, count: int
    ) -> Generator[TaskResult]:
        """Yield tasks stored in the task hash for the given sorted-set key."""
        key = key_template.format(prefix=self.key_prefix, queue_name=queue_name)
        task_ids = [
            tid.decode() if isinstance(tid, bytes) else tid
            for tid in self.client.zrange(key, 0, count - 1)
        ]
        if not task_ids:
            return
        pipe = self.client.pipeline()
        for task_id in task_ids:
            pipe.hget(
                self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id), "data"
            )
        for data in pipe.execute():
            if not data:
                continue
            yield self.deserialize_task_result(
                data.decode() if isinstance(data, bytes) else data
            )

    def _peek_results(
        self, queue_name: str, count: int, status: TaskResultStatus | None
    ) -> Generator[TaskResult]:
        """Yield acknowledged results, optionally filtered by status."""
        key = self.RESULTS_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
        result_ids = [
            rid.decode() if isinstance(rid, bytes) else rid
            for rid in self.client.zrange(key, 0, count - 1)
        ]
        if not result_ids:
            return
        pipe = self.client.pipeline()
        for result_id in result_ids:
            pipe.get(
                self.RESULT_KEY.format(prefix=self.key_prefix, result_id=result_id)
            )
        for data in pipe.execute():
            if not data:
                continue
            result = self.deserialize_task_result(
                data.decode() if isinstance(data, bytes) else data
            )
            if status is None or result.status == status:
                yield result

    def get_result(self, result_id: str) -> TaskResult:
        if data := self.client.get(
            self.RESULT_KEY.format(prefix=self.key_prefix, result_id=result_id)
        ):
            return self.deserialize_task_result(data)
        raise TaskResultDoesNotExist(f"Task result {result_id!r} does not exist.")

    def _trim_telemetry(self, queue_name: str, *, now_ms: float) -> None:
        """Drop ingress and egress events older than the telemetry TTL for one queue."""
        cutoff = now_ms - self.telemetry_ttl.total_seconds() * 1000
        pipe = self.client.pipeline()
        pipe.zremrangebyscore(
            self.INGRESS_KEY.format(prefix=self.key_prefix, queue_name=queue_name),
            0,
            cutoff,
        )
        pipe.zremrangebyscore(
            self.EGRESS_KEY.format(prefix=self.key_prefix, queue_name=queue_name),
            0,
            cutoff,
        )
        pipe.execute()

    def queue_telemetry(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> QueueTelemetry:
        """Return a snapshot of stats for all configured queues.

        Ingress and egress are rolling counts over ``interval``, so they
        reflect recent traffic rather than a lifetime total.
        """
        now_ms = time.time() * 1000
        window_start_ms = now_ms - interval.total_seconds() * 1000
        exclusive_start = f"({window_start_ms}"
        pipe = self.client.pipeline()
        for queue_name in self.queues:
            pipe.zcard(
                self.QUEUE_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
            )
            pipe.zcard(
                self.RUNNING_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
            )
            pipe.zcard(
                self.DEFERRED_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
            )
            ingress_key = self.INGRESS_KEY.format(
                prefix=self.key_prefix, queue_name=queue_name
            )
            egress_key = self.EGRESS_KEY.format(
                prefix=self.key_prefix, queue_name=queue_name
            )
            pipe.zcount(ingress_key, exclusive_start, now_ms)
            pipe.zremrangebyscore(ingress_key, 0, window_start_ms)
            pipe.zcount(egress_key, exclusive_start, now_ms)
            pipe.zremrangebyscore(egress_key, 0, window_start_ms)
            pipe.get(
                self.SUCCESSFUL_KEY.format(
                    prefix=self.key_prefix, queue_name=queue_name
                )
            )
            pipe.get(
                self.FAILED_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
            )
        results = pipe.execute()
        queues: dict[str, QueueStats] = {}
        for index, queue_name in enumerate(self.queues):
            base = index * 9
            (
                ready,
                running,
                deferred,
                ingress,
                _,
                egress,
                _,
                successful,
                failed,
            ) = (int(c or 0) for c in results[base : base + 9])
            queues[queue_name] = QueueStats(
                ingress=ingress,
                egress=egress,
                ready=ready,
                running=running,
                deferred=deferred,
                successful=successful,
                failed=failed,
            )
        return QueueTelemetry(queues=queues)

    def close(self) -> None:
        """Close the Redis connection."""
        self.client.close()
