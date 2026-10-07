"""Redis-backed durable priority queue backend for Django's task framework."""

import collections.abc
import dataclasses
import datetime
import logging
import queue
import random
import time
import uuid
from collections.abc import Generator, Sequence
from pathlib import Path

import redis
import redis.asyncio
from django.tasks import DEFAULT_TASK_QUEUE_NAME, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone

from threadmill.backends.base import (
    BackendTelemetry,
    Broker,
    QueueCounts,
    QueueRates,
    QueueStats,
    TelemetryDirection,
    TelemetryEvent,
    ThreadmillTaskBackend,
    ThreadmillTaskResult,
)
from threadmill.exceptions import AcknowledgementTimeout

logger = logging.getLogger(__name__)

_LUA_DIR = Path(__file__).resolve().parent / "lua"


def _load_lua(name: str) -> str:
    """Load a Lua script from the lua directory."""
    return (_LUA_DIR / f"{name}.lua").read_text()


def _parse_lease_issued_at(value: bytes | None) -> datetime.datetime | None:
    """Return the lease issue time stamped beside a task, or None when the hash holds no timestamp."""
    try:
        return datetime.datetime.fromisoformat(value.decode())
    except AttributeError, ValueError:
        return None


class RedisBroker(Broker):
    """Background maintenance broker for the Redis backend."""

    backend: RedisTaskBackend

    MOVER_SCRIPT = _load_lua("mover")
    """Move tasks whose scheduled time has passed from the deferred to the active queue."""
    REAPER_SCRIPT = _load_lua("reaper")
    """Claim tasks whose processing lease has expired for the broker to decide their fate."""

    CLAIM_TTL = datetime.timedelta(seconds=60)
    """How long a reaper claim is held before another broker pass may take it over."""

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
        queue_key = self.backend._segment_key(TaskResultStatus.READY, queue_name)
        self._mover_script(
            keys=[deferred_key, queue_key],
            args=[
                str(time.time() * 1000),
                f"{self.backend.key_prefix}:task:",
                str(self.backend.batch_size),
            ],
        )

    def _reap_running_queue(self, queue_name: str) -> None:
        """Claim tasks whose processing lease has expired from the running set."""
        running_key = self.backend._segment_key(TaskResultStatus.RUNNING, queue_name)
        claimed_ids = self._reaper_script(
            keys=[running_key],
            args=[
                str(int(self.CLAIM_TTL.total_seconds() * 1000)),
                str(self.backend.batch_size),
                f"{self.backend.key_prefix}:task:",
            ],
        )
        for member in claimed_ids:
            task_id = member.decode()
            try:
                self._reap_task(task_id)
            except ImportError:
                logger.exception(
                    "Task %r retry callback is gone from the code base; "
                    "skipping the reap",
                    task_id,
                )

    def _reap_task(self, task_id: str) -> None:
        """Requeue or fail a claimed task."""
        if (task_result := self.backend.get_leased_task(task_id)) is None:
            logger.warning("Claimed task %r has no task data; skipping", task_id)
            return
        now = timezone.now()
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.FAILED,
            finished_at=now,
            errors=[
                *task_result.errors,
                self.backend.create_task_error(
                    AcknowledgementTimeout("Task processing lease expired.")
                ),
            ],
        )
        delay = self.backend.retry_delay(task_result)
        logger.info(
            "Task '%s@%s' lease expired",
            task_result.id,
            task_result.task.module_path,
        )
        if delay is None:
            self.backend.acknowledge(task_result)
        else:
            self.backend.requeue(task_result, now + delay)

    def main(self) -> None:
        """Run mover and running reaper passes for all queues."""
        for queue_name in self.backend.queues:
            try:
                self._move_queue(queue_name)
            except Exception:  # noqa: BLE001
                logger.exception("Mover error for queue %r", queue_name)

            try:
                self._reap_running_queue(queue_name)
            except Exception:  # noqa: BLE001
                logger.exception("Running reaper error for queue %r", queue_name)


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

    TASK_KEY = "{prefix}:task:{task_id}"
    RESULT_KEY = "{prefix}:result:{result_id}"
    SEGMENT_KEY = "{prefix}:{queue_name}:{status}"
    DEFERRED_KEY = "{prefix}:{queue_name}:deferred"

    LEASE_FIELDS = ("lease_worker", "lease_issued_at", "lease_token")

    TELEMETRY_CHANNEL = "{prefix}:telemetry"

    ACQUIRE_SCRIPT = _load_lua("acquire")
    """Pop the next task from a priority queue, lease it to the worker, and move it to the running set."""
    ACKNOWLEDGE_SCRIPT = _load_lua("acknowledge")
    """Remove from running, persist the result, and clean up."""

    def _segment_key(self, status: TaskResultStatus, queue_name: str) -> str:
        return self.SEGMENT_KEY.format(
            prefix=self.key_prefix,
            queue_name=queue_name,
            status=status.value.lower(),
        )

    def get_leased_task(self, task_id: str) -> ThreadmillTaskResult | None:
        """Return a running task as its lease holds it, or None when its hash is gone."""
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id)
        data, lease_worker, lease_issued_at, lease_token = self.client.hmget(
            task_key, "data", *self.LEASE_FIELDS
        )
        if data is None:
            return None
        return self._apply_lease(
            self.deserialize_task_result(data.decode()),
            worker=lease_worker.decode() if lease_worker else None,
            lease_issued_at=_parse_lease_issued_at(lease_issued_at),
            lease_token=lease_token.decode() if lease_token is not None else None,
        )

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias=alias, params=params)

        try:
            redis_url = params["REDIS_URL"]
        except KeyError as e:
            raise ValueError(
                f"REDIS_URL must be specified in your settings for the {type(self).__name__}."
            ) from e
        self.client = redis.from_url(redis_url)
        self._async_client: redis.asyncio.Redis | None = None
        self.redis_url = redis_url
        self.key_prefix = f"threadmill:{{{alias}}}"
        self.telemetry_channel = self.TELEMETRY_CHANNEL.format(prefix=self.key_prefix)
        self.lease_ttl = self.options.get("lease_ttl", datetime.timedelta(hours=1))
        self.result_ttl = self.options.get("result_ttl", datetime.timedelta(days=1))
        self.batch_size = self.options.get("batch_size", 100)
        self.poll_interval = self.options.get(
            "poll_interval", datetime.timedelta(seconds=0.01)
        )
        self.poll_max_interval = self.options.get(
            "poll_max_interval", datetime.timedelta(seconds=1)
        )
        self._miss_count = 0
        self._rotation_offset = random.randrange(len(self.queues))  # noqa: S311
        self._acquire_script = self.client.register_script(self.ACQUIRE_SCRIPT)
        self._acknowledge_script = self.client.register_script(self.ACKNOWLEDGE_SCRIPT)

    @property
    def async_client(self) -> redis.asyncio.Redis:
        """Lazily-created async Redis client, reused across calls."""
        if self._async_client is None:
            self._async_client = redis.asyncio.Redis.from_url(self.redis_url)
        return self._async_client

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
            id=str(uuid.uuid7()),
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
        serialized = self.serialize_task_result(task_result)
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_result.id)
        task_data_ttl = int(
            self.lease_ttl.total_seconds() * 3 + self.result_ttl.total_seconds()
        )

        pipe = self.client.pipeline()
        pipe.hset(
            task_key,
            mapping={
                "data": serialized,
                "score": str(score),
            },
        )
        pipe.expire(task_key, task_data_ttl)
        pipe.publish(self.telemetry_channel, f"ingress:{task.queue_name}")

        if task.run_after is not None:
            deferred_key = self.DEFERRED_KEY.format(
                prefix=self.key_prefix, queue_name=task.queue_name
            )
            run_after_ms = task.run_after.timestamp() * 1000
            pipe.zadd(deferred_key, {task_result.id: run_after_ms})
        else:
            queue_key = self._segment_key(TaskResultStatus.READY, task.queue_name)
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
                self._segment_key(TaskResultStatus.RUNNING, queue_name),
                self._segment_key(TaskResultStatus.READY, queue_name),
            )
        ]

        while True:
            now = timezone.now()
            now_ms = now.timestamp() * 1000
            now_iso = now.isoformat()
            lease_token = str(uuid.uuid7())

            if data := self._acquire_script(
                keys=keys,
                args=[
                    str(now_ms),
                    now_iso,
                    f"{self.key_prefix}:task:",
                    str(len(queue_names)),
                    worker,
                    str(int(self.lease_ttl.total_seconds() * 1000)),
                    str(self._rotation_offset),
                    lease_token,
                ],
            ):
                self._miss_count = 0
                self._rotation_offset = (self._rotation_offset + 1) % len(queue_names)
                return self._apply_lease(
                    self.deserialize_task_result(data.decode()),
                    worker=worker,
                    lease_issued_at=now,
                    lease_token=lease_token,
                )

            try:
                remaining = deadline - time.monotonic()
            except TypeError:
                raise queue.Empty("No task available.")
            if remaining <= 0:
                raise TimeoutError("No task available within the specified timeout.")
            # Stop doubling once the interval reaches poll_max_interval; larger
            # exponents would only overflow the float math.
            cap = int(self.poll_max_interval / self.poll_interval).bit_length()
            interval_secs = min(
                self.poll_interval.total_seconds() * 2 ** min(self._miss_count, cap),
                self.poll_max_interval.total_seconds(),
                remaining,
            )
            self._miss_count += 1
            time.sleep(interval_secs)

    @staticmethod
    def _apply_lease(
        task_result: TaskResult,
        *,
        worker: str | None,
        lease_issued_at: datetime.datetime | None,
        lease_token: str | None,
    ) -> ThreadmillTaskResult:
        """Return a stored task result as a running attempt.

        `worker=None` means the lease records no worker; an empty string still
        counts as an attempt, and a task that already records a start keeps it.
        """
        leased = ThreadmillTaskResult.from_result(task_result, lease_token=lease_token)
        return dataclasses.replace(
            leased,
            status=TaskResultStatus.RUNNING,
            started_at=leased.started_at or lease_issued_at,
            last_attempted_at=lease_issued_at or leased.last_attempted_at,
            worker_ids=(
                [*leased.worker_ids, worker]
                if worker is not None
                else leased.worker_ids
            ),
        )

    def acknowledge(self, task_result: TaskResult) -> None:
        serialized = self.serialize_task_result(task_result)
        running_key = self._segment_key(
            TaskResultStatus.RUNNING, task_result.task.queue_name
        )
        result_key = self.RESULT_KEY.format(
            prefix=self.key_prefix, result_id=task_result.id
        )
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_result.id)
        successful_results_key = self._segment_key(
            TaskResultStatus.SUCCESSFUL, task_result.task.queue_name
        )
        failed_results_key = self._segment_key(
            TaskResultStatus.FAILED, task_result.task.queue_name
        )
        finished_at = task_result.finished_at or timezone.now()
        finish_score = finished_at.timestamp() * 1000
        lease_token = (
            task_result.lease_token
            if isinstance(task_result, ThreadmillTaskResult)
            else None
        )

        self._acknowledge_script(
            keys=[
                running_key,
                result_key,
                task_key,
                successful_results_key,
                failed_results_key,
            ],
            args=[
                task_result.id,
                serialized,
                str(int(self.result_ttl.total_seconds())),
                str(finish_score),
                task_result.status.name,
                self.telemetry_channel,
                task_result.task.queue_name,
                lease_token or "",
            ],
        )

    def requeue(self, task_result: TaskResult, run_after: datetime.datetime) -> None:
        task_result = dataclasses.replace(
            task_result,
            status=TaskResultStatus.READY,
            started_at=None,
            finished_at=None,
        )
        serialized = self.serialize_task_result(task_result)
        running_key = self._segment_key(
            TaskResultStatus.RUNNING, task_result.task.queue_name
        )
        deferred_key = self.DEFERRED_KEY.format(
            prefix=self.key_prefix, queue_name=task_result.task.queue_name
        )
        failed_key = self._segment_key(
            TaskResultStatus.FAILED, task_result.task.queue_name
        )
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_result.id)
        result_key = self.RESULT_KEY.format(
            prefix=self.key_prefix, result_id=task_result.id
        )
        score = self._compute_score(task_result.task.priority, task_result.enqueued_at)
        run_after_ms = run_after.timestamp() * 1000
        task_data_ttl = int(
            self.lease_ttl.total_seconds() * 3 + self.result_ttl.total_seconds()
        )

        pipe = self.client.pipeline()
        pipe.zrem(running_key, task_result.id)
        pipe.zrem(failed_key, task_result.id)
        pipe.delete(result_key)
        pipe.hset(
            task_key,
            mapping={
                "data": serialized,
                "score": str(score),
                **dict.fromkeys(self.LEASE_FIELDS, ""),
            },
        )
        pipe.expire(task_key, task_data_ttl)
        pipe.zadd(deferred_key, {task_result.id: run_after_ms})
        pipe.publish(self.telemetry_channel, f"ingress:{task_result.task.queue_name}")
        pipe.execute()

    def dequeue(self, task_result: TaskResult) -> None:
        self.client.zrem(
            self._segment_key(task_result.status, task_result.task.queue_name),
            task_result.id,
        )

    def purge(self, queue_name: str) -> None:
        pattern = f"{self.key_prefix}:{queue_name}:*"
        pipe = self.client.pipeline()
        for key in self.client.scan_iter(match=pattern):
            pipe.delete(key)
        pipe.execute()

    def peek(
        self,
        queue_name: str = DEFAULT_TASK_QUEUE_NAME,
        *,
        status: TaskResultStatus,
        count: int = 1,
    ) -> Generator[TaskResult]:
        match status:
            case TaskResultStatus.READY | TaskResultStatus.RUNNING:
                yield from self._peek_tasks(
                    self._segment_key(status, queue_name),
                    count,
                    leased=status is TaskResultStatus.RUNNING,
                )
            case TaskResultStatus.SUCCESSFUL | TaskResultStatus.FAILED:
                yield from self._peek_results(
                    self._segment_key(status, queue_name), count
                )

    def _peek_tasks(
        self, zset_key: str, count: int, *, leased: bool
    ) -> Generator[TaskResult]:
        """Yield up to `count` stored tasks in queue order.

        Leased tasks are yielded as their lease reports them.
        """
        pipe = self.client.pipeline()
        for member in self.client.zrange(zset_key, 0, count - 1):
            task_key = self.TASK_KEY.format(
                prefix=self.key_prefix, task_id=member.decode()
            )
            if leased:
                pipe.hmget(task_key, "data", *self.LEASE_FIELDS)
            else:
                pipe.hget(task_key, "data")
        for stored in pipe.execute():
            if not stored:
                continue
            if leased:
                data, lease_worker, lease_issued_at, lease_token = stored
                if not data:
                    continue
                yield self._apply_lease(
                    self.deserialize_task_result(data.decode()),
                    worker=lease_worker.decode() if lease_worker else None,
                    lease_issued_at=_parse_lease_issued_at(lease_issued_at),
                    lease_token=(
                        lease_token.decode() if lease_token is not None else None
                    ),
                )
            else:
                yield self.deserialize_task_result(stored.decode())

    def _peek_results(self, zset_key: str, count: int) -> Generator[TaskResult]:
        """Yield up to `count` finished results in finish order."""
        pipe = self.client.pipeline()
        for member in self.client.zrange(zset_key, 0, count - 1):
            result_key = self.RESULT_KEY.format(
                prefix=self.key_prefix, result_id=member.decode()
            )
            pipe.get(result_key)
        for stored in pipe.execute():
            if stored:
                yield self.deserialize_task_result(stored.decode())

    def get_result(self, result_id: str) -> TaskResult:
        if data := self.client.get(
            self.RESULT_KEY.format(prefix=self.key_prefix, result_id=result_id)
        ):
            return self.deserialize_task_result(data.decode())
        raise TaskResultDoesNotExist(f"Task result {result_id!r} does not exist.")

    async def queue_stats(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        client = self.async_client
        pipe = client.pipeline()
        for queue_name in self.queues:
            pipe.zcard(self._segment_key(TaskResultStatus.READY, queue_name))
            pipe.zcard(self._segment_key(TaskResultStatus.RUNNING, queue_name))
            pipe.zcard(
                self.DEFERRED_KEY.format(prefix=self.key_prefix, queue_name=queue_name)
            )
            pipe.zcard(self._segment_key(TaskResultStatus.SUCCESSFUL, queue_name))
            pipe.zcard(self._segment_key(TaskResultStatus.FAILED, queue_name))
        results = await pipe.execute()
        zero_rates = QueueRates(interval=interval, ingress=0, egress=0)
        queues: dict[str, QueueStats] = {}
        for index, queue_name in enumerate(self.queues):
            base = index * 5
            queues[queue_name] = QueueStats(
                counts=QueueCounts(
                    ready=int(results[base] or 0),
                    running=int(results[base + 1] or 0),
                    deferred=int(results[base + 2] or 0),
                    successful=int(results[base + 3] or 0),
                    failed=int(results[base + 4] or 0),
                ),
                rates=zero_rates,
            )
        return BackendTelemetry(queues=queues)

    async def worker_telemetry(
        self,
    ) -> collections.abc.AsyncGenerator[TelemetryEvent]:
        client = self.async_client
        pubsub = client.pubsub()
        await pubsub.subscribe(self.telemetry_channel)
        pubsub.ignore_subscribe_messages = True
        try:
            async for message in pubsub.listen():
                if (data := message.get("data")) is not None:
                    direction, _, queue_name = data.decode().partition(":")
                    try:
                        event = TelemetryEvent(
                            direction=TelemetryDirection(direction),
                            queue_name=queue_name,
                        )
                    except ValueError:
                        continue
                    yield event
        finally:
            await pubsub.unsubscribe(self.telemetry_channel)
            await pubsub.aclose()

    def close(self) -> None:
        """Close the Redis connections."""
        self.client.close()
        self._async_client = None
