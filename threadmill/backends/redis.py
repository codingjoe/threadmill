"""Redis-backed durable priority queue backend for Django's task framework."""

import collections.abc
import dataclasses
import datetime
import logging
import queue
import random
import threading
import time
import uuid
from collections.abc import Awaitable, Generator, Sequence
from pathlib import Path
from typing import Any

import redis
import redis.asyncio
import redis.asyncio.retry
import redis.retry
from django.tasks import DEFAULT_TASK_QUEUE_NAME, TaskResult, TaskResultStatus
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued
from django.utils import timezone
from typing_extensions import sentinel

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
from threadmill.exceptions import AcknowledgementTimeoutError

logger = logging.getLogger(__name__)


class ThreadmillFunctionCommands:
    """
    Functions of the threadmill Redis Functions library.

    The constants mirror the locals declared at the top of the Lua library.
    The methods are shared by the sync and async clients, so on an async client
    they return an awaitable instead of a result.
    """

    LIBRARY = "threadmill"
    DATA = "data"
    SCORE = "score"
    LEASE_WORKER = "lease_worker"
    LEASE_STARTED_AT = "lease_started_at"
    LEASE_TOKEN = "lease_token"  # noqa: S105
    LEASE_FIELDS = (LEASE_WORKER, LEASE_STARTED_AT, LEASE_TOKEN)
    SOURCE = (Path(__file__).resolve().parent / "lua" / "threadmill.lua").read_text()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._functions_loaded = False

    def acquire(
        self,
        *,
        queue_keys: Sequence[str],
        now_ms: float,
        lease_started_at_iso: str,
        task_key_prefix: str,
        worker: str,
        lease_ttl_ms: int,
        start_index: int,
        max_count: int,
    ) -> list[list[str | int]] | Awaitable[list[list[str | int]]]:
        """
        Lease up to `max_count` ready tasks, one round-robin pop per queue.

        Args:
            queue_keys: Interleaved running and ready set keys, one pair per queue.
            now_ms: Current time in milliseconds since the UNIX epoch.
            lease_started_at_iso: Current time as an ISO 8601 string, stamped
                as the lease start.
            task_key_prefix: Prefix of the stored task hashes.
            worker: Worker name stamped as the lease holder.
            lease_ttl_ms: Lease duration in milliseconds.
            start_index: Index of the queue pair to scan first.
            max_count: Maximum number of tasks to lease.

        Returns:
            `[task_id, payload, lease_token, queue_index]` tuples, empty when
            no queue yields a task.
        """
        return self._call(
            "acquire",
            queue_keys,
            [
                str(now_ms),
                lease_started_at_iso,
                task_key_prefix,
                str(len(queue_keys) // 2),
                worker,
                str(lease_ttl_ms),
                str(start_index),
                str(max_count),
            ],
        )

    def acknowledge(
        self,
        *,
        running_key: str,
        result_key: str,
        task_data_key: str,
        successful_results_key: str,
        failed_results_key: str,
        task_id: str,
        serialized_result: str,
        result_ttl_secs: int,
        finished_at_ms: float,
        status: str,
        telemetry_channel: str,
        queue_name: str,
        lease_token: str,
    ) -> int | Awaitable[int]:
        """
        Finalize a running task.

        Removes the running entry, stores the result for `result_ttl_secs`,
        deletes the task hash, records the result in its status history, and
        publishes an egress event.

        Args:
            running_key: Running set of the task's queue.
            result_key: Result key that receives the serialized result.
            task_data_key: Task hash that holds the payload and lease.
            successful_results_key: Result history for successful tasks.
            failed_results_key: Result history for failed tasks.
            task_id: Task identifier.
            serialized_result: Serialized task result to store.
            result_ttl_secs: Result retention in seconds.
            finished_at_ms: Finish time in milliseconds since the UNIX epoch.
            status: Status name of the finished task.
            telemetry_channel: Pub/sub channel for the egress event.
            queue_name: Queue that the task belongs to.
            lease_token: Token of the lease holder, or an empty string.

        Returns:
            1 when the task was finalized, 0 when it was no longer running.
        """
        return self._call(
            "acknowledge",
            [
                running_key,
                result_key,
                task_data_key,
                successful_results_key,
                failed_results_key,
            ],
            [
                task_id,
                serialized_result,
                str(result_ttl_secs),
                str(finished_at_ms),
                status,
                telemetry_channel,
                queue_name,
                lease_token,
            ],
        )

    def dequeue(
        self, *, segment_key: str, task_data_key: str, result_key: str, task_id: str
    ) -> int | Awaitable[int]:
        """
        Delete a task from its segment together with its payload.

        The payload is kept when the segment entry is already gone, so a late
        result or a requeued task survives.

        Args:
            segment_key: Status segment that holds the task ID.
            task_data_key: Task hash to delete.
            result_key: Result key to delete.
            task_id: Task identifier.

        Returns:
            1 when the segment entry existed, 0 otherwise.
        """
        return self._call(
            "dequeue", [segment_key, task_data_key, result_key], [task_id]
        )

    def mover(
        self,
        *,
        deferred_key: str,
        ready_key: str,
        now_ms: float,
        task_key_prefix: str,
        max_count: int,
    ) -> list[str] | Awaitable[list[str]]:
        """
        Move due deferred tasks to the ready queue.

        Args:
            deferred_key: Deferred set scored by scheduled time.
            ready_key: Ready queue that receives the due tasks.
            now_ms: Current time in milliseconds since the UNIX epoch.
            task_key_prefix: Prefix of the stored task hashes.
            max_count: Maximum number of tasks to move.

        Returns:
            IDs dropped because their stored payload is missing.
        """
        return self._call(
            "mover",
            [deferred_key, ready_key],
            [str(now_ms), task_key_prefix, str(max_count)],
        )

    def purge(
        self,
        *,
        ready_key: str,
        running_key: str,
        deferred_key: str,
        successful_results_key: str,
        failed_results_key: str,
        task_key_prefix: str,
        result_key_prefix: str,
    ) -> int | Awaitable[int]:
        """
        Delete every task and payload of one queue.

        Args:
            ready_key: Ready set of the queue.
            running_key: Running set of the queue.
            deferred_key: Deferred set of the queue.
            successful_results_key: Successful result history of the queue.
            failed_results_key: Failed result history of the queue.
            task_key_prefix: Prefix of the stored task hashes.
            result_key_prefix: Prefix of the stored results.

        Returns:
            Number of deleted payloads.
        """
        return self._call(
            "purge",
            [
                ready_key,
                running_key,
                deferred_key,
                successful_results_key,
                failed_results_key,
            ],
            [task_key_prefix, result_key_prefix],
        )

    def reaper(
        self,
        *,
        running_key: str,
        claim_ttl_ms: int,
        max_count: int,
        task_key_prefix: str,
    ) -> list[str] | Awaitable[list[str]]:
        """
        Claim tasks whose lease expired.

        Args:
            running_key: Running set scored by lease deadline.
            claim_ttl_ms: How long a claim is held, in milliseconds.
            max_count: Maximum number of tasks to claim.
            task_key_prefix: Prefix of the stored task hashes.

        Returns:
            IDs of the claimed tasks.
        """
        return self._call(
            "reaper",
            [running_key],
            [str(claim_ttl_ms), str(max_count), task_key_prefix],
        )


class ThreadmillRedis(ThreadmillFunctionCommands, redis.Redis):
    """Synchronous Redis client with the threadmill function commands."""

    def _call(self, name: str, keys: Sequence[str], args: Sequence[str]) -> Any:
        if not self._functions_loaded:
            self.function_load(self.SOURCE, replace=True)
            self._functions_loaded = True
        return self.fcall(f"{self.LIBRARY}_{name}", len(keys), *keys, *args)


class ThreadmillAsyncRedis(ThreadmillFunctionCommands, redis.asyncio.Redis):
    """Asynchronous Redis client with the threadmill function commands."""

    async def _call(self, name: str, keys: Sequence[str], args: Sequence[str]) -> Any:
        if not self._functions_loaded:
            await self.function_load(self.SOURCE, replace=True)
            self._functions_loaded = True
        return await self.fcall(f"{self.LIBRARY}_{name}", len(keys), *keys, *args)


def _parse_lease_started_at(value: str | None) -> datetime.datetime | None:
    """Return the lease start stamped beside a task, or None when the hash holds no timestamp."""
    try:
        return datetime.datetime.fromisoformat(value)
    except TypeError, ValueError:
        return None


@dataclasses.dataclass(kw_only=True, slots=True)
class _IdleBackoff:
    """
    Idle polling state for a single worker thread.

    Every worker thread backs off and rotates independently, so one thread's
    misses never reset or double another thread's delay. Private, so it is not
    mistaken for a public backoff like ``threadmill.retry.ExponentialBackoff``,
    which retries failed tasks rather than pacing idle polls.
    """

    miss_count: int = 0
    rotation_offset: int = 0


class RedisBroker(Broker):
    """Background maintenance broker for the Redis backend."""

    backend: RedisTaskBackend

    CLAIM_TTL = datetime.timedelta(seconds=60)
    """How long a reaper claim is held before another broker pass may take it over."""

    def __init__(self, backend: RedisTaskBackend) -> None:
        interval = backend.options.get("broker_interval", datetime.timedelta(seconds=1))
        super().__init__(backend, interval=interval)

    def _move_queue(self, queue_name: str) -> None:
        """Move due deferred tasks from a single deferred set."""
        deferred_key = self.backend.DEFERRED_KEY.format(
            prefix=self.backend.key_prefix, queue_name=queue_name
        )
        queue_key = self.backend._segment_key(TaskResultStatus.READY, queue_name)
        if dropped := self.backend.client.mover(
            deferred_key=deferred_key,
            ready_key=queue_key,
            now_ms=time.time() * 1000,
            task_key_prefix=self.backend.task_key_prefix,
            max_count=self.backend.batch_size,
        ):
            logger.warning(
                "Dropped %d deferred task(s) of queue %r without a stored payload: %s",
                len(dropped),
                queue_name,
                ", ".join(dropped),
            )

    def _reap_running_queue(self, queue_name: str) -> None:
        """Claim tasks whose processing lease has expired from the running set."""
        running_key = self.backend._segment_key(TaskResultStatus.RUNNING, queue_name)
        claimed_ids = self.backend.client.reaper(
            running_key=running_key,
            claim_ttl_ms=int(self.CLAIM_TTL.total_seconds() * 1000),
            max_count=self.backend.batch_size,
            task_key_prefix=self.backend.task_key_prefix,
        )
        for task_id in claimed_ids:
            self._reap_task(task_id, queue_name)

    def _reap_task(self, task_id: str, queue_name: str) -> None:
        """Requeue or fail it, and remove it when the payload cannot be read."""
        try:
            task_result = self.backend.get_leased_task(task_id)
        except (ImportError, ValueError) as exception:
            self.backend.discard_unreadable_task(
                task_id, queue_name=queue_name, error=exception
            )
            return
        if task_result is None:
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
                    AcknowledgementTimeoutError("Task processing lease expired.")
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
            except Exception:
                logger.exception("Mover error for queue %r", queue_name)

            try:
                self._reap_running_queue(queue_name)
            except Exception:
                logger.exception("Running reaper error for queue %r", queue_name)


class RedisTaskBackend(ThreadmillTaskBackend):
    """
    Redis-backed durable priority queue backend.

    Uses sorted sets for priority ordering, a running set for in-flight
    tracking, and a deferred set for scheduled tasks. All multi-step operations
    are atomic via Redis Functions, loaded on first use.
    """

    supports_async_task = True
    supports_get_result = True
    supports_priority = True
    supports_defer = True

    broker_class = RedisBroker

    TASK_KEY_PREFIX = "{prefix}:task:"
    TASK_KEY = TASK_KEY_PREFIX + "{task_id}"
    RESULT_KEY_PREFIX = "{prefix}:result:"
    RESULT_KEY = RESULT_KEY_PREFIX + "{result_id}"
    SEGMENT_KEY = "{prefix}:{queue_name}:{status}"
    DEFERRED_KEY = "{prefix}:{queue_name}:deferred"

    TELEMETRY_CHANNEL = "{prefix}:telemetry"

    def _segment_key(self, status: TaskResultStatus, queue_name: str) -> str:
        return self.SEGMENT_KEY.format(
            prefix=self.key_prefix,
            queue_name=queue_name,
            status=status.value.lower(),
        )

    def get_leased_task(self, task_id: str) -> ThreadmillTaskResult | None:
        """Return a running task as its lease holds it, or None when its hash is gone."""
        task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id)
        data, lease_worker, lease_started_at, lease_token = self.client.hmget(
            task_key,
            ThreadmillFunctionCommands.DATA,
            *ThreadmillFunctionCommands.LEASE_FIELDS,
        )
        if data is None:
            return None
        return self._apply_lease(
            self.deserialize_task_result(data),
            worker=lease_worker or None,
            lease_started_at=_parse_lease_started_at(lease_started_at),
            lease_token=lease_token,
        )

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias=alias, params=params)

        try:
            redis_url = params["REDIS_URL"]
        except KeyError as e:
            message = (
                "REDIS_URL must be specified in your settings for the "
                f"{type(self).__name__}."
            )
            raise ValueError(message) from e
        self.redis_connection_retries = self.options.get("redis_connection_retries", 3)
        self.client = ThreadmillRedis.from_url(
            redis_url,
            decode_responses=True,
            retry=redis.retry.Retry(
                redis.default_backoff(), self.redis_connection_retries
            ),
        )
        self.redis_url = redis_url
        self.key_prefix = f"threadmill:{{{alias}}}"
        self.task_key_prefix = self.TASK_KEY_PREFIX.format(prefix=self.key_prefix)
        self.result_key_prefix = self.RESULT_KEY_PREFIX.format(prefix=self.key_prefix)
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
        self._idle_backoffs = threading.local()

    @property
    def _idle_backoff(self) -> _IdleBackoff:
        """Return the idle polling state of the calling worker thread."""
        backoff = getattr(self._idle_backoffs, "backoff", None)
        if backoff is None:
            backoff = _IdleBackoff(
                rotation_offset=random.randrange(len(self.queues))  # noqa: S311
            )
            self._idle_backoffs.backoff = backoff
        return backoff

    def async_client(self) -> ThreadmillAsyncRedis:
        return ThreadmillAsyncRedis.from_url(
            self.redis_url,
            decode_responses=True,
            retry=redis.asyncio.retry.Retry(
                redis.default_backoff(), self.redis_connection_retries
            ),
        )

    def _compute_score(self, priority: int, enqueued_at: datetime.datetime) -> float:
        """
        Compute a ZSET score for priority-ordered FIFO queueing.

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
        """
        Enqueue a task for execution.

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

        pipe = self.client.pipeline()
        pipe.hset(
            task_key,
            mapping={
                ThreadmillFunctionCommands.DATA: serialized,
                ThreadmillFunctionCommands.SCORE: str(score),
            },
        )
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
        count: int = 1,
        timeout: datetime.timedelta | None = None,
        worker: str = "",
    ) -> list[ThreadmillTaskResult]:
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

        idle_backoff = self._idle_backoff
        while True:
            now = timezone.now()
            now_ms = now.timestamp() * 1000
            now_iso = now.isoformat()

            if result := self.client.acquire(
                queue_keys=keys,
                now_ms=now_ms,
                lease_started_at_iso=now_iso,
                task_key_prefix=self.task_key_prefix,
                worker=worker,
                lease_ttl_ms=int(self.lease_ttl.total_seconds() * 1000),
                start_index=idle_backoff.rotation_offset,
                max_count=count,
            ):
                idle_backoff.miss_count = 0
                idle_backoff.rotation_offset = (idle_backoff.rotation_offset + 1) % len(
                    queue_names
                )
                task_results: list[ThreadmillTaskResult] = []
                for task_id, data, lease_token, queue_index in result:
                    try:
                        task_result = self.deserialize_task_result(data)
                    except (ImportError, ValueError) as exception:
                        self.discard_unreadable_task(
                            task_id,
                            queue_name=queue_names[int(queue_index)],
                            error=exception,
                        )
                    else:
                        task_results.append(
                            self._apply_lease(
                                task_result,
                                worker=worker,
                                lease_started_at=now,
                                lease_token=lease_token,
                            )
                        )
                if task_results:
                    return task_results

            try:
                remaining = deadline - time.monotonic()
            except TypeError:
                message = "No task available."
                raise queue.Empty(message)
            if remaining <= 0:
                message = "No task available within the specified timeout."
                raise TimeoutError(message)
            # Stop doubling once the interval reaches poll_max_interval; larger
            # exponents would only overflow the float math.
            cap = int(self.poll_max_interval / self.poll_interval).bit_length()
            interval_secs = min(
                self.poll_interval.total_seconds()
                * 2 ** min(idle_backoff.miss_count, cap),
                self.poll_max_interval.total_seconds(),
                remaining,
            )
            idle_backoff.miss_count += 1
            time.sleep(interval_secs)

    @staticmethod
    def _apply_lease(
        task_result: TaskResult,
        *,
        worker: str | None,
        lease_started_at: datetime.datetime | None,
        lease_token: str | None,
    ) -> ThreadmillTaskResult:
        """
        Return a stored task result as a running attempt.

        `worker=None` means the lease records no worker; an empty string still
        counts as an attempt, and a task that already records a start keeps it.
        """
        task_result = ThreadmillTaskResult.from_result(
            task_result, lease_token=lease_token
        )
        return dataclasses.replace(
            task_result,
            status=TaskResultStatus.RUNNING,
            started_at=task_result.started_at or lease_started_at,
            last_attempted_at=lease_started_at or task_result.last_attempted_at,
            worker_ids=(
                [*task_result.worker_ids, worker]
                if worker is not None
                else task_result.worker_ids
            ),
        )

    def discard_unreadable_task(
        self,
        task_id: str,
        *,
        queue_name: str,
        error: BaseException,
    ) -> None:
        """Remove a task whose stored payload cannot be read."""
        logger.error(
            "Task %r payload cannot be read: %s; removing it from queue %r",
            task_id,
            error,
            queue_name,
            exc_info=error,
        )
        self.client.dequeue(
            segment_key=self._segment_key(TaskResultStatus.RUNNING, queue_name),
            task_data_key=self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id),
            result_key=self.RESULT_KEY.format(
                prefix=self.key_prefix, result_id=task_id
            ),
            task_id=task_id,
        )
        self.client.publish(self.telemetry_channel, f"egress:{queue_name}")

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

        self.client.acknowledge(
            running_key=running_key,
            result_key=result_key,
            task_data_key=task_key,
            successful_results_key=successful_results_key,
            failed_results_key=failed_results_key,
            task_id=task_result.id,
            serialized_result=serialized,
            result_ttl_secs=int(self.result_ttl.total_seconds()),
            finished_at_ms=finish_score,
            status=task_result.status.name,
            telemetry_channel=self.telemetry_channel,
            queue_name=task_result.task.queue_name,
            lease_token=lease_token or "",
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

        pipe = self.client.pipeline()
        pipe.zrem(running_key, task_result.id)
        pipe.zrem(failed_key, task_result.id)
        pipe.delete(result_key)
        pipe.delete(task_key)
        pipe.hset(
            task_key,
            mapping={
                ThreadmillFunctionCommands.DATA: serialized,
                ThreadmillFunctionCommands.SCORE: str(score),
                **dict.fromkeys(ThreadmillFunctionCommands.LEASE_FIELDS, ""),
            },
        )
        pipe.zadd(deferred_key, {task_result.id: run_after_ms})
        pipe.publish(self.telemetry_channel, f"ingress:{task_result.task.queue_name}")
        pipe.execute()

        task_enqueued.send(self.__class__, task_result=task_result)

    def dequeue(self, task_result: TaskResult) -> bool:
        return bool(
            self.client.dequeue(
                segment_key=self._segment_key(
                    task_result.status, task_result.task.queue_name
                ),
                task_data_key=self.TASK_KEY.format(
                    prefix=self.key_prefix, task_id=task_result.id
                ),
                result_key=self.RESULT_KEY.format(
                    prefix=self.key_prefix, result_id=task_result.id
                ),
                task_id=task_result.id,
            )
        )

    def purge(self, queue_name: str) -> None:
        self.client.purge(
            ready_key=self._segment_key(TaskResultStatus.READY, queue_name),
            running_key=self._segment_key(TaskResultStatus.RUNNING, queue_name),
            deferred_key=self.DEFERRED_KEY.format(
                prefix=self.key_prefix, queue_name=queue_name
            ),
            successful_results_key=self._segment_key(
                TaskResultStatus.SUCCESSFUL, queue_name
            ),
            failed_results_key=self._segment_key(TaskResultStatus.FAILED, queue_name),
            task_key_prefix=self.task_key_prefix,
            result_key_prefix=self.result_key_prefix,
        )

    def peek(
        self,
        queue_name: str = DEFAULT_TASK_QUEUE_NAME,
        *,
        status: TaskResultStatus,
        run_before: datetime.datetime | sentinel = ThreadmillTaskBackend.FOREVER,
        count: int = 1,
    ) -> Generator[TaskResult]:
        if run_before is not ThreadmillTaskBackend.FOREVER and timezone.is_naive(
            run_before
        ):
            message = "run_before must be timezone-aware."
            raise ValueError(message)
        match status:
            case TaskResultStatus.READY:
                ready = list(
                    self._peek_tasks(
                        self._segment_key(status, queue_name), count, leased=False
                    )
                )
                yield from ready
                if not count or len(ready) < count:
                    yield from self._peek_deferred(
                        queue_name, run_before, max(count - len(ready), 0)
                    )
            case TaskResultStatus.RUNNING:
                yield from self._peek_tasks(
                    self._segment_key(status, queue_name), count, leased=True
                )
            case TaskResultStatus.SUCCESSFUL | TaskResultStatus.FAILED:
                yield from self._peek_results(
                    self._segment_key(status, queue_name), count
                )

    def _peek_tasks(
        self, zset_key: str, count: int, *, leased: bool
    ) -> Generator[TaskResult]:
        pipe = self.client.pipeline()
        for task_id in self.client.zrange(zset_key, 0, count - 1):
            task_key = self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id)
            if leased:
                pipe.hmget(
                    task_key,
                    ThreadmillFunctionCommands.DATA,
                    *ThreadmillFunctionCommands.LEASE_FIELDS,
                )
            else:
                pipe.hget(task_key, ThreadmillFunctionCommands.DATA)
        for stored in pipe.execute():
            if not stored:
                continue
            if leased:
                data, lease_worker, lease_started_at, lease_token = stored
                if not data:
                    continue
                yield self._apply_lease(
                    self.deserialize_task_result(data),
                    worker=lease_worker or None,
                    lease_started_at=_parse_lease_started_at(lease_started_at),
                    lease_token=lease_token,
                )
            else:
                yield self.deserialize_task_result(stored)

    def _peek_deferred(
        self, queue_name: str, run_before: datetime.datetime | sentinel, count: int
    ) -> Generator[TaskResult]:
        deferred_key = self.DEFERRED_KEY.format(
            prefix=self.key_prefix, queue_name=queue_name
        )
        max_score: float | str = (
            "+inf"
            if run_before is ThreadmillTaskBackend.FOREVER
            else run_before.timestamp() * 1000
        )
        pipe = self.client.pipeline()
        for task_id in self.client.zrangebyscore(
            deferred_key, "-inf", max_score, start=0, num=count or -1
        ):
            pipe.hget(
                self.TASK_KEY.format(prefix=self.key_prefix, task_id=task_id),
                ThreadmillFunctionCommands.DATA,
            )
        for stored in pipe.execute():
            if stored:
                yield self.deserialize_task_result(stored)

    def _peek_results(self, zset_key: str, count: int) -> Generator[TaskResult]:
        pipe = self.client.pipeline()
        for result_id in self.client.zrange(zset_key, 0, count - 1, desc=True):
            result_key = self.RESULT_KEY.format(
                prefix=self.key_prefix, result_id=result_id
            )
            pipe.get(result_key)
        for stored in pipe.execute():
            if stored:
                yield self.deserialize_task_result(stored)

    def get_result(self, result_id: str) -> TaskResult:
        if data := self.client.get(
            self.RESULT_KEY.format(prefix=self.key_prefix, result_id=result_id)
        ):
            return self.deserialize_task_result(data)
        message = f"Task result {result_id!r} does not exist."
        raise TaskResultDoesNotExist(message)

    async def queue_stats(
        self, *, interval: datetime.timedelta = datetime.timedelta(seconds=60)
    ) -> BackendTelemetry:
        async with self.async_client() as client:
            pipe = client.pipeline()
            for queue_name in self.queues:
                pipe.zcard(self._segment_key(TaskResultStatus.READY, queue_name))
                pipe.zcard(self._segment_key(TaskResultStatus.RUNNING, queue_name))
                pipe.zcard(
                    self.DEFERRED_KEY.format(
                        prefix=self.key_prefix, queue_name=queue_name
                    )
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
        async with self.async_client() as client:
            pubsub = client.pubsub()
            await pubsub.subscribe(self.telemetry_channel)
            pubsub.ignore_subscribe_messages = True
            try:
                async for message in pubsub.listen():
                    direction, _, queue_name = message["data"].partition(":")
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
