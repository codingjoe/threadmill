from __future__ import annotations

import dataclasses
import datetime
import logging
import time
from dataclasses import replace
from unittest.mock import patch

from django.tasks import default_task_backend
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from tests.testapp.tasks import boom, compute_workload, echo
from threadmill.backends.base import (
    BackendTelemetry,
    QueueCounts,
    QueueRates,
    QueueStats,
)
from threadmill.backends.redis import RedisBroker, RedisTaskBackend  # noqa: E402

TELEMETRY_INTERVAL = datetime.timedelta(seconds=60)


def _stats(**overrides: int | datetime.timedelta) -> QueueStats:
    interval = overrides.pop("interval", TELEMETRY_INTERVAL)
    counts = QueueCounts(
        ready=overrides.get("ready", 0),
        running=overrides.get("running", 0),
        deferred=overrides.get("deferred", 0),
        successful=overrides.get("successful", 0),
        failed=overrides.get("failed", 0),
    )
    rates = QueueRates(
        interval=interval,
        ingress=overrides.get("ingress", 0),
        egress=overrides.get("egress", 0),
    )
    return QueueStats(counts=counts, rates=rates)


class TestRedisBroker:
    def test_mover__moves_deferred_task_to_ready(self):
        """Mover promotes due deferred tasks to the ready queue."""
        deferred_task = replace(
            compute_workload,
            run_after=timezone.now() - datetime.timedelta(seconds=10),
        )
        task_result = default_task_backend.enqueue(deferred_task, args=[])
        broker = RedisBroker(default_task_backend)
        broker.main()
        acquired = default_task_backend.acquire(timeout=datetime.timedelta(seconds=1))
        assert acquired is not None
        assert acquired.id == task_result.id

    def test_error_path__maintain_continues_after_exception(self, caplog):
        """main() logs and continues when any per-queue step raises."""
        broker = RedisBroker(default_task_backend)
        with caplog.at_level(logging.ERROR):
            with (
                patch.object(broker, "_move_queue", side_effect=RuntimeError("mover")),
                patch.object(
                    broker, "_reap_running_queue", side_effect=RuntimeError("reaper")
                ),
                patch.object(
                    default_task_backend,
                    "_trim_telemetry",
                    side_effect=RuntimeError("trim"),
                ),
            ):
                broker.main()
        assert "Mover error for queue" in caplog.text
        assert "Running reaper error for queue" in caplog.text
        assert "Telemetry trim error for queue" in caplog.text

    def test_main__trims_stale_telemetry(self):
        """main() trims ingress events older than the result_ttl retention horizon."""
        backend = RedisTaskBackend(
            "broker_trim_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[1])
            ingress_key = backend.INGRESS_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            old = (timezone.now() - datetime.timedelta(seconds=120)).timestamp() * 1000
            backend.client.zadd(ingress_key, {task_result.id: old})
            assert backend.client.zcard(ingress_key) == 1

            broker = RedisBroker(backend)
            broker.main()

            assert backend.client.zcard(ingress_key) == 0
        finally:
            backend.close()


class TestRedisTaskBackend:
    """Tests for the RedisTaskBackend update and lease functionality."""

    def test_acquire__moves_to_running_set(self):
        """acquire() moves task directly to running set with worker info."""
        backend = RedisTaskBackend(
            "acquire_running_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="worker-1"
            )
            assert acquired is not None
            assert acquired.id == task_result.id

            # Verify task is in running set, not in any processing set
            running_key = backend.RUNNING_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            assert backend.client.zscore(running_key, task_result.id) is not None

            # Verify task data was updated with worker info
            task_key = backend.TASK_KEY.format(
                prefix=backend.key_prefix, task_id=task_result.id
            )
            stored_data = backend.client.hget(task_key, "data")
            deserialized = backend.deserialize_task_result(stored_data)
            assert deserialized.status == TaskResultStatus.RUNNING
            assert deserialized.worker_ids == ["worker-1"]
            assert deserialized.last_attempted_at is not None
        finally:
            backend.close()

    def test_acquire__sets_last_attempted_at(self):
        """acquire() sets last_attempted_at and worker_ids in the stored task data."""
        backend = RedisTaskBackend(
            "last_attempted_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(hours=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="test-worker"
            )
            assert acquired is not None
            assert acquired.last_attempted_at is not None
            assert acquired.worker_ids == ["test-worker"]

            # Verify it's persisted in Redis
            task_key = backend.TASK_KEY.format(
                prefix=backend.key_prefix, task_id=task_result.id
            )
            stored_data = backend.client.hget(task_key, "data")
            deserialized = backend.deserialize_task_result(stored_data)
            assert deserialized.last_attempted_at is not None
            assert deserialized.worker_ids == ["test-worker"]
        finally:
            backend.close()

    def test_running_reaper__fails_expired_tasks(self):
        """Running reaper creates FAILED results for tasks with expired lease."""
        backend = RedisTaskBackend(
            "running_reaper_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(seconds=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="reaper-test"
            )
            assert acquired is not None

            # Wait for lease to expire
            time.sleep(1.1)

            # Run the broker
            broker = RedisBroker(backend)
            broker.main()

            # Verify the task result exists and is FAILED
            result = backend.get_result(task_result.id)
            assert result.status == TaskResultStatus.FAILED
            assert len(result.errors) == 1
            assert "AcknowledgementTimeout" in result.errors[0].exception_class_path

            # Reaping an expired task must count as egress and failed
            stats = backend.telemetry().queues["default"]
            assert stats.rates.egress == 1
            assert stats.counts.failed == 1
            assert stats.counts.successful == 0
        finally:
            backend.close()

    def test_stale_acknowledge__is_noop(self):
        """acknowledge() is a no-op when the task is no longer in the running set."""
        backend = RedisTaskBackend(
            "stale_ack_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(seconds=1),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="stale-ack-test"
            )
            assert acquired is not None

            # Wait for lease to expire
            time.sleep(1.1)

            # Run the broker to reap the running set
            broker = RedisBroker(backend)
            broker.main()

            # Try to acknowledge the task (should be a no-op since it was reaped)
            finished = dataclasses.replace(
                acquired,
                status=TaskResultStatus.SUCCESSFUL,
                finished_at=timezone.now(),
            )
            # This should not raise
            backend.acknowledge(finished)

            # The result should still be the FAILED one from the reaper
            result = backend.get_result(task_result.id)
            assert result.status == TaskResultStatus.FAILED
        finally:
            backend.close()

    def test_telemetry__empty_backend(self):
        """queue_telemetry returns zero counts for an empty backend."""
        backend = RedisTaskBackend(
            "telemetry_empty_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            telemetry = backend.telemetry()
            assert telemetry == BackendTelemetry(queues={"default": _stats()})
        finally:
            backend.close()

    def test_telemetry__counts_tasks(self):
        """queue_telemetry reflects per-minute ingress/egress and status counters."""
        backend = RedisTaskBackend(
            "telemetry_counts_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[42])
            backend.enqueue(boom, args=[])

            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="telemetry-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )

            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="telemetry-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )

            telemetry = backend.telemetry()
            assert telemetry.queues["default"] == _stats(
                ingress=2,
                egress=2,
                successful=1,
                failed=1,
            )
        finally:
            backend.close()

    def test_telemetry__egress_equals_successful_plus_failed(self):
        """Egress is derived as successful + failed over the display window."""
        backend = RedisTaskBackend(
            "telemetry_egress_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            backend.enqueue(echo, args=[1])
            backend.enqueue(echo, args=[2])
            backend.enqueue(echo, args=[3])

            for _ in range(2):
                acquired = backend.acquire(
                    timeout=datetime.timedelta(seconds=1), worker="egress-test"
                )
                assert acquired is not None
                backend.acknowledge(
                    dataclasses.replace(
                        acquired,
                        status=TaskResultStatus.SUCCESSFUL,
                        finished_at=timezone.now(),
                    )
                )
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="egress-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.FAILED,
                    finished_at=timezone.now(),
                )
            )

            stats = backend.telemetry().queues["default"]
            assert stats.rates.egress == stats.counts.successful + stats.counts.failed
            assert stats.rates.egress == 3
            assert stats.counts.successful == 2
            assert stats.counts.failed == 1
        finally:
            backend.close()

    def test_telemetry__successful_failed_evicted_by_result_ttl(self):
        """successful/failed are segment counts that drop when results age out."""
        backend = RedisTaskBackend(
            "telemetry_eviction_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            first = backend.enqueue(echo, args=[1])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="eviction-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )
            successful_key = backend.SUCCESSFUL_RESULTS_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            assert backend.client.zcard(successful_key) == 1

            # Age the first result beyond the retention horizon.
            old = (timezone.now() - datetime.timedelta(seconds=120)).timestamp() * 1000
            backend.client.zadd(successful_key, {first.id: old})

            # A subsequent acknowledge evicts results older than result_ttl.
            backend.enqueue(echo, args=[2])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="eviction-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )

            assert backend.client.zcard(successful_key) == 1
            stats = backend.telemetry().queues["default"]
            assert stats.counts.successful == 1
        finally:
            backend.close()

    def test_telemetry__ingress_egress_age_out_of_window(self):
        """Ingress and egress older than the display window are excluded from the rates."""
        backend = RedisTaskBackend(
            "telemetry_window_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            acquired = backend.acquire(
                timeout=datetime.timedelta(seconds=1), worker="window-test"
            )
            assert acquired is not None
            backend.acknowledge(
                dataclasses.replace(
                    acquired,
                    status=TaskResultStatus.SUCCESSFUL,
                    finished_at=timezone.now(),
                )
            )

            ingress_key = backend.INGRESS_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            successful_results_key = backend.SUCCESSFUL_RESULTS_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            old = (timezone.now() - datetime.timedelta(seconds=120)).timestamp() * 1000
            backend.client.zadd(ingress_key, {task_result.id: old})
            backend.client.zadd(successful_results_key, {task_result.id: old})

            stats = backend.telemetry().queues["default"]
            # Older than the 60s display window: not counted as recent throughput.
            assert stats.rates.ingress == 0
            assert stats.rates.egress == 0
            # Ingress is evicted by the read path (retention = result_ttl = 60s).
            assert backend.client.zcard(ingress_key) == 0
            # The results segment is evicted by acknowledge/reaper, not by reads,
            # so the aged result is still present in the segment count.
            assert backend.client.zcard(successful_results_key) == 1
            assert stats.counts.successful == 1
        finally:
            backend.close()

    def test_telemetry__interval_shrinks_window(self):
        """A custom interval excludes events from the rate but never trims below result_ttl."""
        backend = RedisTaskBackend(
            "telemetry_interval_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            task_result = backend.enqueue(echo, args=[42])
            ingress_key = backend.INGRESS_KEY.format(
                prefix=backend.key_prefix, queue_name="default"
            )
            recent = (timezone.now() - datetime.timedelta(seconds=5)).timestamp() * 1000
            backend.client.zadd(ingress_key, {task_result.id: recent})

            # The 30s-old event is inside the default 60s window but outside a 10s one.
            stats = backend.telemetry(interval=datetime.timedelta(seconds=10)).queues[
                "default"
            ]
            assert stats.rates.ingress == 1

            stale = (timezone.now() - datetime.timedelta(seconds=30)).timestamp() * 1000
            backend.client.zadd(ingress_key, {task_result.id: stale})
            stats = backend.telemetry(interval=datetime.timedelta(seconds=10)).queues[
                "default"
            ]
            # Outside the 10s count window, so not displayed...
            assert stats.rates.ingress == 0
            # ...but still retained for result_ttl (60s): a narrower display
            # window must not clobber the retention policy.
            assert backend.client.zcard(ingress_key) == 1
        finally:
            backend.close()


class TestRedisTaskBackendPeek:
    """Tests for the RedisTaskBackend peek API."""

    def _acknowledge(self, status: TaskResultStatus) -> str:
        """Enqueue, acquire, and acknowledge a task with the given status."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="peek-test"
        )
        assert acquired.id == task_result.id
        default_task_backend.acknowledge(
            dataclasses.replace(acquired, status=status, finished_at=timezone.now())
        )
        return task_result.id

    def test_peek__ready_tasks(self):
        """Peek READY returns enqueued tasks in queue order."""
        default_task_backend.enqueue(echo, args=[1])
        default_task_backend.enqueue(echo, args=[2])
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.READY, count=10
            )
        )
        assert [r.args for r in results] == [[1], [2]]

    def test_peek__running_tasks(self):
        """Peek RUNNING returns acquired tasks with worker info."""
        default_task_backend.enqueue(echo, args=[1])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="peek-test"
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.RUNNING, count=10
            )
        )
        assert [r.id for r in results] == [acquired.id]
        assert results[0].status == TaskResultStatus.RUNNING

    def test_peek__successful_and_failed_history(self):
        """Peek SUCCESSFUL/FAILED filter acknowledged results by status."""
        successful_id = self._acknowledge(TaskResultStatus.SUCCESSFUL)
        failed_id = self._acknowledge(TaskResultStatus.FAILED)
        successful = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.SUCCESSFUL, count=10
            )
        )
        failed = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.FAILED, count=10
            )
        )
        assert [r.id for r in successful] == [successful_id]
        assert [r.id for r in failed] == [failed_id]

    def test_peek__status_none_returns_ready_and_history(self):
        """Peek with status None yields ready and acknowledged tasks."""
        first = default_task_backend.enqueue(echo, args=[1])
        second = default_task_backend.enqueue(echo, args=[2])
        acquired = default_task_backend.acquire(
            timeout=datetime.timedelta(seconds=1), worker="peek-test"
        )
        default_task_backend.acknowledge(
            dataclasses.replace(
                acquired, status=TaskResultStatus.FAILED, finished_at=timezone.now()
            )
        )
        results = list(
            default_task_backend.peek(queue_name="default", status=None, count=10)
        )
        ids = {r.id for r in results}
        assert second.id in ids
        assert acquired.id in ids
        assert first.id == acquired.id

    def test_peek__status_none_skips_empty_running_and_history(self):
        """Peek None yields ready tasks and skips empty running/history zsets."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        results = list(
            default_task_backend.peek(queue_name="default", status=None, count=10)
        )
        assert [r.id for r in results] == [task_result.id]

    def test_peek__skips_expired_task_data(self):
        """Peek skips queue entries whose task data hash has expired."""
        task_result = default_task_backend.enqueue(echo, args=[1])
        default_task_backend.client.delete(
            default_task_backend.TASK_KEY.format(
                prefix=default_task_backend.key_prefix, task_id=task_result.id
            )
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.READY, count=10
            )
        )
        assert results == []

    def test_peek__skips_expired_result_data(self):
        """Peek skips history entries whose result key has expired."""
        result_id = self._acknowledge(TaskResultStatus.SUCCESSFUL)
        default_task_backend.client.delete(
            default_task_backend.RESULT_KEY.format(
                prefix=default_task_backend.key_prefix, result_id=result_id
            )
        )
        results = list(
            default_task_backend.peek(
                queue_name="default", status=TaskResultStatus.SUCCESSFUL, count=10
            )
        )
        assert results == []
