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

from tests.testapp.tasks import compute_workload, echo
from threadmill.backends.redis import RedisBroker, RedisTaskBackend  # noqa: E402


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
        """main() logs and continues when _move_queue raises."""
        broker = RedisBroker(default_task_backend)
        with caplog.at_level(logging.ERROR):
            with patch.object(
                broker,
                "_move_queue",
                side_effect=RuntimeError("boom"),
            ):
                broker.main()
        assert "Mover error for queue" in caplog.text


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

    def test_lease_ttl_defaults(self):
        """lease_ttl defaults to 1h."""
        backend = RedisTaskBackend(
            "default_lease_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            assert backend.lease_ttl == datetime.timedelta(hours=1)
        finally:
            backend.close()

    def test_explicit_lease_ttl(self):
        """lease_ttl is used when set."""
        backend = RedisTaskBackend(
            "explicit_lease_test",
            {
                "QUEUES": ["default"],
                "REDIS_URL": "redis://localhost:6379/0",
                "OPTIONS": {
                    "lease_ttl": datetime.timedelta(seconds=120),
                    "result_ttl": datetime.timedelta(seconds=60),
                },
            },
        )
        try:
            assert backend.lease_ttl == datetime.timedelta(seconds=120)
        finally:
            backend.close()
