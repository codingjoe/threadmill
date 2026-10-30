import logging

import pytest
from django.tasks import default_task_backend


def _flush_keys(backend) -> None:
    """Delete all threadmill-prefixed Redis keys."""
    keys = backend.client.keys("threadmill:*")
    if keys:
        backend.client.delete(*keys)


@pytest.fixture(autouse=True)
def restore_root_logger():
    """Restore root logger handlers and level after each test."""
    root_logger = logging.getLogger()
    handlers = root_logger.handlers[:]
    level = root_logger.level
    yield
    root_logger.handlers[:] = handlers
    root_logger.setLevel(level)


@pytest.fixture(autouse=True)
def flush_default_backend():
    """Flush all threadmill keys and reset async client before and after each test."""
    _flush_keys(default_task_backend)
    default_task_backend._async_client = None
    yield
    _flush_keys(default_task_backend)
    default_task_backend._async_client = None


@pytest.fixture(autouse=True)
def isolate_metrics_environment(monkeypatch):
    """Keep unrelated tests from starting an exporter or reading host settings."""
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    for name in (
        "OTEL_SDK_DISABLED",
        "OTEL_METRIC_EXPORT_INTERVAL",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_SERVICE_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
