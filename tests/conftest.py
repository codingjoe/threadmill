import pytest
from django.tasks import default_task_backend


def _flush_keys(backend) -> None:
    """Delete all threadmill-prefixed Redis keys."""
    keys = backend.client.keys("threadmill:*")
    if keys:
        backend.client.delete(*keys)


@pytest.fixture(autouse=True)
def flush_default_backend():
    """Flush all threadmill keys before and after each test."""
    _flush_keys(default_task_backend)
    yield
    _flush_keys(default_task_backend)
