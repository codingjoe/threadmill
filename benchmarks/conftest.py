"""Fixtures shared by the benchmark suite."""

import logging

import pytest


@pytest.fixture(autouse=True)
def restore_root_logger():
    """Restore root logger handlers and level after each benchmark.

    Running the threadmill worker reconfigures the root logger of this process,
    and its JSON handler would otherwise format every later record.
    """
    root_logger = logging.getLogger()
    handlers = root_logger.handlers[:]
    level = root_logger.level
    yield
    root_logger.handlers[:] = handlers
    root_logger.setLevel(level)
