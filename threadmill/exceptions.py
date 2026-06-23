"""Custom exceptions for the threadmill task framework."""

from __future__ import annotations


class AcknowledgementTimeout(Exception):
    """Raised when a task's lease has expired before it could be acknowledged."""
