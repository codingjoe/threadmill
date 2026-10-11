"""Custom exceptions for the threadmill task framework."""


class AcknowledgementTimeoutError(Exception):
    """Raised when a task's lease has expired before it could be acknowledged."""
