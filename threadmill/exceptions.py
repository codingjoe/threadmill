"""Custom exceptions for the threadmill task framework."""


class MissingExtraError(ImportError):
    """Raised when an optional dependency extra is not installed."""

    def __init__(self, extra: str) -> None:
        self.extra = extra
        super().__init__(f"Optional dependency missing. Install: threadmill[{extra}]")


class AcknowledgementTimeoutError(Exception):
    """Raised when a task's lease has expired before it could be acknowledged."""
