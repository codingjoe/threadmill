"""A queue agnostic worker for Django's task framework."""

from . import _version

__version__ = _version.version
VERSION = _version.version_tuple


__all__ = ["VERSION", "__version__"]
