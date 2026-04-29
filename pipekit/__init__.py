"""pipekit — A lightweight Python library for composing and monitoring ETL pipelines."""

from pipekit.pipeline import Pipeline
from pipekit.step import Step

__version__ = "0.1.0"
__all__ = ["Pipeline", "Step", "get_version", "version_info"]


def get_version() -> str:
    """Return the current version of pipekit.

    Returns:
        str: The version string in PEP 440 format (e.g. ``"0.1.0"``).

    Example::

        >>> import pipekit
        >>> pipekit.get_version()
        '0.1.0'
    """
    return __version__


def version_info() -> tuple[int, ...]:
    """Return the current version of pipekit as a tuple of integers.

    Returns:
        tuple[int, ...]: The version components as integers (e.g. ``(0, 1, 0)``).

    Example::

        >>> import pipekit
        >>> pipekit.version_info()
        (0, 1, 0)
    """
    return tuple(int(part) for part in __version__.split("."))
