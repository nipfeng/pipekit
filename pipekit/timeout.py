"""Timeout support for pipeline steps."""

from __future__ import annotations

import signal
import functools
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class TimeoutError(Exception):
    """Raised when a step exceeds its allowed execution time."""


def _timeout_handler(signum, frame):
    raise TimeoutError("Step exceeded allowed execution time")


@dataclass
class TimeoutPolicy:
    """Enforces a maximum execution time for a callable.

    Attributes:
        seconds: Maximum number of seconds a step may run.
    """

    seconds: float

    def __post_init__(self):
        if self.seconds <= 0:
            raise ValueError(f"seconds must be positive, got {self.seconds}")

    def execute(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run *fn* with the configured timeout.

        Raises:
            TimeoutError: If *fn* does not complete within *seconds*.
        """
        # signal-based timeout only works on Unix; raises on Windows
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.setitimer(signal.ITIMER_REAL, self.seconds)
        try:
            return fn(*args, **kwargs)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

    def __repr__(self) -> str:
        return f"TimeoutPolicy(seconds={self.seconds})"


class TimedStep:
    """Wraps a callable step with a :class:`TimeoutPolicy`.

    Parameters
    ----------
    name:
        Human-readable label for the step.
    fn:
        The transformation function ``fn(data) -> data``.
    policy:
        A :class:`TimeoutPolicy` controlling the maximum run time.
    """

    def __init__(self, name: str, fn: Callable, policy: TimeoutPolicy) -> None:
        self.name = name
        self._fn = fn
        self._policy = policy

    def run(self, data: Any) -> Any:
        """Execute the step, raising :class:`TimeoutError` if too slow."""
        return self._policy.execute(self._fn, data)

    def __repr__(self) -> str:
        return f"TimedStep(name={self.name!r}, policy={self._policy})"
