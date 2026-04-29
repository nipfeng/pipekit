"""Throttle support for pipeline steps — rate-limiting and concurrency control."""

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Any, Optional


@dataclass
class ThrottlePolicy:
    """Controls the rate at which a step is allowed to execute.

    Args:
        min_interval: Minimum seconds between consecutive executions.
        max_calls: Maximum number of calls allowed within the window.
        window: Time window in seconds for max_calls enforcement.
    """

    min_interval: float = 0.0
    max_calls: Optional[int] = None
    window: float = 1.0

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _last_called: float = field(default=0.0, init=False, repr=False)
    _call_times: list = field(default_factory=list, init=False, repr=False)

    def __post_init__(self):
        if self.min_interval < 0:
            raise ValueError("min_interval must be >= 0")
        if self.max_calls is not None and self.max_calls < 1:
            raise ValueError("max_calls must be >= 1")
        if self.window <= 0:
            raise ValueError("window must be > 0")

    def execute(self, fn: Callable, *args, **kwargs) -> Any:
        """Execute fn, blocking if necessary to respect the throttle policy."""
        with self._lock:
            now = time.monotonic()

            # Enforce min_interval
            if self.min_interval > 0:
                elapsed = now - self._last_called
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
                    now = time.monotonic()

            # Enforce max_calls within window
            if self.max_calls is not None:
                self._call_times = [
                    t for t in self._call_times if now - t < self.window
                ]
                if len(self._call_times) >= self.max_calls:
                    oldest = self._call_times[0]
                    wait = self.window - (now - oldest)
                    if wait > 0:
                        time.sleep(wait)
                        now = time.monotonic()
                    self._call_times = [
                        t for t in self._call_times if now - t < self.window
                    ]

                self._call_times.append(now)

            self._last_called = time.monotonic()

        return fn(*args, **kwargs)

    def __repr__(self) -> str:
        return (
            f"ThrottlePolicy(min_interval={self.min_interval}, "
            f"max_calls={self.max_calls}, window={self.window})"
        )
