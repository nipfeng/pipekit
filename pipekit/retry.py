"""Retry policy support for pipeline steps."""

import time
import logging
from typing import Callable, Optional, Type, Tuple

logger = logging.getLogger(__name__)


class RetryPolicy:
    """Defines retry behaviour for a step that raises an exception."""

    def __init__(
        self,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        on_retry: Optional[Callable[[int, Exception], None]] = None,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if delay < 0:
            raise ValueError("delay must be non-negative")
        if backoff < 1:
            raise ValueError("backoff must be >= 1")

        self.max_attempts = max_attempts
        self.delay = delay
        self.backoff = backoff
        self.exceptions = exceptions
        self.on_retry = on_retry

    def execute(self, func: Callable, *args, **kwargs):
        """Execute *func* with retry logic, returning its result on success."""
        attempt = 0
        current_delay = self.delay

        while True:
            try:
                return func(*args, **kwargs)
            except self.exceptions as exc:
                attempt += 1
                if attempt >= self.max_attempts:
                    logger.error(
                        "Step failed after %d attempt(s): %s", attempt, exc
                    )
                    raise

                logger.warning(
                    "Attempt %d/%d failed (%s). Retrying in %.2fs…",
                    attempt,
                    self.max_attempts,
                    exc,
                    current_delay,
                )

                if self.on_retry:
                    self.on_retry(attempt, exc)

                time.sleep(current_delay)
                current_delay *= self.backoff

    def __repr__(self) -> str:
        return (
            f"RetryPolicy(max_attempts={self.max_attempts}, "
            f"delay={self.delay}, backoff={self.backoff})"
        )
