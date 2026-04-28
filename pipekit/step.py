"""Pipeline step definition."""

from typing import Any, Callable, Optional
from pipekit.retry import RetryPolicy


class Step:
    """A single unit of work in a pipeline.

    Parameters
    ----------
    name:
        Human-readable identifier shown in logs and reports.
    func:
        Callable that receives the previous step's output and returns new data.
    retry_policy:
        Optional :class:`RetryPolicy` applied when *func* raises.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[Any], Any],
        retry_policy: Optional[RetryPolicy] = None,
    ):
        self.name = name
        self.func = func
        self.retry_policy = retry_policy

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, data: Any) -> Any:
        """Execute this step, honouring any attached retry policy."""
        if self.retry_policy is not None:
            return self.retry_policy.execute(self.func, data)
        return self.func(data)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:  # pragma: no cover
        return f"Step(name={self.name!r}, retry_policy={self.retry_policy!r})"
