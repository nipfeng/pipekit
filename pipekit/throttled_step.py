"""A Step wrapper that applies a ThrottlePolicy before each execution."""

from typing import Callable, Any, Optional
from pipekit.step import Step
from pipekit.throttle import ThrottlePolicy


class ThrottledStep(Step):
    """Wraps a callable step with a ThrottlePolicy.

    Example::

        policy = ThrottlePolicy(min_interval=0.5)
        step = ThrottledStep("slow_api", call_api, throttle=policy)

    Args:
        name: Human-readable step name.
        fn: The callable to execute.
        throttle: A ThrottlePolicy instance controlling execution rate.
    """

    def __init__(self, name: str, fn: Callable, throttle: Optional[ThrottlePolicy] = None):
        super().__init__(name, fn)
        self.throttle = throttle or ThrottlePolicy()

    def run(self, data: Any) -> Any:
        """Execute the step function through the throttle policy."""
        return self.throttle.execute(self.fn, data)

    def __repr__(self) -> str:
        return f"ThrottledStep(name={self.name!r}, throttle={self.throttle!r})"
