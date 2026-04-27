"""Step abstraction for individual pipeline units."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class Step:
    """Represents a single processing step in a pipeline.

    Attributes:
        name: Human-readable identifier for the step.
        fn: Callable that performs the step's transformation.
        description: Optional description of what the step does.
    """

    name: str
    fn: Callable[..., Any]
    description: Optional[str] = field(default=None)

    def run(self, data: Any) -> Any:
        """Execute the step's function with the provided data.

        Args:
            data: Input data passed to the step.

        Returns:
            Transformed data returned by the step's function.

        Raises:
            Exception: Re-raises any exception thrown by the step function.
        """
        return self.fn(data)

    def __repr__(self) -> str:
        return f"Step(name={self.name!r}, description={self.description!r})"
