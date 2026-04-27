"""Pipeline class for composing and executing a sequence of Steps."""

from __future__ import annotations

from typing import Any, List, Optional

from pipekit.step import Step
from pipekit.result import PipelineResult


class Pipeline:
    """Composes multiple Steps and executes them in order.

    Example::

        pipeline = Pipeline(name="my_etl")
        pipeline.add_step(Step("extract", extract_fn))
        pipeline.add_step(Step("transform", transform_fn))
        pipeline.add_step(Step("load", load_fn))
        result = pipeline.run(raw_data)
    """

    def __init__(self, name: str, description: Optional[str] = None) -> None:
        self.name = name
        self.description = description
        self._steps: List[Step] = []

    def add_step(self, step: Step) -> "Pipeline":
        """Append a step to the pipeline. Returns self for chaining."""
        if not isinstance(step, Step):
            raise TypeError(f"Expected a Step instance, got {type(step).__name__}")
        self._steps.append(step)
        return self

    @property
    def steps(self) -> List[Step]:
        """Read-only view of the registered steps."""
        return list(self._steps)

    def run(self, data: Any) -> PipelineResult:
        """Execute all steps in order, passing output of each step as input to the next.

        Args:
            data: Initial input data for the first step.

        Returns:
            A PipelineResult containing the final output and per-step metadata.
        """
        completed: List[dict] = []
        current = data

        for step in self._steps:
            try:
                current = step.run(current)
                completed.append({"step": step.name, "status": "success"})
            except Exception as exc:  # noqa: BLE001
                completed.append({"step": step.name, "status": "failed", "error": str(exc)})
                return PipelineResult(
                    pipeline=self.name,
                    output=None,
                    steps=completed,
                    success=False,
                )

        return PipelineResult(
            pipeline=self.name,
            output=current,
            steps=completed,
            success=True,
        )

    def __repr__(self) -> str:
        return f"Pipeline(name={self.name!r}, steps={len(self._steps)})"
