"""A Step subclass that receives a PipelineContext alongside its input."""

from typing import Any, Callable, Optional

from pipekit.context import PipelineContext
from pipekit.step import Step


class ContextStep(Step):
    """Like :class:`~pipekit.step.Step`, but the wrapped function also
    receives the current :class:`~pipekit.context.PipelineContext`.

    The callable signature must be ``fn(data, ctx) -> Any``.

    Example::

        def enrich(data, ctx):
            ctx.set("row_count", len(data))
            return data

        step = ContextStep("enrich", enrich)
    """

    def __init__(
        self,
        name: str,
        fn: Callable[[Any, PipelineContext], Any],
        description: Optional[str] = None,
    ) -> None:
        super().__init__(name=name, fn=fn, description=description)

    def run(self, data: Any, ctx: Optional[PipelineContext] = None) -> Any:  # type: ignore[override]
        """Execute the step, injecting *ctx* (created if not provided)."""
        if ctx is None:
            ctx = PipelineContext()
        return self.fn(data, ctx)

    def __repr__(self) -> str:  # pragma: no cover
        return f"ContextStep(name={self.name!r})"
