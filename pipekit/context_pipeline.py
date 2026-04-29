"""A Pipeline variant that threads a PipelineContext through every step."""

from typing import Any, Dict, Optional

from pipekit.context import PipelineContext
from pipekit.context_step import ContextStep
from pipekit.pipeline import Pipeline
from pipekit.result import PipelineResult
from pipekit.step import Step


class ContextPipeline(Pipeline):
    """Executes steps in sequence, passing a shared :class:`PipelineContext`
    to every :class:`ContextStep`.  Plain :class:`Step` instances are still
    supported and receive only *data*.

    The final :class:`PipelineResult` exposes the context via
    ``result.context``.
    """

    def run(
        self,
        data: Any,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> "ContextPipelineResult":
        ctx = PipelineContext(initial_context)
        current = data

        for step in self._steps:
            try:
                if isinstance(step, ContextStep):
                    current = step.run(current, ctx)
                else:
                    current = step.run(current)
            except Exception as exc:  # noqa: BLE001
                return ContextPipelineResult(
                    data=None,
                    failed_step=step.name,
                    error=exc,
                    context=ctx,
                )

        return ContextPipelineResult(data=current, context=ctx)


class ContextPipelineResult(PipelineResult):
    """Extends :class:`PipelineResult` with a :attr:`context` attribute."""

    def __init__(
        self,
        data: Any,
        context: PipelineContext,
        failed_step: Optional[str] = None,
        error: Optional[Exception] = None,
    ) -> None:
        super().__init__(data=data, failed_step=failed_step, error=error)
        self.context = context

    def __repr__(self) -> str:  # pragma: no cover
        status = "ok" if self.success else f"failed at '{self.failed_step}'"
        return f"ContextPipelineResult(status={status}, context_keys={list(self.context.keys())})"
