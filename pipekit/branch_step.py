"""BranchStep — wraps a BranchingPipeline as a single Step."""

from typing import Any

from pipekit.branching import BranchingPipeline
from pipekit.step import Step


class BranchStep(Step):
    """Embeds a BranchingPipeline as a composable Step.

    Raises RuntimeError if the inner branching pipeline fails.
    """

    def __init__(self, branching_pipeline: BranchingPipeline) -> None:
        self._bp = branching_pipeline
        super().__init__(
            name=branching_pipeline.name,
            fn=self._delegate,
        )

    def _delegate(self, data: Any) -> Any:
        result = self._bp.run(data)
        if not result.success:
            raise RuntimeError(
                f"BranchStep '{self._bp.name}' failed on branch "
                f"'{result.branch_taken}': {result.error}"
            ) from result.error
        return result.output

    def __repr__(self) -> str:
        return f"BranchStep(pipeline={self._bp!r})"
