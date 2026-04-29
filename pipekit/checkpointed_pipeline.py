"""A Pipeline variant that skips already-completed steps using checkpoints."""

from __future__ import annotations

import uuid
from typing import Any, List, Optional

from pipekit.checkpoint import CheckpointStore
from pipekit.pipeline import Pipeline
from pipekit.result import PipelineResult
from pipekit.step import Step


class CheckpointedPipeline:
    """Runs a pipeline and checkpoints each step's output.

    On a subsequent run with the same *run_id* any step whose output was
    already saved is skipped and its persisted result is used instead.
    """

    def __init__(
        self,
        steps: Optional[List[Step]] = None,
        *,
        store: Optional[CheckpointStore] = None,
        run_id: Optional[str] = None,
    ) -> None:
        self._pipeline = Pipeline(steps or [])
        self.store = store or CheckpointStore()
        self.run_id = run_id or str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def add_step(self, step: Step) -> "CheckpointedPipeline":
        self._pipeline.add_step(step)
        return self

    def reset(self) -> None:
        """Clear all checkpoints for the current *run_id*."""
        self.store.clear(self.run_id)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, data: Any) -> "CheckpointedPipelineResult":
        """Execute the pipeline, skipping steps that are already checkpointed."""
        current = data
        skipped: List[str] = []
        executed: List[str] = []
        failed_step: Optional[str] = None
        error: Optional[Exception] = None

        for step in self._pipeline.steps:
            name = step.name
            if self.store.has(self.run_id, name):
                current = self.store.load(self.run_id, name)
                skipped.append(name)
                continue
            try:
                current = step.run(current)
                self.store.save(self.run_id, name, current)
                executed.append(name)
            except Exception as exc:  # noqa: BLE001
                failed_step = name
                error = exc
                break

        return CheckpointedPipelineResult(
            output=current if error is None else None,
            skipped=skipped,
            executed=executed,
            failed_step=failed_step,
            error=error,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CheckpointedPipeline(run_id={self.run_id!r}, "
            f"steps={len(self._pipeline.steps)})"
        )


class CheckpointedPipelineResult:
    """Result returned by :class:`CheckpointedPipeline`."""

    def __init__(
        self,
        output: Any,
        skipped: List[str],
        executed: List[str],
        failed_step: Optional[str],
        error: Optional[Exception],
    ) -> None:
        self.output = output
        self.skipped = skipped
        self.executed = executed
        self.failed_step = failed_step
        self.error = error

    @property
    def success(self) -> bool:
        return self.error is None

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CheckpointedPipelineResult(success={self.success}, "
            f"executed={self.executed}, skipped={self.skipped})"
        )
