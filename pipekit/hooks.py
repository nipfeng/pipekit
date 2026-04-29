"""Pipeline lifecycle hooks for pipekit."""

from typing import Callable, Optional
from dataclasses import dataclass, field


@dataclass
class PipelineHooks:
    """Container for optional lifecycle callbacks attached to a pipeline.

    Hooks are called at key points during pipeline execution:
      - on_start:      called before any step runs
      - on_step_start: called before each individual step
      - on_step_end:   called after each individual step (even on failure)
      - on_success:    called when the pipeline completes without errors
      - on_failure:    called when a step raises an unhandled exception
      - on_finish:     called after the pipeline finishes, regardless of outcome
    """

    on_start: Optional[Callable[[str], None]] = field(default=None)
    on_step_start: Optional[Callable[[str, str], None]] = field(default=None)
    on_step_end: Optional[Callable[[str, str, bool], None]] = field(default=None)
    on_success: Optional[Callable[[str], None]] = field(default=None)
    on_failure: Optional[Callable[[str, str, Exception], None]] = field(default=None)
    on_finish: Optional[Callable[[str], None]] = field(default=None)

    def fire_start(self, pipeline_name: str) -> None:
        if self.on_start:
            self.on_start(pipeline_name)

    def fire_step_start(self, pipeline_name: str, step_name: str) -> None:
        if self.on_step_start:
            self.on_step_start(pipeline_name, step_name)

    def fire_step_end(self, pipeline_name: str, step_name: str, success: bool) -> None:
        if self.on_step_end:
            self.on_step_end(pipeline_name, step_name, success)

    def fire_success(self, pipeline_name: str) -> None:
        if self.on_success:
            self.on_success(pipeline_name)

    def fire_failure(
        self, pipeline_name: str, step_name: str, exc: Exception
    ) -> None:
        if self.on_failure:
            self.on_failure(pipeline_name, step_name, exc)

    def fire_finish(self, pipeline_name: str) -> None:
        if self.on_finish:
            self.on_finish(pipeline_name)

    def __repr__(self) -> str:  # pragma: no cover
        registered = [
            name
            for name in (
                "on_start",
                "on_step_start",
                "on_step_end",
                "on_success",
                "on_failure",
                "on_finish",
            )
            if getattr(self, name) is not None
        ]
        return f"PipelineHooks(registered={registered})"
