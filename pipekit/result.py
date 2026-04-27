"""Data container for pipeline execution results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional


@dataclass
class PipelineResult:
    """Holds the outcome of a pipeline run.

    Attributes:
        pipeline: Name of the pipeline that produced this result.
        output: Final data produced by the last successful step.
        steps: List of per-step status dictionaries.
        success: True if all steps completed without error.
    """

    pipeline: str
    output: Any
    steps: List[dict] = field(default_factory=list)
    success: bool = True

    @property
    def failed_step(self) -> Optional[str]:
        """Return the name of the first failed step, or None if all succeeded."""
        for entry in self.steps:
            if entry.get("status") == "failed":
                return entry["step"]
        return None

    def summary(self) -> str:
        """Return a human-readable summary of the pipeline run."""
        status = "SUCCESS" if self.success else f"FAILED at step '{self.failed_step}'"
        lines = [f"Pipeline '{self.pipeline}' — {status}"]
        for entry in self.steps:
            icon = "✓" if entry["status"] == "success" else "✗"
            lines.append(f"  {icon} {entry['step']}")
            if "error" in entry:
                lines.append(f"      error: {entry['error']}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"PipelineResult(pipeline={self.pipeline!r}, "
            f"success={self.success}, steps={len(self.steps)})"
        )
