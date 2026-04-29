"""Branching support for conditional pipeline execution."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pipekit.step import Step
from pipekit.result import PipelineResult


@dataclass
class Branch:
    """A named branch with a condition and a list of steps."""

    name: str
    condition: Callable[[Any], bool]
    steps: List[Step] = field(default_factory=list)

    def add_step(self, step: Step) -> "Branch":
        self.steps.append(step)
        return self

    def matches(self, data: Any) -> bool:
        try:
            return bool(self.condition(data))
        except Exception:
            return False

    def __repr__(self) -> str:
        return f"Branch(name={self.name!r}, steps={len(self.steps)})"


class BranchingPipeline:
    """Pipeline that routes data through one of several named branches."""

    def __init__(self, name: str = "branching_pipeline") -> None:
        self.name = name
        self._branches: List[Branch] = []
        self._default: Optional[List[Step]] = None

    def add_branch(self, branch: Branch) -> "BranchingPipeline":
        self._branches.append(branch)
        return self

    def set_default(self, steps: List[Step]) -> "BranchingPipeline":
        self._default = steps
        return self

    def run(self, data: Any) -> "BranchingResult":
        matched_branch: Optional[str] = None
        steps_to_run: Optional[List[Step]] = None

        for branch in self._branches:
            if branch.matches(data):
                matched_branch = branch.name
                steps_to_run = branch.steps
                break

        if steps_to_run is None:
            if self._default is not None:
                matched_branch = "__default__"
                steps_to_run = self._default
            else:
                return BranchingResult(
                    output=data,
                    branch_taken=None,
                    success=False,
                    error=ValueError("No branch matched and no default is set."),
                )

        current = data
        for step in steps_to_run:
            try:
                current = step.run(current)
            except Exception as exc:
                return BranchingResult(
                    output=current,
                    branch_taken=matched_branch,
                    success=False,
                    error=exc,
                )

        return BranchingResult(output=current, branch_taken=matched_branch, success=True)

    def __repr__(self) -> str:
        return f"BranchingPipeline(name={self.name!r}, branches={len(self._branches)})"


@dataclass
class BranchingResult:
    """Result of a BranchingPipeline run."""

    output: Any
    branch_taken: Optional[str]
    success: bool
    error: Optional[Exception] = None

    def __repr__(self) -> str:
        status = "ok" if self.success else f"failed: {self.error}"
        return f"BranchingResult(branch={self.branch_taken!r}, status={status})"
