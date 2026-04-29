"""Fan-out pipeline: run multiple independent pipelines on the same input in parallel."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pipekit.pipeline import Pipeline


@dataclass
class FanOutResult:
    """Holds the per-branch results of a FanOutPipeline run."""

    results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, Exception] = field(default_factory=dict)

    @property
    def succeeded(self) -> List[str]:
        return list(self.results.keys())

    @property
    def failed(self) -> List[str]:
        return list(self.errors.keys())

    @property
    def all_succeeded(self) -> bool:
        return len(self.errors) == 0

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"FanOutResult(succeeded={self.succeeded}, failed={self.failed})"
        )


class FanOutPipeline:
    """Run multiple named pipelines on the same input concurrently.

    Parameters
    ----------
    max_workers:
        Maximum number of threads used for parallel execution.
        Defaults to the number of registered branches.
    """

    def __init__(self, max_workers: Optional[int] = None) -> None:
        self._branches: Dict[str, Pipeline] = {}
        self._max_workers = max_workers

    def add_branch(self, name: str, pipeline: Pipeline) -> "FanOutPipeline":
        """Register a named pipeline branch."""
        if name in self._branches:
            raise ValueError(f"Branch '{name}' is already registered.")
        self._branches[name] = pipeline
        return self

    @property
    def branch_names(self) -> List[str]:
        return list(self._branches.keys())

    def run(self, data: Any) -> FanOutResult:
        """Execute all branches concurrently with *data* as input."""
        if not self._branches:
            return FanOutResult()

        workers = self._max_workers or len(self._branches)
        result = FanOutResult()

        def _run_branch(name: str, pipeline: Pipeline):
            return name, pipeline.run(data)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_run_branch, name, pipeline): name
                for name, pipeline in self._branches.items()
            }
            for future in as_completed(futures):
                try:
                    name, output = future.result()
                    result.results[name] = output
                except Exception as exc:  # noqa: BLE001
                    name = futures[future]
                    result.errors[name] = exc

        return result

    def __repr__(self) -> str:  # pragma: no cover
        return f"FanOutPipeline(branches={self.branch_names})"
