"""Pipeline monitoring and metrics collection for pipekit.

Provides the PipelineMonitor class which tracks step execution times,
successes, failures, and emits structured log output during pipeline runs.
"""

import time
import logging
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class StepMetrics:
    """Metrics captured for a single step execution."""

    step_name: str
    start_time: float
    end_time: Optional[float] = None
    success: Optional[bool] = None
    error: Optional[Exception] = None

    @property
    def duration(self) -> Optional[float]:
        """Return elapsed time in seconds, or None if step hasn't finished."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    def as_dict(self) -> Dict[str, Any]:
        """Serialize metrics to a plain dictionary."""
        return {
            "step": self.step_name,
            "duration_s": round(self.duration, 6) if self.duration is not None else None,
            "success": self.success,
            "error": str(self.error) if self.error else None,
        }


class PipelineMonitor:
    """Collects and reports metrics for a pipeline run.

    Attach a monitor to a Pipeline to automatically track how long each
    step takes and whether it succeeds or fails.

    Example::

        monitor = PipelineMonitor()
        pipeline = Pipeline(monitor=monitor)
        pipeline.add_step(my_step)
        pipeline.run(data)
        print(monitor.report())
    """

    def __init__(self, verbose: bool = False) -> None:
        """Initialise the monitor.

        Args:
            verbose: When True, log step start/end events at INFO level.
        """
        self.verbose = verbose
        self._metrics: List[StepMetrics] = []

    # ------------------------------------------------------------------
    # Lifecycle hooks called by Pipeline.run
    # ------------------------------------------------------------------

    def on_step_start(self, step_name: str) -> StepMetrics:
        """Record that a step has begun and return its metrics object."""
        metrics = StepMetrics(step_name=step_name, start_time=time.perf_counter())
        self._metrics.append(metrics)
        if self.verbose:
            logger.info("[pipekit] starting step '%s'", step_name)
        return metrics

    def on_step_success(self, metrics: StepMetrics) -> None:
        """Mark a step as successfully completed."""
        metrics.end_time = time.perf_counter()
        metrics.success = True
        if self.verbose:
            logger.info(
                "[pipekit] step '%s' completed in %.4fs",
                metrics.step_name,
                metrics.duration,
            )

    def on_step_failure(self, metrics: StepMetrics, error: Exception) -> None:
        """Mark a step as failed and capture the exception."""
        metrics.end_time = time.perf_counter()
        metrics.success = False
        metrics.error = error
        logger.error(
            "[pipekit] step '%s' failed after %.4fs: %s",
            metrics.step_name,
            metrics.duration,
            error,
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def all_metrics(self) -> List[Dict[str, Any]]:
        """Return all collected step metrics as a list of dicts."""
        return [m.as_dict() for m in self._metrics]

    def report(self) -> str:
        """Return a human-readable summary of the pipeline run."""
        if not self._metrics:
            return "No steps recorded."

        lines = ["Pipeline run summary:", "-" * 40]
        total_duration = 0.0
        for m in self._metrics:
            status = "OK" if m.success else "FAILED"
            dur = f"{m.duration:.4f}s" if m.duration is not None else "n/a"
            lines.append(f"  [{status}] {m.step_name:<30} {dur}")
            if m.duration:
                total_duration += m.duration
        lines.append("-" * 40)
        lines.append(f"  Total time: {total_duration:.4f}s")
        return "\n".join(lines)

    def reset(self) -> None:
        """Clear all recorded metrics (useful between runs)."""
        self._metrics.clear()
