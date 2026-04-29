"""pipekit — A lightweight Python library for composing and monitoring ETL pipelines."""

version_info = (0, 7, 0)


def get_version() -> str:
    """Return the current pipekit version as a string."""
    return ".".join(str(v) for v in version_info)


__all__ = [
    "get_version",
    "version_info",
    # core
    "Step",
    "Pipeline",
    "PipelineResult",
    # monitoring
    "PipelineMonitor",
    "StepMetrics",
    # retry
    "RetryPolicy",
    # hooks
    "PipelineHooks",
    # context
    "PipelineContext",
    "ContextStep",
    "ContextPipeline",
    "ContextPipelineResult",
    # throttle
    "ThrottlePolicy",
    "ThrottledStep",
    # checkpoint
    "CheckpointStore",
    "CheckpointedPipeline",
    # branching
    "Branch",
    "BranchingPipeline",
    "BranchingResult",
    "BranchStep",
]

from pipekit.step import Step
from pipekit.pipeline import Pipeline
from pipekit.result import PipelineResult
from pipekit.monitor import PipelineMonitor, StepMetrics
from pipekit.retry import RetryPolicy
from pipekit.hooks import PipelineHooks
from pipekit.context import PipelineContext
from pipekit.context_step import ContextStep
from pipekit.context_pipeline import ContextPipeline, ContextPipelineResult
from pipekit.throttle import ThrottlePolicy
from pipekit.throttled_step import ThrottledStep
from pipekit.checkpoint import CheckpointStore
from pipekit.checkpointed_pipeline import CheckpointedPipeline
from pipekit.branching import Branch, BranchingPipeline, BranchingResult
from pipekit.branch_step import BranchStep
