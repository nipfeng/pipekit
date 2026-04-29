"""pipekit – lightweight ETL pipeline composition and monitoring."""

from __future__ import annotations

from typing import Tuple

__all__ = [
    "get_version",
    "version_info",
    "Step",
    "Pipeline",
    "PipelineResult",
    "PipelineMonitor",
    "RetryPolicy",
    "PipelineHooks",
    "PipelineContext",
    "ContextStep",
    "ContextPipeline",
    "ThrottlePolicy",
    "ThrottledStep",
    "CheckpointStore",
    "CheckpointedPipeline",
]

_VERSION: Tuple[int, int, int] = (0, 8, 0)


def version_info() -> Tuple[int, int, int]:
    """Return the version as a ``(major, minor, patch)`` tuple."""
    return _VERSION


def get_version() -> str:
    """Return the version string, e.g. ``'0.8.0'``."""
    return ".".join(str(part) for part in _VERSION)


# Lazy imports kept here so the public API is available at the top level.
from pipekit.step import Step  # noqa: E402
from pipekit.pipeline import Pipeline  # noqa: E402
from pipekit.result import PipelineResult  # noqa: E402
from pipekit.monitor import PipelineMonitor  # noqa: E402
from pipekit.retry import RetryPolicy  # noqa: E402
from pipekit.hooks import PipelineHooks  # noqa: E402
from pipekit.context import PipelineContext  # noqa: E402
from pipekit.context_step import ContextStep  # noqa: E402
from pipekit.context_pipeline import ContextPipeline  # noqa: E402
from pipekit.throttle import ThrottlePolicy  # noqa: E402
from pipekit.throttled_step import ThrottledStep  # noqa: E402
from pipekit.checkpoint import CheckpointStore  # noqa: E402
from pipekit.checkpointed_pipeline import CheckpointedPipeline  # noqa: E402
