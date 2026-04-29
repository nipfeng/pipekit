"""Tests for ContextPipeline and ContextStep integration."""

import pytest

from pipekit.context import PipelineContext
from pipekit.context_pipeline import ContextPipeline, ContextPipelineResult
from pipekit.context_step import ContextStep
from pipekit.step import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(data):
    return data * 2


def record_length(data, ctx: PipelineContext):
    ctx.set("length", len(data))
    return data


def tag_run(data, ctx: PipelineContext):
    ctx.tag("stage", "tagged")
    return data


def boom(data, ctx: PipelineContext):
    raise ValueError("context step exploded")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestContextPipelineRun:
    def test_plain_step_still_works(self):
        pipeline = ContextPipeline()
        pipeline.add_step(Step("double", double))
        result = pipeline.run(3)
        assert result.success
        assert result.data == 6

    def test_context_step_receives_context(self):
        pipeline = ContextPipeline()
        pipeline.add_step(ContextStep("record", record_length))
        result = pipeline.run("hello")
        assert result.success
        assert result.context.get("length") == 5

    def test_context_shared_across_steps(self):
        pipeline = ContextPipeline()
        pipeline.add_step(ContextStep("record", record_length))
        pipeline.add_step(ContextStep("tag", tag_run))
        result = pipeline.run("hi")
        assert result.context.get("length") == 2
        assert result.context.get_tag("stage") == "tagged"

    def test_initial_context_values_available(self):
        def read_initial(data, ctx):
            ctx.set("seen", ctx.require("seed"))
            return data

        pipeline = ContextPipeline()
        pipeline.add_step(ContextStep("read", read_initial))
        result = pipeline.run(None, initial_context={"seed": 42})
        assert result.context.get("seen") == 42

    def test_failed_context_step_returns_failure_result(self):
        pipeline = ContextPipeline()
        pipeline.add_step(ContextStep("boom", boom))
        result = pipeline.run("data")
        assert not result.success
        assert result.failed_step == "boom"
        assert isinstance(result.error, ValueError)

    def test_result_has_context_on_failure(self):
        pipeline = ContextPipeline()
        pipeline.add_step(ContextStep("record", record_length))
        pipeline.add_step(ContextStep("boom", boom))
        result = pipeline.run("abc")
        assert not result.success
        # context written before the failure is still accessible
        assert result.context.get("length") == 3

    def test_result_is_context_pipeline_result_instance(self):
        pipeline = ContextPipeline()
        result = pipeline.run("x")
        assert isinstance(result, ContextPipelineResult)
