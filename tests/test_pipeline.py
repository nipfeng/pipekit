"""Tests for Pipeline, Step, and PipelineResult."""

import pytest

from pipekit.pipeline import Pipeline
from pipekit.result import PipelineResult
from pipekit.step import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2


def add_one(x):
    return x + 1


def boom(_):
    raise ValueError("intentional failure")


# ---------------------------------------------------------------------------
# Step tests
# ---------------------------------------------------------------------------

class TestStep:
    def test_run_returns_transformed_data(self):
        step = Step(name="double", fn=double)
        assert step.run(5) == 10

    def test_repr_contains_name(self):
        step = Step(name="my_step", fn=double, description="doubles it")
        assert "my_step" in repr(step)

    def test_run_propagates_exception(self):
        step = Step(name="broken", fn=boom)
        with pytest.raises(ValueError, match="intentional failure"):
            step.run(None)


# ---------------------------------------------------------------------------
# Pipeline tests
# ---------------------------------------------------------------------------

class TestPipeline:
    def test_add_step_returns_self_for_chaining(self):
        pipeline = Pipeline(name="chain_test")
        result = pipeline.add_step(Step("s1", double))
        assert result is pipeline

    def test_steps_property_returns_copy(self):
        pipeline = Pipeline(name="p")
        pipeline.add_step(Step("s1", double))
        steps = pipeline.steps
        steps.clear()  # mutating the copy should not affect the pipeline
        assert len(pipeline.steps) == 1

    def test_add_step_rejects_non_step(self):
        pipeline = Pipeline(name="p")
        with pytest.raises(TypeError):
            pipeline.add_step(lambda x: x)  # type: ignore[arg-type]

    def test_run_chains_steps(self):
        pipeline = (
            Pipeline(name="math")
            .add_step(Step("double", double))
            .add_step(Step("add_one", add_one))
        )
        result = pipeline.run(3)  # (3 * 2) + 1 = 7
        assert result.success is True
        assert result.output == 7
        assert len(result.steps) == 2

    def test_run_stops_on_failure(self):
        pipeline = (
            Pipeline(name="fail_test")
            .add_step(Step("double", double))
            .add_step(Step("boom", boom))
            .add_step(Step("add_one", add_one))
        )
        result = pipeline.run(2)
        assert result.success is False
        assert result.output is None
        assert result.failed_step == "boom"
        # add_one should never have run
        assert len(result.steps) == 2

    def test_run_empty_pipeline(self):
        pipeline = Pipeline(name="empty")
        result = pipeline.run(42)
        assert result.success is True
        assert result.output == 42


# ---------------------------------------------------------------------------
# PipelineResult tests
# ---------------------------------------------------------------------------

class TestPipelineResult:
    def test_failed_step_returns_none_on_success(self):
        r = PipelineResult(pipeline="p", output=1, steps=[{"step": "s1", "status": "success"}])
        assert r.failed_step is None

    def test_summary_contains_pipeline_name(self):
        r = PipelineResult(pipeline="my_pipe", output=None, success=True, steps=[])
        assert "my_pipe" in r.summary()

    def test_summary_shows_failure(self):
        steps = [
            {"step": "extract", "status": "success"},
            {"step": "transform", "status": "failed", "error": "oops"},
        ]
        r = PipelineResult(pipeline="etl", output=None, success=False, steps=steps)
        summary = r.summary()
        assert "FAILED" in summary
        assert "oops" in summary
