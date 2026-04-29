"""Tests for FanOutPipeline and FanOutResult."""

import pytest

from pipekit.fan_out import FanOutPipeline, FanOutResult
from pipekit.pipeline import Pipeline
from pipekit.step import Step


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2


def add_ten(x):
    return x + 10


def to_str(x):
    return str(x)


def boom(x):
    raise RuntimeError("boom")


def make_pipeline(*fns):
    p = Pipeline()
    for fn in fns:
        p.add_step(Step(fn))
    return p


# ---------------------------------------------------------------------------
# FanOutResult
# ---------------------------------------------------------------------------

class TestFanOutResult:
    def test_all_succeeded_when_no_errors(self):
        r = FanOutResult(results={"a": 1, "b": 2})
        assert r.all_succeeded is True

    def test_not_all_succeeded_when_errors_present(self):
        r = FanOutResult(results={"a": 1}, errors={"b": RuntimeError()})
        assert r.all_succeeded is False

    def test_succeeded_and_failed_lists(self):
        r = FanOutResult(results={"a": 1}, errors={"b": RuntimeError()})
        assert "a" in r.succeeded
        assert "b" in r.failed


# ---------------------------------------------------------------------------
# FanOutPipeline
# ---------------------------------------------------------------------------

class TestFanOutPipelineSetup:
    def test_add_branch_returns_self(self):
        fop = FanOutPipeline()
        returned = fop.add_branch("x", make_pipeline(double))
        assert returned is fop

    def test_duplicate_branch_name_raises(self):
        fop = FanOutPipeline()
        fop.add_branch("x", make_pipeline(double))
        with pytest.raises(ValueError, match="already registered"):
            fop.add_branch("x", make_pipeline(add_ten))

    def test_branch_names_property(self):
        fop = FanOutPipeline()
        fop.add_branch("alpha", make_pipeline(double))
        fop.add_branch("beta", make_pipeline(add_ten))
        assert set(fop.branch_names) == {"alpha", "beta"}


class TestFanOutPipelineRun:
    def test_empty_fan_out_returns_empty_result(self):
        fop = FanOutPipeline()
        result = fop.run(5)
        assert result.results == {}
        assert result.errors == {}
        assert result.all_succeeded

    def test_all_branches_receive_same_input(self):
        fop = FanOutPipeline()
        fop.add_branch("double", make_pipeline(double))
        fop.add_branch("add_ten", make_pipeline(add_ten))
        fop.add_branch("str", make_pipeline(to_str))
        result = fop.run(5)
        assert result.results["double"] == 10
        assert result.results["add_ten"] == 15
        assert result.results["str"] == "5"
        assert result.all_succeeded

    def test_failing_branch_captured_in_errors(self):
        fop = FanOutPipeline()
        fop.add_branch("ok", make_pipeline(double))
        fop.add_branch("bad", make_pipeline(boom))
        result = fop.run(3)
        assert "ok" in result.succeeded
        assert "bad" in result.failed
        assert isinstance(result.errors["bad"], RuntimeError)

    def test_max_workers_respected(self):
        """Smoke-test: explicit max_workers still produces correct output."""
        fop = FanOutPipeline(max_workers=1)
        fop.add_branch("a", make_pipeline(double))
        fop.add_branch("b", make_pipeline(add_ten))
        result = fop.run(4)
        assert result.results["a"] == 8
        assert result.results["b"] == 14
