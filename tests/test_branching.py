"""Tests for BranchingPipeline, Branch, and BranchingResult."""

import pytest

from pipekit.branching import Branch, BranchingPipeline, BranchingResult
from pipekit.step import Step


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2


def add_ten(x):
    return x + 10


def boom(x):
    raise ValueError("step exploded")


def make_step(fn):
    return Step(name=fn.__name__, fn=fn)


# ---------------------------------------------------------------------------
# Branch unit tests
# ---------------------------------------------------------------------------

class TestBranch:
    def test_matches_returns_true_when_condition_holds(self):
        b = Branch(name="even", condition=lambda x: x % 2 == 0)
        assert b.matches(4) is True

    def test_matches_returns_false_when_condition_fails(self):
        b = Branch(name="even", condition=lambda x: x % 2 == 0)
        assert b.matches(3) is False

    def test_matches_returns_false_on_exception(self):
        b = Branch(name="bad", condition=lambda x: x["missing"])
        assert b.matches(42) is False

    def test_repr(self):
        b = Branch(name="pos", condition=lambda x: x > 0)
        assert "pos" in repr(b)


# ---------------------------------------------------------------------------
# BranchingPipeline tests
# ---------------------------------------------------------------------------

class TestBranchingPipelineRun:
    def _pipeline(self):
        bp = BranchingPipeline(name="test_bp")
        even_branch = Branch(name="even", condition=lambda x: x % 2 == 0)
        even_branch.add_step(make_step(double))
        odd_branch = Branch(name="odd", condition=lambda x: x % 2 != 0)
        odd_branch.add_step(make_step(add_ten))
        bp.add_branch(even_branch)
        bp.add_branch(odd_branch)
        return bp

    def test_routes_to_even_branch(self):
        result = self._pipeline().run(4)
        assert result.success is True
        assert result.output == 8
        assert result.branch_taken == "even"

    def test_routes_to_odd_branch(self):
        result = self._pipeline().run(3)
        assert result.success is True
        assert result.output == 13
        assert result.branch_taken == "odd"

    def test_no_match_no_default_returns_failure(self):
        bp = BranchingPipeline()
        bp.add_branch(Branch(name="never", condition=lambda x: False))
        result = bp.run(99)
        assert result.success is False
        assert result.branch_taken is None
        assert isinstance(result.error, ValueError)

    def test_default_branch_is_used_when_no_match(self):
        bp = BranchingPipeline()
        bp.add_branch(Branch(name="never", condition=lambda x: False))
        bp.set_default([make_step(double)])
        result = bp.run(5)
        assert result.success is True
        assert result.output == 10
        assert result.branch_taken == "__default__"

    def test_step_failure_propagates(self):
        bp = BranchingPipeline()
        b = Branch(name="boom", condition=lambda x: True)
        b.add_step(make_step(boom))
        bp.add_branch(b)
        result = bp.run(1)
        assert result.success is False
        assert isinstance(result.error, ValueError)

    def test_repr(self):
        bp = BranchingPipeline(name="my_bp")
        assert "my_bp" in repr(bp)
