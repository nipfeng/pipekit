"""Tests for BranchStep."""

import pytest

from pipekit.branching import Branch, BranchingPipeline
from pipekit.branch_step import BranchStep
from pipekit.pipeline import Pipeline
from pipekit.step import Step


def triple(x):
    return x * 3


def negate(x):
    return -x


def boom(x):
    raise RuntimeError("inner boom")


def make_step(fn):
    return Step(name=fn.__name__, fn=fn)


def make_branching_pipeline():
    bp = BranchingPipeline(name="sign_router")
    pos = Branch(name="positive", condition=lambda x: x >= 0)
    pos.add_step(make_step(triple))
    neg = Branch(name="negative", condition=lambda x: x < 0)
    neg.add_step(make_step(negate))
    bp.add_branch(pos)
    bp.add_branch(neg)
    return bp


class TestBranchStep:
    def test_positive_path(self):
        bs = BranchStep(make_branching_pipeline())
        assert bs.run(4) == 12

    def test_negative_path(self):
        bs = BranchStep(make_branching_pipeline())
        assert bs.run(-3) == 3

    def test_raises_runtime_error_on_inner_failure(self):
        bp = BranchingPipeline(name="bad_pipe")
        b = Branch(name="always", condition=lambda x: True)
        b.add_step(make_step(boom))
        bp.add_branch(b)
        bs = BranchStep(bp)
        with pytest.raises(RuntimeError, match="bad_pipe"):
            bs.run(1)

    def test_raises_when_no_branch_matches(self):
        bp = BranchingPipeline(name="empty")
        bs = BranchStep(bp)
        with pytest.raises(RuntimeError):
            bs.run(42)

    def test_composable_inside_pipeline(self):
        bs = BranchStep(make_branching_pipeline())
        p = Pipeline()
        p.add_step(bs)
        result = p.run(5)
        assert result.output == 15

    def test_repr_contains_pipeline_name(self):
        bs = BranchStep(make_branching_pipeline())
        assert "sign_router" in repr(bs)
