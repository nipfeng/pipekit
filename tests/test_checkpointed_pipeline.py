"""Tests for CheckpointedPipeline."""

import pytest

from pipekit.checkpoint import CheckpointStore
from pipekit.checkpointed_pipeline import CheckpointedPipeline
from pipekit.step import Step


def double(x):
    return x * 2


def add_ten(x):
    return x + 10


def boom(x):
    raise ValueError("pipeline exploded")


@pytest.fixture()
def store(tmp_path):
    return CheckpointStore(directory=str(tmp_path / "ckpts"))


class TestCheckpointedPipelineRun:
    def test_basic_run_returns_correct_output(self, store):
        pipeline = CheckpointedPipeline(
            [Step("double", double), Step("add_ten", add_ten)],
            store=store,
            run_id="r1",
        )
        result = pipeline.run(5)
        assert result.success
        assert result.output == 20  # (5*2)+10

    def test_executed_steps_are_recorded(self, store):
        pipeline = CheckpointedPipeline(
            [Step("double", double), Step("add_ten", add_ten)],
            store=store,
            run_id="r2",
        )
        result = pipeline.run(3)
        assert result.executed == ["double", "add_ten"]
        assert result.skipped == []

    def test_second_run_skips_completed_steps(self, store):
        pipeline = CheckpointedPipeline(
            [Step("double", double), Step("add_ten", add_ten)],
            store=store,
            run_id="r3",
        )
        pipeline.run(4)  # first run – saves checkpoints
        result = pipeline.run(4)  # second run – should skip both
        assert result.skipped == ["double", "add_ten"]
        assert result.executed == []
        assert result.output == 18  # (4*2)+10

    def test_failed_step_is_reported(self, store):
        pipeline = CheckpointedPipeline(
            [Step("boom", boom)],
            store=store,
            run_id="r4",
        )
        result = pipeline.run(1)
        assert not result.success
        assert result.failed_step == "boom"
        assert isinstance(result.error, ValueError)
        assert result.output is None

    def test_reset_clears_checkpoints(self, store):
        pipeline = CheckpointedPipeline(
            [Step("double", double)],
            store=store,
            run_id="r5",
        )
        pipeline.run(7)
        pipeline.reset()
        result = pipeline.run(7)
        assert result.executed == ["double"]
        assert result.skipped == []

    def test_add_step_fluent_interface(self, store):
        pipeline = (
            CheckpointedPipeline(store=store, run_id="r6")
            .add_step(Step("double", double))
            .add_step(Step("add_ten", add_ten))
        )
        result = pipeline.run(2)
        assert result.output == 14  # (2*2)+10

    def test_run_id_is_auto_generated_when_not_provided(self, store):
        p1 = CheckpointedPipeline(store=store)
        p2 = CheckpointedPipeline(store=store)
        assert p1.run_id != p2.run_id
