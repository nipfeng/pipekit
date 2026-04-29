"""Tests for pipekit.hooks."""

import pytest
from pipekit.hooks import PipelineHooks


def make_recorder():
    """Return a simple callable that records its call arguments."""
    calls = []

    def recorder(*args):
        calls.append(args)

    recorder.calls = calls
    return recorder


class TestPipelineHooksDefaults:
    def test_all_hooks_default_to_none(self):
        hooks = PipelineHooks()
        for attr in ("on_start", "on_step_start", "on_step_end",
                     "on_success", "on_failure", "on_finish"):
            assert getattr(hooks, attr) is None

    def test_fire_methods_are_safe_when_no_hooks_registered(self):
        hooks = PipelineHooks()
        # None of these should raise
        hooks.fire_start("p")
        hooks.fire_step_start("p", "s")
        hooks.fire_step_end("p", "s", True)
        hooks.fire_success("p")
        hooks.fire_failure("p", "s", RuntimeError("boom"))
        hooks.fire_finish("p")


class TestPipelineHooksFiring:
    def test_on_start_receives_pipeline_name(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_start=rec)
        hooks.fire_start("my_pipeline")
        assert rec.calls == [("my_pipeline",)]

    def test_on_step_start_receives_pipeline_and_step_name(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_step_start=rec)
        hooks.fire_step_start("pipe", "step_one")
        assert rec.calls == [("pipe", "step_one")]

    def test_on_step_end_receives_success_flag(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_step_end=rec)
        hooks.fire_step_end("pipe", "step_one", False)
        assert rec.calls == [("pipe", "step_one", False)]

    def test_on_success_receives_pipeline_name(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_success=rec)
        hooks.fire_success("pipe")
        assert rec.calls == [("pipe",)]

    def test_on_failure_receives_pipeline_step_and_exception(self):
        rec = make_recorder()
        exc = ValueError("bad value")
        hooks = PipelineHooks(on_failure=rec)
        hooks.fire_failure("pipe", "step_two", exc)
        assert len(rec.calls) == 1
        assert rec.calls[0] == ("pipe", "step_two", exc)

    def test_on_finish_called_with_pipeline_name(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_finish=rec)
        hooks.fire_finish("pipe")
        assert rec.calls == [("pipe",)]

    def test_multiple_hooks_can_be_registered_independently(self):
        start_rec = make_recorder()
        finish_rec = make_recorder()
        hooks = PipelineHooks(on_start=start_rec, on_finish=finish_rec)
        hooks.fire_start("p")
        hooks.fire_finish("p")
        assert len(start_rec.calls) == 1
        assert len(finish_rec.calls) == 1

    def test_hook_called_multiple_times_accumulates(self):
        rec = make_recorder()
        hooks = PipelineHooks(on_start=rec)
        hooks.fire_start("a")
        hooks.fire_start("b")
        assert rec.calls == [("a",), ("b",)]
