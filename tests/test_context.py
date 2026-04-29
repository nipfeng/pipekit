"""Tests for PipelineContext."""

import pytest

from pipekit.context import PipelineContext


class TestPipelineContextBasics:
    def test_set_and_get(self):
        ctx = PipelineContext()
        ctx.set("x", 42)
        assert ctx.get("x") == 42

    def test_get_missing_returns_default(self):
        ctx = PipelineContext()
        assert ctx.get("missing") is None
        assert ctx.get("missing", 99) == 99

    def test_require_raises_for_missing_key(self):
        ctx = PipelineContext()
        with pytest.raises(KeyError, match="missing_key"):
            ctx.require("missing_key")

    def test_require_returns_value_when_present(self):
        ctx = PipelineContext({"a": 1})
        assert ctx.require("a") == 1

    def test_delete_removes_key(self):
        ctx = PipelineContext({"k": "v"})
        ctx.delete("k")
        assert "k" not in ctx

    def test_delete_missing_key_is_noop(self):
        ctx = PipelineContext()
        ctx.delete("ghost")  # should not raise

    def test_contains(self):
        ctx = PipelineContext({"present": True})
        assert "present" in ctx
        assert "absent" not in ctx

    def test_len(self):
        ctx = PipelineContext({"a": 1, "b": 2})
        assert len(ctx) == 2

    def test_keys(self):
        ctx = PipelineContext({"x": 1, "y": 2})
        assert set(ctx.keys()) == {"x", "y"}

    def test_as_dict_is_copy(self):
        ctx = PipelineContext({"n": 7})
        d = ctx.as_dict()
        d["extra"] = 99
        assert "extra" not in ctx


class TestPipelineContextTags:
    def test_tag_and_get_tag(self):
        ctx = PipelineContext()
        ctx.tag("env", "production")
        assert ctx.get_tag("env") == "production"

    def test_get_tag_missing_returns_default(self):
        ctx = PipelineContext()
        assert ctx.get_tag("run_id") is None
        assert ctx.get_tag("run_id", "fallback") == "fallback"

    def test_tags_property_is_copy(self):
        ctx = PipelineContext()
        ctx.tag("k", "v")
        tags = ctx.tags
        tags["extra"] = "x"
        assert "extra" not in ctx.tags

    def test_initial_dict_populates_store(self):
        ctx = PipelineContext({"loaded": True})
        assert ctx.get("loaded") is True
