"""Tests for CheckpointStore."""

import os
import json
import pytest

from pipekit.checkpoint import CheckpointStore


@pytest.fixture()
def store(tmp_path):
    return CheckpointStore(directory=str(tmp_path / "ckpts"))


class TestCheckpointStoreBasics:
    def test_directory_is_created(self, tmp_path):
        d = str(tmp_path / "new_dir")
        assert not os.path.exists(d)
        CheckpointStore(directory=d)
        assert os.path.isdir(d)

    def test_save_and_load_scalar(self, store):
        store.save("run1", "step_a", 42)
        assert store.load("run1", "step_a") == 42

    def test_save_and_load_dict(self, store):
        store.save("run1", "step_b", {"x": [1, 2, 3]})
        assert store.load("run1", "step_b") == {"x": [1, 2, 3]}

    def test_load_missing_returns_none(self, store):
        assert store.load("run1", "nonexistent") is None

    def test_has_returns_false_when_missing(self, store):
        assert not store.has("run1", "step_x")

    def test_has_returns_true_after_save(self, store):
        store.save("run1", "step_x", "data")
        assert store.has("run1", "step_x")

    def test_multiple_steps_same_run(self, store):
        store.save("run2", "a", 1)
        store.save("run2", "b", 2)
        assert store.load("run2", "a") == 1
        assert store.load("run2", "b") == 2

    def test_different_runs_are_isolated(self, store):
        store.save("runA", "step", "alpha")
        store.save("runB", "step", "beta")
        assert store.load("runA", "step") == "alpha"
        assert store.load("runB", "step") == "beta"

    def test_clear_removes_file(self, store, tmp_path):
        store.save("run3", "step", 99)
        store.clear("run3")
        assert not store.has("run3", "step")

    def test_clear_nonexistent_run_is_safe(self, store):
        store.clear("ghost_run")  # should not raise

    def test_persisted_to_disk(self, store, tmp_path):
        store.save("run4", "step", "hello")
        path = os.path.join(str(tmp_path / "ckpts"), "run4.json")
        with open(path) as fh:
            data = json.load(fh)
        assert data["step"] == "hello"
