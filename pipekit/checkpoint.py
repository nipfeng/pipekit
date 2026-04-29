"""Checkpoint support for resumable pipelines."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class CheckpointStore:
    """Persists and retrieves step-level checkpoint data to/from disk."""

    directory: str = ".pipekit_checkpoints"
    _cache: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        os.makedirs(self.directory, exist_ok=True)

    def _path(self, run_id: str) -> str:
        return os.path.join(self.directory, f"{run_id}.json")

    def save(self, run_id: str, step_name: str, output: Any) -> None:
        """Persist the output of a completed step."""
        data = self._load_raw(run_id)
        data[step_name] = output
        with open(self._path(run_id), "w") as fh:
            json.dump(data, fh)
        self._cache[run_id] = data

    def load(self, run_id: str, step_name: str) -> Optional[Any]:
        """Return the saved output for *step_name*, or ``None`` if absent."""
        data = self._load_raw(run_id)
        return data.get(step_name)

    def has(self, run_id: str, step_name: str) -> bool:
        """Return ``True`` if a checkpoint exists for *step_name*."""
        return step_name in self._load_raw(run_id)

    def clear(self, run_id: str) -> None:
        """Delete all checkpoints for *run_id*."""
        path = self._path(run_id)
        if os.path.exists(path):
            os.remove(path)
        self._cache.pop(run_id, None)

    def _load_raw(self, run_id: str) -> Dict[str, Any]:
        if run_id in self._cache:
            return self._cache[run_id]
        path = self._path(run_id)
        if not os.path.exists(path):
            return {}
        with open(path) as fh:
            data = json.load(fh)
        self._cache[run_id] = data
        return data

    def __repr__(self) -> str:  # pragma: no cover
        return f"CheckpointStore(directory={self.directory!r})"
