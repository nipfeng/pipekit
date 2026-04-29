"""Pipeline execution context for sharing state between steps."""

from typing import Any, Dict, Iterator, Optional


class PipelineContext:
    """A key-value store passed through all steps in a pipeline run.

    Allows steps to read and write shared metadata or intermediate
    results without relying on function return values alone.
    """

    def __init__(self, initial: Optional[Dict[str, Any]] = None) -> None:
        self._store: Dict[str, Any] = dict(initial or {})
        self._tags: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # Core store access
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store *value* under *key*."""
        self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Return the value for *key*, or *default* if absent."""
        return self._store.get(key, default)

    def require(self, key: str) -> Any:
        """Return the value for *key*, raising KeyError if absent."""
        if key not in self._store:
            raise KeyError(f"Required context key not found: '{key}'")
        return self._store[key]

    def delete(self, key: str) -> None:
        """Remove *key* from the store (no-op if absent)."""
        self._store.pop(key, None)

    def keys(self) -> Iterator[str]:
        return iter(self._store)

    def __contains__(self, key: str) -> bool:
        return key in self._store

    def __len__(self) -> int:
        return len(self._store)

    # ------------------------------------------------------------------
    # Tags — lightweight string labels (e.g. run_id, env)
    # ------------------------------------------------------------------

    def tag(self, key: str, value: str) -> None:
        """Attach a string tag to this context."""
        self._tags[key] = value

    def get_tag(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self._tags.get(key, default)

    @property
    def tags(self) -> Dict[str, str]:
        return dict(self._tags)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def as_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the internal store."""
        return dict(self._store)

    def __repr__(self) -> str:  # pragma: no cover
        return f"PipelineContext(keys={list(self._store.keys())}, tags={self._tags})"
