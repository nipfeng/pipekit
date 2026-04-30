"""Composable transform steps for mapping, filtering, and reducing pipeline data."""

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional


@dataclass
class MapStep:
    """Applies a function to every element in an iterable payload."""

    name: str
    fn: Callable[[Any], Any]

    def run(self, data: Iterable[Any]) -> list:
        if not hasattr(data, "__iter__"):
            raise TypeError(f"MapStep '{self.name}' requires an iterable, got {type(data).__name__}")
        return [self.fn(item) for item in data]

    def __repr__(self) -> str:
        return f"MapStep(name={self.name!r}, fn={self.fn.__name__!r})"


@dataclass
class FilterStep:
    """Filters elements in an iterable payload using a predicate function."""

    name: str
    predicate: Callable[[Any], bool]

    def run(self, data: Iterable[Any]) -> list:
        if not hasattr(data, "__iter__"):
            raise TypeError(f"FilterStep '{self.name}' requires an iterable, got {type(data).__name__}")
        return [item for item in data if self.predicate(item)]

    def __repr__(self) -> str:
        return f"FilterStep(name={self.name!r}, predicate={self.predicate.__name__!r})"


@dataclass
class ReduceStep:
    """Reduces an iterable payload to a single value using an accumulator function."""

    name: str
    fn: Callable[[Any, Any], Any]
    initial: Any = field(default=None)
    _has_initial: bool = field(default=False, init=False, repr=False)

    def __init__(self, name: str, fn: Callable[[Any, Any], Any], initial: Any = None, *, has_initial: bool = False):
        self.name = name
        self.fn = fn
        self.initial = initial
        self._has_initial = has_initial

    def run(self, data: Iterable[Any]) -> Any:
        if not hasattr(data, "__iter__"):
            raise TypeError(f"ReduceStep '{self.name}' requires an iterable, got {type(data).__name__}")
        items = list(data)
        if not items:
            if self._has_initial:
                return self.initial
            raise ValueError(f"ReduceStep '{self.name}' received empty iterable with no initial value")
        acc = self.initial if self._has_initial else items[0]
        start = 0 if self._has_initial else 1
        for item in items[start:]:
            acc = self.fn(acc, item)
        return acc

    def __repr__(self) -> str:
        return f"ReduceStep(name={self.name!r}, fn={self.fn.__name__!r}, initial={self.initial!r})"


def map_step(name: str, fn: Callable[[Any], Any]) -> MapStep:
    """Convenience factory for MapStep."""
    return MapStep(name=name, fn=fn)


def filter_step(name: str, predicate: Callable[[Any], bool]) -> FilterStep:
    """Convenience factory for FilterStep."""
    return FilterStep(name=name, predicate=predicate)


def reduce_step(name: str, fn: Callable[[Any, Any], Any], initial: Any = None, *, has_initial: bool = False) -> ReduceStep:
    """Convenience factory for ReduceStep."""
    return ReduceStep(name=name, fn=fn, initial=initial, has_initial=has_initial)
