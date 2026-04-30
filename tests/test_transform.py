"""Tests for pipekit.transform — MapStep, FilterStep, ReduceStep."""

import pytest
from pipekit.transform import MapStep, FilterStep, ReduceStep, map_step, filter_step, reduce_step


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2


def is_even(x):
    return x % 2 == 0


def add(a, b):
    return a + b


# ---------------------------------------------------------------------------
# MapStep
# ---------------------------------------------------------------------------

class TestMapStep:
    def test_applies_fn_to_each_element(self):
        step = MapStep(name="double", fn=double)
        assert step.run([1, 2, 3]) == [2, 4, 6]

    def test_returns_empty_list_for_empty_input(self):
        step = MapStep(name="double", fn=double)
        assert step.run([]) == []

    def test_raises_type_error_for_non_iterable(self):
        step = MapStep(name="double", fn=double)
        with pytest.raises(TypeError, match="requires an iterable"):
            step.run(42)

    def test_repr_contains_name_and_fn(self):
        step = MapStep(name="double", fn=double)
        assert "double" in repr(step)

    def test_factory_creates_map_step(self):
        step = map_step("double", double)
        assert isinstance(step, MapStep)
        assert step.run([3]) == [6]


# ---------------------------------------------------------------------------
# FilterStep
# ---------------------------------------------------------------------------

class TestFilterStep:
    def test_keeps_matching_elements(self):
        step = FilterStep(name="evens", predicate=is_even)
        assert step.run([1, 2, 3, 4, 5]) == [2, 4]

    def test_returns_empty_when_nothing_matches(self):
        step = FilterStep(name="evens", predicate=is_even)
        assert step.run([1, 3, 5]) == []

    def test_raises_type_error_for_non_iterable(self):
        step = FilterStep(name="evens", predicate=is_even)
        with pytest.raises(TypeError, match="requires an iterable"):
            step.run(99)

    def test_repr_contains_name_and_predicate(self):
        step = FilterStep(name="evens", predicate=is_even)
        assert "evens" in repr(step)
        assert "is_even" in repr(step)

    def test_factory_creates_filter_step(self):
        step = filter_step("evens", is_even)
        assert isinstance(step, FilterStep)
        assert step.run([1, 2, 3]) == [2]


# ---------------------------------------------------------------------------
# ReduceStep
# ---------------------------------------------------------------------------

class TestReduceStep:
    def test_sums_list(self):
        step = ReduceStep(name="sum", fn=add, initial=0, has_initial=True)
        assert step.run([1, 2, 3, 4]) == 10

    def test_uses_first_element_as_initial_when_not_provided(self):
        step = ReduceStep(name="sum", fn=add)
        assert step.run([1, 2, 3]) == 6

    def test_raises_for_empty_iterable_without_initial(self):
        step = ReduceStep(name="sum", fn=add)
        with pytest.raises(ValueError, match="empty iterable"):
            step.run([])

    def test_returns_initial_for_empty_iterable_with_initial(self):
        step = ReduceStep(name="sum", fn=add, initial=0, has_initial=True)
        assert step.run([]) == 0

    def test_raises_type_error_for_non_iterable(self):
        step = ReduceStep(name="sum", fn=add, initial=0, has_initial=True)
        with pytest.raises(TypeError, match="requires an iterable"):
            step.run(5)

    def test_repr_contains_name_and_fn(self):
        step = ReduceStep(name="sum", fn=add)
        assert "sum" in repr(step)
        assert "add" in repr(step)

    def test_factory_creates_reduce_step(self):
        step = reduce_step("sum", add, initial=0, has_initial=True)
        assert isinstance(step, ReduceStep)
        assert step.run([1, 2, 3]) == 6
