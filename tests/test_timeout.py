"""Tests for pipekit.timeout."""

import time
import pytest

from pipekit.timeout import TimeoutPolicy, TimedStep, TimeoutError


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def double(x):
    return x * 2


def slow(x):
    time.sleep(5)
    return x


# ---------------------------------------------------------------------------
# TimeoutPolicy validation
# ---------------------------------------------------------------------------

class TestTimeoutPolicyValidation:
    def test_positive_seconds_accepted(self):
        policy = TimeoutPolicy(seconds=1.0)
        assert policy.seconds == 1.0

    def test_zero_seconds_raises(self):
        with pytest.raises(ValueError, match="seconds must be positive"):
            TimeoutPolicy(seconds=0)

    def test_negative_seconds_raises(self):
        with pytest.raises(ValueError, match="seconds must be positive"):
            TimeoutPolicy(seconds=-3)

    def test_repr(self):
        policy = TimeoutPolicy(seconds=2.5)
        assert "2.5" in repr(policy)


# ---------------------------------------------------------------------------
# TimeoutPolicy.execute
# ---------------------------------------------------------------------------

class TestTimeoutPolicyExecute:
    def test_fast_function_returns_result(self):
        policy = TimeoutPolicy(seconds=2)
        assert policy.execute(double, 7) == 14

    def test_slow_function_raises_timeout(self):
        policy = TimeoutPolicy(seconds=0.1)
        with pytest.raises(TimeoutError):
            policy.execute(slow, 1)

    def test_exception_from_fn_propagates(self):
        def boom(x):
            raise ValueError("boom")

        policy = TimeoutPolicy(seconds=2)
        with pytest.raises(ValueError, match="boom"):
            policy.execute(boom, 0)


# ---------------------------------------------------------------------------
# TimedStep
# ---------------------------------------------------------------------------

class TestTimedStep:
    def test_run_returns_transformed_data(self):
        step = TimedStep("double", double, TimeoutPolicy(seconds=2))
        assert step.run(5) == 10

    def test_run_raises_on_timeout(self):
        step = TimedStep("slow", slow, TimeoutPolicy(seconds=0.1))
        with pytest.raises(TimeoutError):
            step.run(1)

    def test_repr_contains_name_and_policy(self):
        step = TimedStep("my_step", double, TimeoutPolicy(seconds=1))
        r = repr(step)
        assert "my_step" in r
        assert "TimeoutPolicy" in r
