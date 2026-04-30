"""Tests for ThrottlePolicy and ThrottledStep."""

import time
import pytest
from pipekit.throttle import ThrottlePolicy
from pipekit.throttled_step import ThrottledStep


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def identity(x):
    return x


def double(x):
    return x * 2


# ---------------------------------------------------------------------------
# ThrottlePolicy validation
# ---------------------------------------------------------------------------

class TestThrottlePolicyValidation:
    def test_negative_min_interval_raises(self):
        with pytest.raises(ValueError, match="min_interval"):
            ThrottlePolicy(min_interval=-0.1)

    def test_zero_max_calls_raises(self):
        with pytest.raises(ValueError, match="max_calls"):
            ThrottlePolicy(max_calls=0)

    def test_negative_window_raises(self):
        with pytest.raises(ValueError, match="window"):
            ThrottlePolicy(window=-1.0)

    def test_zero_window_raises(self):
        with pytest.raises(ValueError, match="window"):
            ThrottlePolicy(window=0.0)

    def test_valid_defaults(self):
        p = ThrottlePolicy()
        assert p.min_interval == 0.0
        assert p.max_calls is None
        assert p.window == 1.0

    def test_negative_max_calls_raises(self):
        with pytest.raises(ValueError, match="max_calls"):
            ThrottlePolicy(max_calls=-1)


# ---------------------------------------------------------------------------
# ThrottlePolicy execution
# ---------------------------------------------------------------------------

class TestThrottlePolicyExecution:
    def test_executes_function_and_returns_result(self):
        policy = ThrottlePolicy()
        result = policy.execute(double, 5)
        assert result == 10

    def test_min_interval_delays_second_call(self):
        policy = ThrottlePolicy(min_interval=0.1)
        policy.execute(identity, 1)
        start = time.monotonic()
        policy.execute(identity, 2)
        elapsed = time.monotonic() - start
        assert elapsed >= 0.09  # allow small timing tolerance

    def test_no_delay_when_interval_already_passed(self):
        policy = ThrottlePolicy(min_interval=0.05)
        policy.execute(identity, 1)
        time.sleep(0.1)
        start = time.monotonic()
        policy.execute(identity, 2)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    def test_max_calls_throttles_within_window(self):
        policy = ThrottlePolicy(max_calls=2, window=0.3)
        times = []
        for _ in range(3):
            t = time.monotonic()
            policy.execute(identity, 1)
            times.append(time.monotonic() - t)
        # The third call should have been delayed
        assert sum(times) >= 0.25

    def test_repr(self):
        p = ThrottlePolicy(min_interval=0.5, max_calls=10, window=2.0)
        assert "ThrottlePolicy" in repr(p)
        assert "0.5" in repr(p)

    def test_execute_passes_kwargs(self):
        """Ensure execute forwards keyword arguments correctly to the wrapped function."""
        def add(x, y=0):
            return x + y

        policy = ThrottlePolicy()
        result = policy.execute(add, 3, y=7)
        assert result == 10


# ---------------------------------------------------------------------------
# Throttl
