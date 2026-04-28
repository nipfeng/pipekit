"""Tests for RetryPolicy and its integration with Step."""

import pytest
from unittest.mock import MagicMock

from pipekit.retry import RetryPolicy
from pipekit.step import Step


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_flaky(fail_times: int):
    """Return a callable that raises ValueError *fail_times* before succeeding."""
    calls = {"count": 0}

    def flaky(data):
        calls["count"] += 1
        if calls["count"] <= fail_times:
            raise ValueError(f"transient error #{calls['count']}")
        return data * 2

    return flaky


# ---------------------------------------------------------------------------
# RetryPolicy unit tests
# ---------------------------------------------------------------------------

class TestRetryPolicyValidation:
    def test_max_attempts_must_be_positive(self):
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_delay_must_be_non_negative(self):
        with pytest.raises(ValueError, match="delay"):
            RetryPolicy(delay=-0.1)

    def test_backoff_must_be_at_least_one(self):
        with pytest.raises(ValueError, match="backoff"):
            RetryPolicy(backoff=0.5)


class TestRetryPolicyExecute:
    def test_succeeds_on_first_attempt(self):
        policy = RetryPolicy(max_attempts=3, delay=0)
        result = policy.execute(lambda x: x + 1, 4)
        assert result == 5

    def test_retries_and_succeeds(self):
        policy = RetryPolicy(max_attempts=3, delay=0)
        flaky = make_flaky(fail_times=2)
        assert policy.execute(flaky, 5) == 10

    def test_raises_after_max_attempts(self):
        policy = RetryPolicy(max_attempts=2, delay=0)
        always_fail = make_flaky(fail_times=99)
        with pytest.raises(ValueError):
            policy.execute(always_fail, 5)

    def test_on_retry_callback_invoked(self):
        callback = MagicMock()
        policy = RetryPolicy(max_attempts=3, delay=0, on_retry=callback)
        flaky = make_flaky(fail_times=2)
        policy.execute(flaky, 1)
        assert callback.call_count == 2

    def test_only_catches_specified_exceptions(self):
        policy = RetryPolicy(
            max_attempts=3, delay=0, exceptions=(TypeError,)
        )
        with pytest.raises(ValueError):
            policy.execute(lambda x: (_ for _ in ()).throw(ValueError("nope")), None)


# ---------------------------------------------------------------------------
# Step + RetryPolicy integration
# ---------------------------------------------------------------------------

class TestStepWithRetryPolicy:
    def test_step_retries_transparently(self):
        policy = RetryPolicy(max_attempts=4, delay=0)
        step = Step("flaky_step", make_flaky(fail_times=3), retry_policy=policy)
        assert step.run(3) == 6

    def test_step_without_policy_raises_immediately(self):
        step = Step("boom", make_flaky(fail_times=1))
        with pytest.raises(ValueError):
            step.run(1)

    def test_step_repr_includes_policy(self):
        policy = RetryPolicy(max_attempts=2, delay=0)
        step = Step("s", lambda x: x, retry_policy=policy)
        assert "RetryPolicy" in repr(step)
