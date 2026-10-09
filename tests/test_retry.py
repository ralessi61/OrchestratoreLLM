"""Test unitari per la logica di retry, backoff e gestione retry-after."""

import unittest
from orchestratore_llm.exceptions import (
    AuthenticationError,
    InvalidRequestError,
    RateLimitError,
    ServerTemporarilyUnavailableError,
)
from orchestratore_llm.retry import RetryPolicy, execute_with_retry


class TestRetry(unittest.IsolatedAsyncioTestCase):
    def test_retry_policy_classification(self):
        policy = RetryPolicy()
        self.assertTrue(policy.is_retryable(RateLimitError("429")))
        self.assertTrue(policy.is_retryable(ServerTemporarilyUnavailableError("503")))
        self.assertFalse(policy.is_retryable(InvalidRequestError("400")))
        self.assertFalse(policy.is_retryable(AuthenticationError("401")))

    def test_retry_after_precedence(self):
        policy = RetryPolicy(initial_delay=10.0, backoff_factor=2.0, jitter=False)
        exc = RateLimitError("Rate limited", retry_after=1.5)
        delay = policy.calculate_delay(attempt=1, exception=exc)
        self.assertAlmostEqual(delay, 1.6, delta=0.05)

    async def test_execute_with_retry_eventual_success(self):
        calls = 0

        async def flaky_operation():
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RateLimitError("Temporary 429", retry_after=0.01)
            return "SUCCESS"

        policy = RetryPolicy(max_retries=3, initial_delay=0.01, jitter=False)
        result, attempts = await execute_with_retry(flaky_operation, policy)

        self.assertEqual(result, "SUCCESS")
        self.assertEqual(attempts, 3)
        self.assertEqual(calls, 3)

    async def test_execute_with_retry_permanent_failure_aborts_immediately(self):
        calls = 0

        async def bad_request_operation():
            nonlocal calls
            calls += 1
            raise InvalidRequestError("HTTP 400: Parametri errati")

        policy = RetryPolicy(max_retries=5, initial_delay=0.01)

        with self.assertRaises(InvalidRequestError):
            await execute_with_retry(bad_request_operation, policy)

        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
