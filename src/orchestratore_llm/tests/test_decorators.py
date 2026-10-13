"""I tre decoratori si compongono e conservano nome e docstring."""

import logging
import unittest

from orchestratore_llm.decorators import logged, retry, timing, wrapped_attr
from orchestratore_llm.exceptions import InvalidRequestError, RateLimitError
from orchestratore_llm.retry import RetryPolicy


class TestDecorators(unittest.IsolatedAsyncioTestCase):
    async def test_stack_keeps_name_and_retries(self):
        calls = 0

        @logged
        @timing
        @retry(RetryPolicy(max_retries=3, initial_delay=0.01, jitter=False))
        async def fetch(doc_id: str) -> str:
            """Legge un documento."""
            nonlocal calls
            calls += 1
            if calls < 3:
                raise RateLimitError("429", retry_after=0.01)
            return doc_id

        self.assertEqual(fetch.__name__, "fetch")
        self.assertEqual(fetch.__doc__, "Legge un documento.")

        value = await fetch("DOC-1")

        self.assertEqual(value, "DOC-1")
        self.assertEqual(calls, 3)
        self.assertEqual(wrapped_attr(fetch, "last_attempts"), 3)
        self.assertGreater(wrapped_attr(fetch, "last_elapsed"), 0)

    async def test_permanent_error_does_not_retry(self):
        calls = 0

        @retry(RetryPolicy(max_retries=4, initial_delay=0.01, jitter=False))
        async def fetch() -> None:
            nonlocal calls
            calls += 1
            raise InvalidRequestError("400")

        with self.assertRaises(InvalidRequestError):
            await fetch()

        self.assertEqual(calls, 1)
        self.assertEqual(wrapped_attr(fetch, "last_attempts"), 1)

    async def test_logged_accepts_level_argument(self):
        @logged(level=logging.INFO)
        async def ping() -> str:
            """pong"""
            return "ok"

        self.assertEqual(ping.__name__, "ping")
        self.assertEqual(ping.__doc__, "pong")
        self.assertEqual(await ping(), "ok")


if __name__ == "__main__":
    unittest.main()
