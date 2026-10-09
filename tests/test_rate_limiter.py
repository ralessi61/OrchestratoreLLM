"""Test unitari per i componenti di rate limiting asincrono."""

import asyncio
import time
import unittest
from orchestratore_llm.rate_limiter import SlidingWindowRateLimiter, TokenBucketRateLimiter


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_token_bucket_burst_and_refill(self):
        # 10 richieste al secondo, capacità 2
        limiter = TokenBucketRateLimiter(rate=10, capacity=2, time_unit_seconds=1.0)

        t0 = time.monotonic()
        w1 = await limiter.acquire(1)
        w2 = await limiter.acquire(1)
        t1 = time.monotonic()

        self.assertLess(t1 - t0, 0.05)
        self.assertEqual(w1, 0.0)
        self.assertEqual(w2, 0.0)

        # Il terzo token attende la ricarica
        w3 = await limiter.acquire(1)
        t2 = time.monotonic()
        self.assertGreaterEqual(t2 - t1, 0.07)
        self.assertGreater(w3, 0.0)

    async def test_sliding_window_limiting(self):
        # Massimo 3 richieste per finestra di 0.2 secondi
        limiter = SlidingWindowRateLimiter(max_requests=3, window_seconds=0.2)

        t0 = time.monotonic()
        await limiter.acquire(1)
        await limiter.acquire(1)
        await limiter.acquire(1)
        t1 = time.monotonic()
        self.assertLess(t1 - t0, 0.05)

        # La 4a richiesta deve attendere la scadenza della finestra
        await limiter.acquire(1)
        t2 = time.monotonic()
        self.assertGreaterEqual(t2 - t0, 0.18)


if __name__ == "__main__":
    unittest.main()
