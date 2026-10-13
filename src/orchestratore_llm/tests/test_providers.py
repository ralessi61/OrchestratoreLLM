"""Test unitari per i provider e il registro."""

import time
import unittest
from orchestratore_llm.exceptions import (
    InvalidRequestError,
    NetworkTimeoutError,
    ProviderNotFoundError,
    RateLimitError,
)
from orchestratore_llm.models import LLMRequest, LLMTaskType
from orchestratore_llm.providers.registry import ProviderRegistry, create_default_registry
from orchestratore_llm.providers.simulated import SimulatedLLMProvider


class TestProviders(unittest.IsolatedAsyncioTestCase):
    async def test_simulated_provider_tasks(self):
        provider = SimulatedLLMProvider(name="test-sim", latency_range=(0.0, 0.0))

        # Task Classificazione
        req_class = LLMRequest(prompt="Classifica contratto", task_type=LLMTaskType.CLASSIFICATION)
        res_class = await provider.complete(req_class)
        self.assertIsNotNone(res_class.parsed_content)
        self.assertIn("category", res_class.parsed_content)
        self.assertGreater(res_class.usage.total_tokens, 0)

        # Task Riassunto
        req_sum = LLMRequest(prompt="Riassumi documento", task_type=LLMTaskType.SUMMARY)
        res_sum = await provider.complete(req_sum)
        self.assertIn("Riepilogo sintetico", res_sum.content)

        # Task Estrazione
        req_ext = LLMRequest(prompt="Estrai campi", task_type=LLMTaskType.FIELD_EXTRACTION)
        res_ext = await provider.complete(req_ext)
        self.assertIn("extracted_fields", res_ext.parsed_content)

    async def test_simulated_provider_permanent_error_injection(self):
        provider = SimulatedLLMProvider(name="test-sim", latency_range=(0.0, 0.0))
        req = LLMRequest(prompt="FAIL_PERMANENT")

        with self.assertRaises(InvalidRequestError):
            await provider.complete(req)

    def test_provider_registry(self):
        reg = ProviderRegistry()
        p1 = SimulatedLLMProvider(name="prov-1")
        p2 = SimulatedLLMProvider(name="prov-2")

        reg.register(p1, set_as_default=True)
        reg.register(p2)

        self.assertEqual(len(reg), 2)
        self.assertIs(reg.get("prov-1"), p1)
        self.assertIs(reg.get("prov-2"), p2)
        self.assertIs(reg.get(), p1)

        with self.assertRaises(ProviderNotFoundError):
            reg.get("prov-inesistente")

    async def test_timeout_when_latency_exceeds_threshold(self) -> None:
        provider = SimulatedLLMProvider(
            name="lento",
            latency_range=(0.05, 0.05),
            timeout_after=0.01,
        )
        started = time.monotonic()
        with self.assertRaises(NetworkTimeoutError):
            await provider.complete(LLMRequest(prompt="documento", request_id="T-1"))
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.01)
        self.assertLess(elapsed, 0.2)

    async def test_under_threshold_returns_response(self) -> None:
        provider = SimulatedLLMProvider(
            name="lento",
            latency_range=(0.0, 0.0),
            timeout_after=1.0,
        )
        response = await provider.complete(LLMRequest(prompt="documento", request_id="T-2"))
        self.assertTrue(response.content)

    async def test_over_quota_raises_rate_limit_immediately(self) -> None:
        provider = SimulatedLLMProvider(
            name="quota",
            latency_range=(0.0, 0.0),
            max_requests_per_window=2,
            rate_window_seconds=1.0,
        )
        await provider.complete(LLMRequest(prompt="a", request_id="Q-1"))
        await provider.complete(LLMRequest(prompt="b", request_id="Q-2"))
        started = time.monotonic()
        with self.assertRaises(RateLimitError) as caught:
            await provider.complete(LLMRequest(prompt="c", request_id="Q-3"))
        self.assertLess(time.monotonic() - started, 0.1)
        self.assertGreater(caught.exception.retry_after, 0.5)
        self.assertLessEqual(caught.exception.retry_after, 1.0)

    def test_registered_providers_differ(self) -> None:
        registry = create_default_registry()
        ammollo = registry.get("ammollo")
        gwen = registry.get("gwen")
        grocky = registry.get("grocky")

        self.assertLess(ammollo.latency_range[1], 0.1)
        self.assertIsNone(ammollo.timeout_after)
        self.assertIsNone(ammollo.max_requests_per_window)

        self.assertGreater(gwen.latency_range[0], 0.2)
        self.assertGreater(gwen.latency_range[1], 1.0)
        self.assertLess(gwen.timeout_after, gwen.latency_range[1])
        self.assertGreater(gwen.timeout_after, gwen.latency_range[0])

        self.assertEqual(grocky.rate_limit_probability, 0.0)
        self.assertEqual(grocky.max_requests_per_window, 4)
        self.assertEqual(grocky.rate_window_seconds, 1.0)


if __name__ == "__main__":
    unittest.main()
