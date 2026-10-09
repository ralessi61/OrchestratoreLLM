"""Test unitari per i provider e il registro."""

import unittest
from orchestratore_llm.exceptions import InvalidRequestError, ProviderNotFoundError
from orchestratore_llm.models import LLMRequest, LLMTaskType
from orchestratore_llm.providers.registry import ProviderRegistry
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


if __name__ == "__main__":
    unittest.main()
