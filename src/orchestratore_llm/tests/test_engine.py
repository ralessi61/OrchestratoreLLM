"""Test di integrazione ed esecuzione per l'orchestratore LLM."""

import asyncio
import unittest
from orchestratore_llm.concurrency import AsyncioStrategy, ThreadingStrategy
from orchestratore_llm.engine import LLMOrchestrator
from orchestratore_llm.exceptions import InvalidRequestError
from orchestratore_llm.models import (
    ExecutionStatus,
    LLMRequest,
    LLMTaskType,
    TaskExecutionResult,
)
from orchestratore_llm.providers.registry import ProviderRegistry
from orchestratore_llm.providers.simulated import SimulatedLLMProvider
from orchestratore_llm.retry import RetryPolicy


class TestEngine(unittest.IsolatedAsyncioTestCase):
    async def test_order_preservation_with_heterogeneous_latencies(self):
        """Verifica che l'ordine delle richieste originali sia rigidamente preservato."""
        class VariableLatencyProvider(SimulatedLLMProvider):
            async def complete(self, request: LLMRequest):
                sleep_time = float(request.metadata.get("delay", 0.01))
                await asyncio.sleep(sleep_time)
                return await super().complete(request)

        prov = VariableLatencyProvider(name="var-lat", latency_range=(0.0, 0.0))
        reg = ProviderRegistry()
        reg.register(prov, set_as_default=True)

        orchestrator = LLMOrchestrator(registry=reg, max_concurrency=10)

        requests = [
            LLMRequest(
                prompt=f"Prompt {i}",
                request_id=f"REQ-{i}",
                metadata={"delay": (5 - i) * 0.01},
            )
            for i in range(5)
        ]

        batch = await orchestrator.execute_batch(requests)

        self.assertEqual(len(batch.results), 5)
        for i, res in enumerate(batch.results):
            self.assertEqual(res.request.request_id, f"REQ-{i}")
            self.assertTrue(res.is_success)

    async def test_concurrency_limit_enforced(self):
        """Verifica che il semaforo asincrono limiti le chiamate contemporanee al valore configurato."""
        active_concurrency = 0
        max_observed_concurrency = 0
        lock = asyncio.Lock()

        class ConcurrencyTrackingProvider(SimulatedLLMProvider):
            async def complete(self, request: LLMRequest):
                nonlocal active_concurrency, max_observed_concurrency
                async with lock:
                    active_concurrency += 1
                    if active_concurrency > max_observed_concurrency:
                        max_observed_concurrency = active_concurrency

                await asyncio.sleep(0.03)

                async with lock:
                    active_concurrency -= 1

                return await super().complete(request)

        reg = ProviderRegistry()
        reg.register(ConcurrencyTrackingProvider(name="track"), set_as_default=True)

        orchestrator = LLMOrchestrator(registry=reg, max_concurrency=3)
        requests = [LLMRequest(prompt=f"Req {i}") for i in range(10)]

        batch = await orchestrator.execute_batch(requests)

        self.assertEqual(len(batch.results), 10)
        self.assertLessEqual(max_observed_concurrency, 3)
        self.assertGreater(max_observed_concurrency, 1)

    async def test_fault_tolerance_no_work_lost_on_partial_failures(self):
        """Verifica che richieste con errori permanenti non blocchino né corrompano
        il lavoro già svolto o le altre richieste valide del batch.
        """
        prov = SimulatedLLMProvider(name="test-prov", latency_range=(0.001, 0.003))
        reg = ProviderRegistry()
        reg.register(prov, set_as_default=True)

        orchestrator = LLMOrchestrator(
            registry=reg,
            max_concurrency=5,
            retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01),
        )

        requests = [
            LLMRequest(prompt="Richiesta valida 1", request_id="REQ-OK-1"),
            LLMRequest(prompt="FAIL_PERMANENT", request_id="REQ-FAIL-1"),
            LLMRequest(prompt="Richiesta valida 2", request_id="REQ-OK-2"),
            LLMRequest(prompt="", request_id="REQ-FAIL-2"), # Prompt vuoto -> 400
            LLMRequest(prompt="Richiesta valida 3", request_id="REQ-OK-3"),
        ]

        batch = await orchestrator.execute_batch(requests)

        self.assertEqual(len(batch.results), 5)
        self.assertEqual(batch.metrics.total_requests, 5)
        self.assertEqual(batch.metrics.successful_requests, 3)
        self.assertEqual(batch.metrics.failed_requests, 2)

        self.assertTrue(batch.results_by_id["REQ-OK-1"].is_success)
        self.assertTrue(batch.results_by_id["REQ-OK-2"].is_success)
        self.assertTrue(batch.results_by_id["REQ-OK-3"].is_success)

        fail1 = batch.results_by_id["REQ-FAIL-1"]
        self.assertFalse(fail1.is_success)
        self.assertEqual(fail1.error_type, "InvalidRequestError")

        fail2 = batch.results_by_id["REQ-FAIL-2"]
        self.assertFalse(fail2.is_success)
        self.assertEqual(fail2.error_type, "InvalidRequestError")

    async def test_checkpoint_callback_execution(self):
        """Verifica l'invocazione del callback incrementale su ogni singolo completamento."""
        completed_ids = []

        def on_checkpoint(result: TaskExecutionResult):
            completed_ids.append(result.request_id)

        prov = SimulatedLLMProvider(name="sim", latency_range=(0.001, 0.002))
        reg = ProviderRegistry()
        reg.register(prov, set_as_default=True)

        orchestrator = LLMOrchestrator(
            registry=reg,
            max_concurrency=4,
            checkpoint_callback=on_checkpoint,
        )

        requests = [LLMRequest(prompt=f"p {i}", request_id=f"ID-{i}") for i in range(4)]
        await orchestrator.execute_batch(requests)

        self.assertEqual(len(completed_ids), 4)
        self.assertEqual(set(completed_ids), {"ID-0", "ID-1", "ID-2", "ID-3"})

    async def test_checkpoint_follows_completion_not_input_order(self):
        seen = []

        class VariableLatencyProvider(SimulatedLLMProvider):
            async def complete(self, request: LLMRequest):
                await asyncio.sleep(float(request.metadata["delay"]))
                return await super().complete(request)

        reg = ProviderRegistry()
        reg.register(
            VariableLatencyProvider(name="var", latency_range=(0.0, 0.0)),
            set_as_default=True,
        )
        orchestrator = LLMOrchestrator(
            registry=reg,
            max_concurrency=4,
            checkpoint_callback=lambda result: seen.append(result.request_id),
        )
        requests = [
            LLMRequest(prompt=f"p {i}", request_id=f"REQ-{i}", metadata={"delay": (4 - i) * 0.05})
            for i in range(4)
        ]

        batch = await orchestrator.execute_batch(requests)

        self.assertEqual(seen[0], "REQ-3")
        self.assertEqual(seen[-1], "REQ-0")
        self.assertEqual([res.request.request_id for res in batch.results], [f"REQ-{i}" for i in range(4)])

    async def test_cancel_does_not_wait_for_the_slow_ones(self):
        started = 0
        finished = 0

        class SlowProvider(SimulatedLLMProvider):
            async def complete(self, request: LLMRequest):
                nonlocal started, finished
                started += 1
                await asyncio.sleep(0.8)
                finished += 1
                return await super().complete(request)

        reg = ProviderRegistry()
        reg.register(SlowProvider(name="slow", latency_range=(0.0, 0.0)), set_as_default=True)
        orchestrator = LLMOrchestrator(registry=reg, max_concurrency=2)
        requests = [LLMRequest(prompt=f"p {i}", request_id=f"S-{i}") for i in range(6)]

        batch_task = asyncio.create_task(orchestrator.execute_batch(requests))
        await asyncio.sleep(0.1)
        batch_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await batch_task

        self.assertGreaterEqual(started, 1)
        self.assertEqual(finished, 0)

    async def test_threading_strategy_keeps_order_on_the_same_batch(self):
        class VariableLatencyProvider(SimulatedLLMProvider):
            async def complete(self, request: LLMRequest):
                await asyncio.sleep(float(request.metadata.get("delay", 0.01)))
                return await super().complete(request)

        requests = [
            LLMRequest(prompt=f"Prompt {i}", request_id=f"REQ-{i}", metadata={"delay": (5 - i) * 0.02})
            for i in range(5)
        ]

        outcomes = []
        for strategy in (AsyncioStrategy(4), ThreadingStrategy(4)):
            reg = ProviderRegistry()
            reg.register(
                VariableLatencyProvider(name="var-lat", latency_range=(0.0, 0.0)),
                set_as_default=True,
            )
            orchestrator = LLMOrchestrator(registry=reg, strategy=strategy)
            batch = await orchestrator.execute_batch(requests)
            outcomes.append([res.request.request_id for res in batch.results])
            self.assertTrue(all(res.is_success for res in batch.results))

        self.assertEqual(outcomes[0], outcomes[1])
        self.assertEqual(outcomes[0], [f"REQ-{i}" for i in range(5)])


if __name__ == "__main__":
    unittest.main()
