"""La chiamata a complete è un AsyncMock. Il resto dell'engine esegue il codice reale."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from typing import Any, Callable, Optional, Tuple

from orchestratore_llm.engine import LLMOrchestrator
from orchestratore_llm.exceptions import InvalidRequestError, RateLimitError
from orchestratore_llm.models import LLMRequest, LLMResponse, TokenUsage
from orchestratore_llm.providers.registry import ProviderRegistry
from orchestratore_llm.providers.simulated import SimulatedLLMProvider
from orchestratore_llm.retry import RetryPolicy


def _response(request: LLMRequest, text: str = "") -> LLMResponse:
    """Risposta simulata con lo stesso request_id della richiesta."""
    return LLMResponse(
        request_id=request.request_id,
        content=text or f"testo {request.request_id}",
        provider="remoto",
        model="finto-1",
        document_id=request.document_id,
        usage=TokenUsage(prompt_tokens=2, completion_tokens=3, total_tokens=5),
    )


def _orchestrator(
    max_concurrency: int = 4,
    checkpoint_callback: Optional[Callable[[Any], None]] = None,
) -> Tuple[SimulatedLLMProvider, LLMOrchestrator]:
    """Provider registrato e orchestratore, con complete ancora da sostituire."""
    provider = SimulatedLLMProvider(name="remoto", latency_range=(0.0, 0.0))
    registry = ProviderRegistry()
    registry.register(provider, set_as_default=True)
    orchestrator = LLMOrchestrator(
        registry=registry,
        max_concurrency=max_concurrency,
        retry_policy=RetryPolicy(max_retries=3, initial_delay=0.01, jitter=False),
        checkpoint_callback=checkpoint_callback,
    )
    return provider, orchestrator


class TestMockedCalls(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path(self) -> None:
        provider, orchestrator = _orchestrator()
        request = LLMRequest(prompt="Classifica il contratto", request_id="REQ-1")

        with patch.object(provider, "complete", new=AsyncMock(side_effect=_response)) as complete:
            result = await orchestrator.execute_request(request)

        self.assertTrue(result.is_success)
        self.assertEqual(result.response.content, "testo REQ-1")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(complete.await_count, 1)
        self.assertIs(complete.await_args.args[0], request)

    async def test_retry_on_transient_error(self) -> None:
        provider, orchestrator = _orchestrator()
        request = LLMRequest(prompt="Riassumi", request_id="REQ-2")
        responses = [
            RateLimitError("429", retry_after=0.01),
            RateLimitError("429", retry_after=0.01),
            _response(request),
        ]

        with patch.object(provider, "complete", new=AsyncMock(side_effect=responses)) as complete:
            result = await orchestrator.execute_request(request)

        self.assertTrue(result.is_success)
        self.assertEqual(result.attempts, 3)
        self.assertEqual(complete.await_count, 3)

    async def test_permanent_error_is_not_retried(self) -> None:
        provider, orchestrator = _orchestrator()
        request = LLMRequest(prompt="Estrai", request_id="REQ-3")

        with patch.object(
            provider,
            "complete",
            new=AsyncMock(side_effect=InvalidRequestError("400")),
        ) as complete:
            result = await orchestrator.execute_request(request)

        self.assertFalse(result.is_success)
        self.assertEqual(result.error_type, "InvalidRequestError")
        self.assertEqual(result.attempts, 1)
        self.assertEqual(complete.await_count, 1)

    async def test_semaphore_enforces_the_cap(self) -> None:
        provider, orchestrator = _orchestrator(max_concurrency=2)
        in_flight = 0
        peak = 0
        lock = asyncio.Lock()

        async def complete_one(request: LLMRequest) -> LLMResponse:
            nonlocal in_flight, peak
            async with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            await asyncio.sleep(0.05)
            async with lock:
                in_flight -= 1
            return _response(request)

        requests = [LLMRequest(prompt=f"p {i}", request_id=f"C-{i}") for i in range(6)]
        with patch.object(provider, "complete", new=AsyncMock(side_effect=complete_one)) as complete:
            batch = await orchestrator.execute_batch(requests)

        self.assertEqual(len(batch.results), 6)
        self.assertTrue(all(res.is_success for res in batch.results))
        self.assertLessEqual(peak, 2)
        self.assertGreater(peak, 1)
        self.assertEqual(complete.await_count, 6)

    async def test_out_of_order_responses_stay_on_their_request(self) -> None:
        seen = []
        provider, orchestrator = _orchestrator(
            max_concurrency=4,
            checkpoint_callback=lambda result: seen.append(result.request_id),
        )

        async def complete_one(request: LLMRequest) -> LLMResponse:
            await asyncio.sleep(float(request.metadata["delay"]))
            return _response(request, text=request.prompt)

        requests = [
            LLMRequest(prompt=f"doc {i}", request_id=f"REQ-{i}", metadata={"delay": (3 - i) * 0.04})
            for i in range(4)
        ]
        with patch.object(provider, "complete", new=AsyncMock(side_effect=complete_one)) as complete:
            batch = await orchestrator.execute_batch(requests)

        self.assertEqual(seen[0], "REQ-3")
        self.assertEqual(seen[-1], "REQ-0")
        self.assertEqual(
            [res.request.request_id for res in batch.results],
            ["REQ-0", "REQ-1", "REQ-2", "REQ-3"],
        )
        for res in batch.results:
            self.assertEqual(res.response.request_id, res.request.request_id)
            self.assertEqual(res.response.content, res.request.prompt)
        self.assertEqual(complete.await_count, 4)

    async def test_response_for_another_request(self) -> None:
        """Scritto prima del controllo: una risposta con request_id altrui non è un successo."""
        provider, orchestrator = _orchestrator()
        request = LLMRequest(prompt="Classifica", request_id="REQ-MIA")

        async def complete_one(request: LLMRequest) -> LLMResponse:
            response = _response(request)
            return response.model_copy(update={"request_id": "REQ-ALTRUI"})

        with patch.object(provider, "complete", new=AsyncMock(side_effect=complete_one)) as complete:
            result = await orchestrator.execute_request(request)

        self.assertFalse(result.is_success)
        self.assertEqual(result.error_type, "ResponseMismatchError")
        self.assertEqual(result.attempts, 1)
        self.assertIn("REQ-ALTRUI", result.error)
        self.assertIn("REQ-MIA", result.error)
        self.assertEqual(complete.await_count, 1)


if __name__ == "__main__":
    unittest.main()
