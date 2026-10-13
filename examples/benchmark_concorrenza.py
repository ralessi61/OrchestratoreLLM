"""Tempi sullo stesso batch: sequenziale, asyncio e thread, con max_concurrency diversi.

Il simulatore attende un tempo fisso e non fallisce. Non c'è rate limiter.
Il cronometro misura il parallelismo, non i retry.
"""

from __future__ import annotations

import asyncio
import statistics
import sys
import time
from pathlib import Path
from typing import Awaitable, Callable, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from orchestratore_llm import (
    AsyncioStrategy,
    LLMOrchestrator,
    LLMRequest,
    RetryPolicy,
    ThreadingStrategy,
)
from orchestratore_llm.concurrency import ConcurrencyStrategy
from orchestratore_llm.models import TaskExecutionResult
from orchestratore_llm.providers.registry import ProviderRegistry
from orchestratore_llm.providers.simulated import SimulatedLLMProvider

# 24 chiamate da 50 ms: in sequenza circa 1,2 s, abbastanza per distinguere i valori di max_concurrency.
REQUEST_COUNT = 24
LATENCY_SECONDS = 0.05
CONCURRENCY_LEVELS = (1, 4, 12, 24)
REPEATS = 3

Runner = Callable[[], Awaitable[Tuple[float, Sequence[TaskExecutionResult]]]]


def _batch() -> List[LLMRequest]:
    """Lo stesso elenco per ogni misura."""
    return [
        LLMRequest(prompt=f"documento {i}", request_id=f"B-{i:02d}")
        for i in range(REQUEST_COUNT)
    ]


def _orchestrator(strategy: Optional[ConcurrencyStrategy] = None) -> LLMOrchestrator:
    """Orchestratore senza rate limiter e senza retry."""
    registry = ProviderRegistry()
    registry.register(
        SimulatedLLMProvider(
            name="fixed",
            model="fixed",
            latency_range=(LATENCY_SECONDS, LATENCY_SECONDS),
        ),
        set_as_default=True,
    )
    return LLMOrchestrator(
        registry=registry,
        strategy=strategy,
        max_concurrency=1,
        retry_policy=RetryPolicy(max_retries=0, jitter=False),
    )


async def _run_sequential(
    requests: Sequence[LLMRequest],
) -> Tuple[float, List[TaskExecutionResult]]:
    """Una execute_request dopo l'altra."""
    orchestrator = _orchestrator()
    started = time.perf_counter()
    results: List[TaskExecutionResult] = []
    for request in requests:
        results.append(await orchestrator.execute_request(request))
    return time.perf_counter() - started, results


async def _run_concurrent(
    requests: Sequence[LLMRequest],
    strategy: ConcurrencyStrategy,
) -> Tuple[float, List[TaskExecutionResult]]:
    """execute_batch con la strategia indicata."""
    orchestrator = _orchestrator(strategy)
    started = time.perf_counter()
    batch = await orchestrator.execute_batch(requests)
    return time.perf_counter() - started, batch.results


async def _measure(label: str, limit: Optional[int], runner: Runner) -> dict:
    """Mediana, minimo e massimo di REPEATS giri. Tutte le richieste devono riuscire."""
    samples: List[float] = []
    for _ in range(REPEATS):
        elapsed, results = await runner()
        succeeded = sum(1 for result in results if result.is_success)
        if succeeded != REQUEST_COUNT:
            raise RuntimeError(f"{label} limit={limit}: succeeded {succeeded}/{REQUEST_COUNT}")
        samples.append(elapsed)
    return {
        "mode": label,
        "limit": limit,
        "median_s": statistics.median(samples),
        "min_s": min(samples),
        "max_s": max(samples),
    }


async def main() -> None:
    """Stampa la tabella dei tempi."""
    requests = _batch()
    # Una chiamata a vuoto, così import e primo loop non finiscono nella prima riga.
    await _run_sequential(requests[:1])

    rows = [
        await _measure("sequential", None, lambda: _run_sequential(requests)),
    ]
    for limit in CONCURRENCY_LEVELS:
        rows.append(
            await _measure(
                "asyncio",
                limit,
                lambda limit=limit: _run_concurrent(requests, AsyncioStrategy(limit)),
            )
        )
    for limit in CONCURRENCY_LEVELS:
        rows.append(
            await _measure(
                "threading",
                limit,
                lambda limit=limit: _run_concurrent(requests, ThreadingStrategy(limit)),
            )
        )

    print(
        f"batch={REQUEST_COUNT} latency={LATENCY_SECONDS:.3f}s repeats={REPEATS} median"
    )
    print(f"{'mode':<14} {'max_concurrency':>16} {'median_s':>10} {'min_s':>8} {'max_s':>8}")
    for row in rows:
        limit = "-" if row["limit"] is None else str(row["limit"])
        print(
            f"{row['mode']:<14} {limit:>16} {row['median_s']:10.3f} "
            f"{row['min_s']:8.3f} {row['max_s']:8.3f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
