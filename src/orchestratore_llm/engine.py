"""Motore principale dell'orchestratore per l'esecuzione batch concorrente e resiliente."""

from __future__ import annotations

import asyncio
import logging
import statistics
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Union

from .concurrency import AsyncioStrategy, ConcurrencyStrategy
from .decorators import logged, retry, timing, wrapped_attr
from .exceptions import OrchestratorError, PermanentError, ResponseMismatchError, TransientError
from .models import (
    BatchMetrics,
    BatchResult,
    ExecutionStatus,
    LLMRequest,
    LLMResponse,
    TaskExecutionResult,
)
from .providers.base import BaseLLMProvider
from .providers.registry import ProviderRegistry, create_default_registry
from .rate_limiter import BaseRateLimiter
from .retry import RetryPolicy

logger = logging.getLogger(__name__)


class LLMOrchestrator:
    """Orchestratore di chiamate a modelli linguistici con tolleranza d'errore.
    
    Limita le chiamate concorrenti, impone limiti di frequenza,
    applica retry adattivo ed elabora batch di grandi dimensioni preservando l'ordine
    e la corrispondenza con le richieste d'origine.
    """

    def __init__(
        self,
        registry: Optional[ProviderRegistry] = None,
        max_concurrency: int = 15,
        rate_limiter: Optional[BaseRateLimiter] = None,
        retry_policy: Optional[RetryPolicy] = None,
        checkpoint_callback: Optional[Callable[[TaskExecutionResult], Union[None, Awaitable[None]]]] = None,
        strategy: Optional[ConcurrencyStrategy] = None,
    ) -> None:
        """Inizializza l'orchestratore.
        
        Args:
            registry: Registro dei provider. Se omesso, istanzia i provider simulati predefiniti.
            max_concurrency: Numero massimo di chiamate di rete simultanee attive.
            rate_limiter: Componente opzionale di rate limiting (TokenBucket o SlidingWindow).
            retry_policy: Regole di retry e backoff per errori transitori.
            checkpoint_callback: Funzione invocata non appena un task termina,
                                utile per persistere su disco/database i singoli avanzamenti.
            strategy: AsyncioStrategy o ThreadingStrategy. Se omessa, asyncio
                      con quel max_concurrency.
        """
        if max_concurrency <= 0:
            raise ValueError("max_concurrency deve essere strettamente positivo")

        self.registry = registry or create_default_registry()
        self.strategy = strategy or AsyncioStrategy(max_concurrency)
        self.max_concurrency = self.strategy.max_concurrency
        self.rate_limiter = rate_limiter
        self.retry_policy = retry_policy or RetryPolicy()
        self.checkpoint_callback = checkpoint_callback

    async def execute_request(self, request: LLMRequest) -> TaskExecutionResult:
        """Elabora una singola richiesta gestendo validazione, semafori, rate limit e retry."""
        start_time = time.monotonic()
        total_attempts = 1
        last_exception: Optional[Exception] = None

        # 1. Risoluzione del provider dal registro
        try:
            provider: BaseLLMProvider = self.registry.get(request.provider)
        except Exception as exc:
            # Fallimento immediato: provider sconosciuto (errore permanente)
            duration = time.monotonic() - start_time
            logger.error(
                f"Impossibile risolvere provider per richiesta {request.request_id}: {exc}",
                extra={"request_id": request.request_id, "document_id": request.document_id},
            )
            return TaskExecutionResult(
                request=request,
                status=ExecutionStatus.FAILED,
                error=str(exc),
                error_type=exc.__class__.__name__,
                is_transient=False,
                attempts=1,
                total_duration_seconds=duration,
            )

        # 3. Callback interna per tracciamento dei retry
        def _on_retry(attempt_num: int, exc: Exception, delay_sec: float) -> None:
            """Conta il tentativo ed emette un warning prima del backoff."""
            nonlocal total_attempts
            # attempt_num conta i retry già avviati; includi anche il primo tentativo.
            total_attempts = attempt_num + 1
            logger.warning(
                f"Retry richiesta {request.request_id} (doc={request.document_id}) - "
                f"Tentativo #{attempt_num} a causa di {exc.__class__.__name__}: {exc}. "
                f"Backoff di {delay_sec:.2f}s",
                extra={
                    "request_id": request.request_id,
                    "document_id": request.document_id,
                    "provider": provider.name,
                    "attempt": attempt_num,
                    "retry_delay": delay_sec,
                },
            )

        # 2. e 4. Rate limit, retry, durata e log sono decoratori sulla stessa coroutine.
        #    Il decoratore più interno è @retry: @timing misura anche i backoff, @logged vede un solo esito.
        @logged
        @timing
        @retry(self.retry_policy, on_retry=_on_retry)
        async def _call_provider() -> LLMResponse:
            """Chiama il provider dopo aver preso il permesso dal rate limiter."""
            if self.rate_limiter is not None:
                await self.rate_limiter.acquire(tokens=1)
            response = await provider.complete(request)
            if response.request_id != request.request_id:
                raise ResponseMismatchError(
                    f"response.request_id vale {response.request_id!r}, "
                    f"la richiesta era {request.request_id!r}",
                    provider=provider.name,
                )
            return response

        try:
            response = await _call_provider()
            total_attempts = wrapped_attr(_call_provider, "last_attempts", total_attempts)
            duration = time.monotonic() - start_time

            logger.info(
                f"Richiesta completata con successo: {request.request_id} "
                f"(doc={request.document_id}, tentativi={total_attempts}, latenza={duration:.2f}s)",
                extra={
                    "request_id": request.request_id,
                    "document_id": request.document_id,
                    "provider": provider.name,
                    "status": "success",
                    "latency": duration,
                    "tokens": response.usage.total_tokens,
                },
            )

            result = TaskExecutionResult(
                request=request,
                status=ExecutionStatus.SUCCESS,
                response=response,
                attempts=total_attempts,
                total_duration_seconds=duration,
            )

        except Exception as exc:
            duration = time.monotonic() - start_time
            total_attempts = wrapped_attr(_call_provider, "last_attempts", total_attempts)
            is_trans = isinstance(exc, TransientError)
            err_type = exc.__class__.__name__

            logger.error(
                f"Richiesta fallita definitivamente: {request.request_id} "
                f"(doc={request.document_id}, tentativi={total_attempts}, tipo={err_type}): {exc}",
                extra={
                    "request_id": request.request_id,
                    "document_id": request.document_id,
                    "provider": provider.name,
                    "status": "failed",
                    "error_type": err_type,
                    "is_transient": is_trans,
                },
            )

            result = TaskExecutionResult(
                request=request,
                status=ExecutionStatus.FAILED,
                error=str(exc),
                error_type=err_type,
                is_transient=is_trans,
                attempts=total_attempts,
                total_duration_seconds=duration,
            )

        # 5. Notifica eventuale checkpoint per salvataggio incrementale
        if self.checkpoint_callback:
            try:
                res = self.checkpoint_callback(result)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as cb_err:
                logger.error(f"Errore durante l'esecuzione del callback di checkpoint: {cb_err}")

        return result

    async def execute_batch(
        self,
        requests: Sequence[LLMRequest],
    ) -> BatchResult:
        """Elabora in concorrenza una collezione di richieste garantendo l'ordine dei risultati.
        
        Args:
            requests: Sequenza di richieste in ingresso.
            
        Returns:
            BatchResult con l'elenco ordinato corrispondente 1:1 alle richieste di partenza
            e le relative metriche analitiche.
        """
        if not requests:
            return BatchResult(results=[], metrics=BatchMetrics())

        batch_start = time.monotonic()
        total_count = len(requests)

        # La posizione è quella di partenza. Si riempie quando la chiamata finisce,
        # non quando sono finite tutte.
        ordered_results: List[Optional[TaskExecutionResult]] = [None] * total_count

        def _store(index: int, result: TaskExecutionResult) -> None:
            """Scrive l'esito all'indice originale della richiesta."""
            ordered_results[index] = result

        jobs = [(lambda req=req: self.execute_request(req)) for req in requests]
        await self.strategy.run(jobs, _store)

        wall_clock = time.monotonic() - batch_start
        final_results = [r for r in ordered_results if r is not None]

        # Calcolo delle metriche aggregate
        metrics = self._calculate_metrics(final_results, wall_clock)

        logger.info(
            f"Elaborazione batch conclusa: {metrics.successful_requests}/{metrics.total_requests} successi, "
            f"{metrics.failed_requests} fallimenti, {metrics.total_retries} retry, "
            f"tempo totale: {metrics.wall_clock_seconds:.2f}s"
        )

        return BatchResult(results=final_results, metrics=metrics)

    def run_batch(self, requests: Sequence[LLMRequest]) -> BatchResult:
        """Punto d'ingresso sincrono per pipeline ed script batch tradizionali."""
        return asyncio.run(self.execute_batch(requests))

    def _calculate_metrics(
        self,
        results: List[TaskExecutionResult],
        wall_clock_seconds: float,
    ) -> BatchMetrics:
        """Elabora le statistiche di rendimento e qualità del batch."""
        total = len(results)
        successful = sum(1 for r in results if r.is_success)
        failed = total - successful

        total_retries = sum(max(0, r.attempts - 1) for r in results)

        prompt_tokens = 0
        completion_tokens = 0
        latencies: List[float] = []
        error_breakdown: Dict[str, int] = {}

        for r in results:
            if r.is_success and r.response:
                prompt_tokens += r.response.usage.prompt_tokens
                completion_tokens += r.response.usage.completion_tokens
                latencies.append(r.response.latency_seconds)
            else:
                err_key = r.error_type or "UnknownError"
                error_breakdown[err_key] = error_breakdown.get(err_key, 0) + 1

        total_tokens = prompt_tokens + completion_tokens

        avg_lat = statistics.mean(latencies) if latencies else 0.0
        min_lat = min(latencies) if latencies else 0.0
        max_lat = max(latencies) if latencies else 0.0

        safe_wall_clock = max(0.0001, wall_clock_seconds)
        rps = total / safe_wall_clock
        tps = total_tokens / safe_wall_clock

        return BatchMetrics(
            total_requests=total,
            successful_requests=successful,
            failed_requests=failed,
            total_retries=total_retries,
            total_tokens=total_tokens,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            wall_clock_seconds=wall_clock_seconds,
            avg_latency_seconds=avg_lat,
            min_latency_seconds=min_lat,
            max_latency_seconds=max_lat,
            requests_per_second=rps,
            tokens_per_second=tps,
            error_breakdown=error_breakdown,
        )
