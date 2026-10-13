"""Politiche di retry con Backoff Esponenziale, Full Jitter e supporto Retry-After."""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Sequence, Tuple, Type, TypeVar

from .exceptions import PermanentError, RateLimitError, TransientError

T = TypeVar("T")
logger = logging.getLogger(__name__)


@dataclass
class RetryPolicy:
    """Configurazione dell'algoritmo di retry con backoff e jitter."""

    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    backoff_factor: float = 2.0
    jitter: bool = True
    retryable_exceptions: Tuple[Type[Exception], ...] = (TransientError,)

    def is_retryable(self, exception: Exception) -> bool:
        """Determina se l'eccezione riscontrata giustifica un nuovo tentativo."""
        # Se è un errore permanente, fallisce tassativamente
        if isinstance(exception, PermanentError):
            return False
        # Verifica appartenenza alle eccezioni transitorie configurate
        return isinstance(exception, self.retryable_exceptions)

    def calculate_delay(self, attempt: int, exception: Optional[Exception] = None) -> float:
        """Calcola l'intervallo di attesa ottimale prima della successiva esecuzione.
        
        Applica l'algoritmo 'Full Jitter' raccomandato per sistemi distribuiti:
        t = uniform(0, min(max_delay, initial_delay * factor^attempt))
        Se l'eccezione include un'indicazione esplicita `retry_after` (HTTP 429),
        tale valore ha precedenza assoluta.
        """
        if isinstance(exception, RateLimitError) and exception.retry_after is not None:
            # Aggiunge un lieve margine (100ms) per evitare collisioni al bordo esatto
            return max(0.1, exception.retry_after + 0.1)

        # Calcolo esponenziale canonico
        exponential = self.initial_delay * (self.backoff_factor ** attempt)
        ceiling = min(self.max_delay, exponential)

        if self.jitter:
            return random.uniform(0.0, ceiling)
        return ceiling


async def execute_with_retry(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Tuple[T, int]:
    """Esegue un'operazione asincrona applicando la policy di retry.
    
    Returns:
        Tupla composta dal valore restituito dall'operazione e dal numero
        totale di tentativi effettuati (1 = riuscito al primo tentativo).
        
    Raises:
        L'eccezione originale se i tentativi vengono esauriti o se l'errore è permanente.
    """
    attempt = 0

    while True:
        try:
            result = await operation()
            return result, attempt + 1
        except Exception as exc:
            if not policy.is_retryable(exc) or attempt >= policy.max_retries:
                # Errore permanente o tentativi esauriti
                raise exc

            delay = policy.calculate_delay(attempt, exc)
            if on_retry:
                on_retry(attempt + 1, exc, delay)
            else:
                logger.warning(
                    f"Tentativo {attempt + 1} fallito con {exc.__class__.__name__}: {exc}. "
                    f"Nuovo tentativo tra {delay:.2f}s..."
                )

            await asyncio.sleep(delay)
            attempt += 1
