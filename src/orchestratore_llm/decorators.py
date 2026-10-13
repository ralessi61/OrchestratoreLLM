"""Decoratori async da comporre: retry, durata, log.

Ogni wrapper usa functools.wraps, quindi __name__ e __doc__ restano quelli
della funzione decorata. L'ordine di applicazione è quello di Python, dal basso verso l'alto.
"""

from __future__ import annotations

import functools
import logging
import time
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

from .retry import RetryPolicy, execute_with_retry

T = TypeVar("T")


def wrapped_attr(fn: Callable, name: str, default: Any = None) -> Any:
    """Cerca l'attributo sul wrapper e, se manca, sui wrapper interni tramite __wrapped__."""
    seen: Any = fn
    while seen is not None:
        values = getattr(seen, "__dict__", None)
        if values is not None and name in values:
            return values[name]
        seen = getattr(seen, "__wrapped__", None)
    return default


def retry(
    policy: Optional[RetryPolicy] = None,
    on_retry: Optional[Callable[[int, Exception, float], None]] = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Ritenta la coroutine con la RetryPolicy data. Senza policy usa i default.

    Dopo la chiamata, wrapper.last_attempts è il numero di tentativi fatti
    (1 se è riuscita al primo tentativo).
    """
    policy = policy or RetryPolicy()

    def decorate(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        """Applica la RetryPolicy e imposta last_attempts sul wrapper."""

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            """Esegue la coroutine, applica i retry e registra il numero di tentativi."""
            seen = {"n": 1}

            def _bridge(attempt_num: int, exc: Exception, delay: float) -> None:
                """Aggiorna il conteggio e invoca on_retry, se è stata passata."""
                # come in execute_request: attempt_num conta i retry, +1 include il primo tentativo
                seen["n"] = attempt_num + 1
                if on_retry is not None:
                    on_retry(attempt_num, exc, delay)

            async def _operation() -> T:
                """Una sola esecuzione: la RetryPolicy decide se ripeterla."""
                return await fn(*args, **kwargs)

            try:
                result, attempts = await execute_with_retry(
                    _operation, policy, on_retry=_bridge
                )
                wrapper.last_attempts = attempts
                return result
            except Exception:
                wrapper.last_attempts = seen["n"]
                raise

        return wrapper

    return decorate


def timing(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    """Misura la durata della coroutine, compresi i retry se @retry è il decoratore interno.

    Il valore finisce in wrapper.last_elapsed (secondi, time.monotonic).
    """

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> T:
        """Misura la coroutine e scrive last_elapsed anche se l'eccezione viene rilanciata."""
        started = time.monotonic()
        try:
            return await fn(*args, **kwargs)
        finally:
            wrapper.last_elapsed = time.monotonic() - started

    return wrapper


def logged(
    fn: Optional[Callable[..., Awaitable[T]]] = None,
    *,
    level: int = logging.DEBUG,
) -> Union[Callable[..., Awaitable[T]], Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]]:
    """Scrive a log ingresso, uscita ed eccezione, sul logger del modulo della funzione.

    Di default è DEBUG, così il log operativo dell'engine (quello con request_id)
    non viene raddoppiato a INFO. @logged e @logged(level=logging.INFO) vanno bene entrambi.
    """

    def decorate(fn: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        """Usa il logger del modulo della funzione decorata."""
        log = logging.getLogger(fn.__module__)

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            """Log di ingresso e di uscita; in caso di eccezione, un warning e poi il raise."""
            log.log(level, "chiamata %s", fn.__name__)
            try:
                result = await fn(*args, **kwargs)
            except Exception as exc:
                log.warning("%s ha rilanciato %s", fn.__name__, exc.__class__.__name__)
                raise
            spent = wrapped_attr(wrapper, "last_elapsed", None)
            if spent is None:
                log.log(level, "%s ok", fn.__name__)
            else:
                log.log(level, "%s ok in %.3fs", fn.__name__, spent)
            return result

        return wrapper

    if fn is None:
        return decorate
    return decorate(fn)
