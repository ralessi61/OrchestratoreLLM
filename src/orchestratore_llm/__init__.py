"""Libreria Python per l'orchestrazione asincrona e resiliente di chiamate LLM.

Offre controllo della concorrenza, rate limiting (Token Bucket/Sliding Window),
politiche di retry con Full Jitter, classificazione degli errori (transienti vs permanenti)
e tracciamento metriche.
"""

from .concurrency import AsyncioStrategy, ThreadingStrategy
from .engine import LLMOrchestrator
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    ContextWindowExceededError,
    InvalidRequestError,
    ModelNotFoundError,
    NetworkTimeoutError,
    OrchestratorError,
    PermanentError,
    ProviderNotFoundError,
    RateLimitError,
    ServerTemporarilyUnavailableError,
    TransientError,
)
from .logging_config import configure_logging
from .models import (
    BatchMetrics,
    BatchResult,
    ExecutionStatus,
    LLMRequest,
    LLMResponse,
    LLMTaskType,
    TaskExecutionResult,
    TokenUsage,
)
from .providers.base import BaseLLMProvider
from .providers.registry import ProviderRegistry, create_default_registry
from .providers.simulated import SimulatedLLMProvider
from .rate_limiter import BaseRateLimiter, SlidingWindowRateLimiter, TokenBucketRateLimiter
from .retry import RetryPolicy, execute_with_retry

__version__ = "0.1.0"

__all__ = [
    # Motore principale
    "LLMOrchestrator",
    "AsyncioStrategy",
    "ThreadingStrategy",
    # Modelli di dati
    "LLMRequest",
    "LLMResponse",
    "LLMTaskType",
    "TokenUsage",
    "TaskExecutionResult",
    "ExecutionStatus",
    "BatchMetrics",
    "BatchResult",
    # Eccezioni
    "OrchestratorError",
    "PermanentError",
    "TransientError",
    "RateLimitError",
    "ServerTemporarilyUnavailableError",
    "NetworkTimeoutError",
    "InvalidRequestError",
    "AuthenticationError",
    "AuthorizationError",
    "ModelNotFoundError",
    "ContextWindowExceededError",
    "ProviderNotFoundError",
    # Rate Limiting
    "BaseRateLimiter",
    "TokenBucketRateLimiter",
    "SlidingWindowRateLimiter",
    # Retry
    "RetryPolicy",
    "execute_with_retry",
    # Provider
    "BaseLLMProvider",
    "SimulatedLLMProvider",
    "ProviderRegistry",
    "create_default_registry",
    # Logging
    "configure_logging",
]
