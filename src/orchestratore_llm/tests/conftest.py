"""Configurazione e fixture condivise per la suite di test."""

import sys
from pathlib import Path

# Assicura la raggiungibilità del modulo src anche con esecuzioni esterne
src_path = Path(__file__).resolve().parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

import pytest
from orchestratore_llm.models import LLMRequest, LLMTaskType
from orchestratore_llm.providers.registry import ProviderRegistry
from orchestratore_llm.providers.simulated import SimulatedLLMProvider
from orchestratore_llm.retry import RetryPolicy


@pytest.fixture
def sample_request() -> LLMRequest:
    return LLMRequest(
        prompt="Analizza la clausola di recesso contrattuale",
        task_type=LLMTaskType.CLASSIFICATION,
        document_id="DOC-TEST-001",
    )


@pytest.fixture
def fast_provider() -> SimulatedLLMProvider:
    return SimulatedLLMProvider(
        name="test-fast",
        model="sim-fast",
        latency_range=(0.001, 0.005),
    )


@pytest.fixture
def fast_registry(fast_provider: SimulatedLLMProvider) -> ProviderRegistry:
    reg = ProviderRegistry()
    reg.register(fast_provider, set_as_default=True)
    return reg


@pytest.fixture
def fast_retry_policy() -> RetryPolicy:
    return RetryPolicy(
        max_retries=2,
        initial_delay=0.01,
        max_delay=0.05,
        jitter=False,
    )
