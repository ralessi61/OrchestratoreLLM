"""Registro centralizzato dei provider per disaccoppiare invocazione da implementazione."""

from __future__ import annotations
from typing import Dict, List, Optional
from ..exceptions import ProviderNotFoundError
from .base import BaseLLMProvider
from .simulated import SimulatedLLMProvider


class ProviderRegistry:
    """
    Implementa il Registry Pattern per la gestione estendibile dei provider LLM.
    Consente di iniettare nuovi provider conformi all'interfaccia senza alterare la logica dell'orchestratore.
    """

    def __init__(self) -> None:
        """Registro vuoto, ancora senza provider di default."""
        self._providers: Dict[str, BaseLLMProvider] = {}
        self._default_provider_name: Optional[str] = None

    def register(self, provider: BaseLLMProvider, set_as_default: bool = False) -> None:
        """Registra una nuova istanza di provider."""
        self._providers[provider.name] = provider
        if set_as_default or self._default_provider_name is None:
            self._default_provider_name = provider.name

    def get(self, name: Optional[str] = None) -> BaseLLMProvider:
        """Recupera il provider richiesto o quello predefinito se non specificato."""
        target_name = name or self._default_provider_name
        if not target_name:
            raise ProviderNotFoundError("Nessun provider registrato nel sistema")

        provider = self._providers.get(target_name)
        if not provider:
            raise ProviderNotFoundError(
                f"Provider '{target_name}' non trovato. Disponibili: {list(self._providers.keys())}",
                provider=target_name,
            )
        return provider

    def list_providers(self) -> List[str]:
        """Elenca i nomi di tutti i provider correntemente registrati."""
        return list(self._providers.keys())

    def __contains__(self, name: str) -> bool:
        return name in self._providers

    def __len__(self) -> int:
        return len(self._providers)


def create_default_registry() -> ProviderRegistry:
    """Costruisce un registry predefinito con provider simulati."""
    registry = ProviderRegistry()

    # Veloce e affidabile: è anche il default della CLI.
    default_sim = SimulatedLLMProvider(
        name="ammollo",
        model="cerri-gpt-4k-bio",
        latency_range=(0.01, 0.04),
    )
    registry.register(default_sim, set_as_default=True)

    # Lento, latenza variabile. Se il valore estratto supera timeout_after, la chiamata va in timeout.
    slow_sim = SimulatedLLMProvider(
        name="gwen",
        model="parker-gpt-4o-spider",
        latency_range=(0.4, 1.4),
        timeout_after=1.0,
    )
    registry.register(slow_sim)

    # Quota stretta: 4 chiamate ammesse per secondo, poi 429 con retry_after.
    limited_sim = SimulatedLLMProvider(
        name="grocky",
        model="drago-27b-tispiezzo-in2",
        latency_range=(0.01, 0.03),
        max_requests_per_window=4,
        rate_window_seconds=1.0,
    )
    registry.register(limited_sim)

    return registry
