"""Interfaccia astratta condivisa per tutti i provider di modelli linguistici."""

from __future__ import annotations
from abc import ABC, abstractmethod
from ..models import LLMRequest, LLMResponse


class BaseLLMProvider(ABC):
    """Contratto base per l'interazione con qualunque motore di inferenza LLM.
    
    Implementa lo Strategy Pattern: l'orchestratore interagisce unicamente con questo
    contratto, consentendo l'aggiunta di nuovi provider (es. OpenAI, Anthropic, Ollama,
    vLLM o mock locali) senza alterare il codice dell'orchestratore (Open/Closed Principle).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Identificativo univoco del provider (es. 'mock-openai', 'mock-claude')."""
        pass

    @abstractmethod
    async def complete(self, request: LLMRequest) -> LLMResponse:
        """Esegue l'elaborazione del prompt e restituisce la risposta strutturata.
        
        Raise:
            PermanentError: In caso di errore irreversibile (400, 401, 403, 404).
            TransientError: In caso di rate limit o indisponibilità temporanea (429, 503).
        """
        pass

    @abstractmethod
    def estimate_tokens(self, text: str) -> int:
        """Fornisce una stima del numero di token contenuti nella stringa di testo."""
        pass

    @abstractmethod
    async def check_health(self) -> bool:
        """Verifica lo stato operativo dell'endpoint del provider."""
        pass
