"""Gerarchia delle eccezioni per l'orchestratore di chiamate LLM.

Separa nettamente gli errori transitori (recuperabili con retry e backoff)
dagli errori permanenti (non recuperabili, da interrompere subito).
"""

from typing import Optional


class OrchestratorError(Exception):
    """Classe base per tutte le eccezioni sollevate dall'orchestratore."""

    def __init__(self, message: str, provider: Optional[str] = None) -> None:
        """Messaggio e, se c'è, il nome del provider che ha fallito."""
        super().__init__(message)
        self.message = message
        self.provider = provider

    def __str__(self) -> str:
        if self.provider:
            return f"[{self.provider}] {self.message}"
        return self.message


class PermanentError(OrchestratorError):
    """Errore permanente non recuperabile.
    
    Indica una richiesta sintatticamente errata, credenziali non valide,
    modello inesistente o violazione delle policy del provider.
    Un nuovo tentativo produrrebbe identico esito negativo.
    """
    pass


class InvalidRequestError(PermanentError):
    """Payload non valido, schema JSON corrotto o parametri non supportati (es. HTTP 400, 422)."""
    pass


class AuthenticationError(PermanentError):
    """Chiave API mancante, revocata o non valida (es. HTTP 401)."""
    pass


class AuthorizationError(PermanentError):
    """Permessi insufficienti per accedere alla risorsa o al modello richiesto (es. HTTP 403)."""
    pass


class ModelNotFoundError(PermanentError):
    """Il modello LLM richiesto non esiste o è stato deprecato (es. HTTP 404)."""
    pass


class ContextWindowExceededError(PermanentError):
    """Il prompt ha superato la finestra di contesto massima consentita dal modello."""
    pass


class ProviderNotFoundError(PermanentError):
    """Il provider specificato nella richiesta non è registrato nell'orchestratore."""
    pass


class ResponseMismatchError(PermanentError):
    """La risposta porta il request_id di un'altra chiamata."""
    pass


class TransientError(OrchestratorError):
    """Errore transitorio recuperabile mediante nuova iterazione con backoff.
    
    Indica un sovraccarico momentaneo, indisponibilità temporanea del server,
    o problemi di rete che possono risolversi nei secondi successivi.
    """
    pass


class RateLimitError(TransientError):
    """Raggiungimento dei limiti di frequenza (Requests Per Minute o Tokens Per Minute:) del provider (es. HTTP 429)."""

    def __init__(
        self,
        message: str,
        provider: Optional[str] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        """Come OrchestratorError, con retry_after in secondi."""
        super().__init__(message, provider=provider)
        self.retry_after = retry_after

    def __str__(self) -> str:
        base = super().__str__()
        if self.retry_after is not None:
            return f"{base} (retry_after={self.retry_after:.1f}s)"
        return base


class ServerTemporarilyUnavailableError(TransientError):
    """Server del provider momentaneamente indisponibile o sovraccarico (es. HTTP 500, 502, 503, 504)."""
    pass


class NetworkTimeoutError(TransientError):
    """Timeout della richiesta di rete prima di aver ricevuto la risposta completa."""
    pass


class ConnectionDroppedError(TransientError):
    """Connessione TCP interrotta bruscamente o reset da peer."""
    pass
