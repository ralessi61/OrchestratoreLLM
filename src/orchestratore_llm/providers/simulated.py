"""Provider simulato ad alta fedeltà per test, benchmark e ambienti offline."""

from __future__ import annotations

import asyncio
import json
import random
import time
from typing import Any, Dict, Optional, Tuple

from ..exceptions import (
    AuthenticationError,
    InvalidRequestError,
    RateLimitError,
    ServerTemporarilyUnavailableError,
)
from ..models import LLMRequest, LLMResponse, LLMTaskType, TokenUsage
from .base import BaseLLMProvider


class SimulatedLLMProvider(BaseLLMProvider):
    """Implementazione simulata di un provider LLM.
    
    Genera output coerenti per task di classificazione, riassunto ed estrazione
    campi, consentendo di iniettare latenze controllate ed errori transitori
    o permanenti per convalidare il comportamento dell'orchestratore.
    """

    def __init__(
        self,
        name: str = "ammollo",
        model: str = "cerri-gpt-4k-bio",
        latency_range: Tuple[float, float] = (0.02, 0.06),    # (min_secondi, max_secondi) interv. latenza temporale simulata per chiamata al provider
        rate_limit_probability: float = 0.0,
        server_error_probability: float = 0.0,
        permanent_error_probability: float = 0.0,
        simulated_retry_after: Optional[float] = None,
        fail_first_n_attempts: Dict[str, int] | None = None,
    ) -> None:
        self._name = name
        self._model = model
        self.latency_range = latency_range
        self.rate_limit_probability = rate_limit_probability
        self.server_error_probability = server_error_probability
        self.permanent_error_probability = permanent_error_probability
        self.simulated_retry_after = simulated_retry_after
        self.fail_first_n_attempts = fail_first_n_attempts or {}
        self._request_attempts: Dict[str, int] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def model(self) -> str:
        return self._model

    def estimate_tokens(self, text: str) -> int:
        """Stima pressapoco token (~4 caratteri per token in media)."""
        if not text:
            return 0
        return max(1, len(text) // 4)

    async def check_health(self) -> bool:
        return True

    async def complete(self, request: LLMRequest) -> LLMResponse:
        start_time = time.monotonic()

        # Tracciamento dei tentativi per specifica richiesta (per test di recovery controllato)
        req_id = request.request_id
        current_attempt = self._request_attempts.get(req_id, 0) + 1
        self._request_attempts[req_id] = current_attempt

        # Simulazione latenza di rete e inferenza
        min_lat, max_lat = self.latency_range
        sleep_duration = random.uniform(min_lat, max_lat) if max_lat > min_lat else min_lat
        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)

        # Iniezione guasti predeterminati per test unitari specifici
        if req_id in self.fail_first_n_attempts:
            threshold = self.fail_first_n_attempts[req_id]
            if current_attempt <= threshold:
                raise RateLimitError(
                    f"Simulated HTTP 429: limite di frequenza superato per richiesta {req_id}",
                    provider=self.name,
                    retry_after=self.simulated_retry_after or 0.2,
                )

        # Validazione input per simulazione errori permanenti (Bad Request)
        if not request.prompt or not request.prompt.strip():
            raise InvalidRequestError("Prompt non valido o vuoto", provider=self.name)

        if "FAIL_PERMANENT" in request.prompt:
            raise InvalidRequestError("Errore permanente forzato da payload", provider=self.name)

        if "FAIL_AUTH" in request.prompt:
            raise AuthenticationError("API key simulata non valida (HTTP 401)", provider=self.name)

        # Iniezione stocastica di errori transitori (HTTP 429 Rate Limit)
        if self.rate_limit_probability > 0 and random.random() < self.rate_limit_probability:
            retry_after = self.simulated_retry_after or round(random.uniform(0.1, 0.4), 2)
            raise RateLimitError(
                f"Rate limit simulato (HTTP 429) su {self.name}",
                provider=self.name,
                retry_after=retry_after,
            )

        # Iniezione stocastica di errori transitori server (HTTP 503 Service Unavailable)
        if self.server_error_probability > 0 and random.random() < self.server_error_probability:
            raise ServerTemporarilyUnavailableError(
                f"Server temporaneamente sovraccarico (HTTP 503) su {self.name}",
                provider=self.name,
            )

        # Iniezione stocastica di errori permanenti (HTTP 400 Bad Request)
        if self.permanent_error_probability > 0 and random.random() < self.permanent_error_probability:
            raise InvalidRequestError(
                f"Richiesta malformata irrecuperabile (HTTP 400) su {self.name}",
                provider=self.name,
            )

        # Generazione risposta coerente con il task
        content_str, parsed = self._generate_payload(request)

        # Calcolo token reali stimati
        prompt_tokens = self.estimate_tokens(request.prompt)
        completion_tokens = self.estimate_tokens(content_str)
        usage = TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        )

        latency = time.monotonic() - start_time

        return LLMResponse(
            request_id=request.request_id,
            document_id=request.document_id,
            content=content_str,
            parsed_content=parsed,
            provider=self.name,
            model=request.model or self.model,
            usage=usage,
            latency_seconds=latency,
            raw_response={"simulated": True, "attempt": current_attempt},
        )

    def _generate_payload(self, request: LLMRequest) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Produce payload specifici in base al tipo di task richiesto."""
        doc_ref = request.document_id or "DOC-GENERIC"

        if request.task_type == LLMTaskType.CLASSIFICATION:
            categories = ["Fattura Commerciale", "Contratto Quadro", "Reclamo Servizi", "DDT Consegna"]
            chosen_cat = categories[hash(doc_ref) % len(categories)]
            data = {
                "document_id": doc_ref,
                "category": chosen_cat,
                "confidence": round(0.88 + (hash(doc_ref) % 12) / 100.0, 2),
                "is_urgent": bool(hash(doc_ref) % 3 == 0),
            }
            return json.dumps(data, ensure_ascii=False), data

        elif request.task_type == LLMTaskType.SUMMARY:
            summary_text = (
                f"Riepilogo sintetico del documento {doc_ref}: il testo analizza i termini concordati, "
                f"le clausole di rinnovo e gli obblighi contrattuali con verifica di conformità completata."
            )
            data = {"document_id": doc_ref, "summary": summary_text, "key_points_count": 3}
            return summary_text, data

        elif request.task_type == LLMTaskType.FIELD_EXTRACTION:
            data = {
                "document_id": doc_ref,
                "extracted_fields": {
                    "identificativo": f"ID-{abs(hash(doc_ref)) % 100000:05d}",
                    "data_riferimento": "2026-10-01",
                    "importo_totale_eur": round(150.0 + (abs(hash(doc_ref)) % 85000) / 100.0, 2),
                    "codice_fiscale_piva": f"IT{abs(hash(doc_ref)) % 10000000000:011d}",
                    "stato_validazione": "CONFERMATO",
                },
            }
            return json.dumps(data, ensure_ascii=False), data

        else:
            generic_text = f"Risposta elaborata per la richiesta {request.request_id} sul documento {doc_ref}."
            return generic_text, None
