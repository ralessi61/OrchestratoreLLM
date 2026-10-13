"""Modelli di dati sul confine: ciò che entra e ciò che esce passa da Pydantic."""

from __future__ import annotations

import json
import time
import uuid
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

# Un prompt più lungo di così non entra: è già oltre una finestra di contesto ragionevole.
PROMPT_MAX_CHARS = 100_000


class LLMTaskType(str, Enum):
    """Tipologia dell'operazione delegata al modello linguistico."""
    CLASSIFICATION = "classification"
    SUMMARY = "summary"
    FIELD_EXTRACTION = "field_extraction"
    GENERIC = "generic"


class ExecutionStatus(str, Enum):
    """Stato finale dell'elaborazione di una singola richiesta."""
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


def format_validation_errors(exc: ValidationError) -> str:
    """Una riga per errore: il campo, poi il motivo."""
    lines = []
    for err in exc.errors():
        field = _format_loc(err.get("loc") or ())
        msg = _clean_message(err)
        if field and not msg.startswith(field):
            lines.append(f"{field}: {msg}")
        else:
            lines.append(msg or field)
    return "\n".join(lines)


def parse_request_batch(data: Any) -> List[LLMRequest]:
    """Valida il JSON della CLI: una lista di richieste."""
    if not isinstance(data, list):
        trovato = type(data).__name__
        raise ValueError(f"file: deve essere una lista di richieste, non un {trovato}")
    return TypeAdapter(List[LLMRequest]).validate_python(data)


def _format_loc(loc: Any) -> str:
    """Da ('prompt',) o (1, 'prompt') a prompt oppure richiesta[1].prompt."""
    out = ""
    for item in loc:
        if isinstance(item, int):
            if not out:
                out = f"richiesta[{item}]"
            else:
                out += f"[{item}]"
        elif not out:
            out = str(item)
        else:
            out += f".{item}"
    return out


def _clean_message(err: Dict[str, Any]) -> str:
    """Traduce il messaggio di Pydantic in una riga sul campo."""
    msg = str(err.get("msg") or "")
    if msg.startswith("Value error, "):
        msg = msg[len("Value error, ") :]
    kind = err.get("type")
    if kind == "missing":
        return "manca ed è obbligatorio"
    if kind in ("string_type", "string_unicode"):
        return "deve essere testo"
    if kind in ("dict_type", "model_type"):
        return "deve essere un oggetto, non una lista o un valore singolo"
    if kind in ("int_type", "int_parsing"):
        return "deve essere un intero"
    if kind in ("float_type", "float_parsing"):
        return "deve essere un numero"
    if kind == "list_type":
        return "deve essere una lista"
    return msg


class LLMRequest(BaseModel):
    """Richiesta immutabile destinata all'orchestratore per l'inoltro al provider."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    prompt: str
    task_type: LLMTaskType = LLMTaskType.GENERIC
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    document_id: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: float = Field(default_factory=time.time)

    @field_validator("prompt", mode="before")
    @classmethod
    def _prompt_text(cls, value: Any) -> str:
        """Il prompt è testo non vuoto, entro PROMPT_MAX_CHARS."""
        if not isinstance(value, str):
            raise ValueError("deve essere testo")
        cleaned = value.strip()
        if not cleaned:
            raise ValueError(
                "è vuoto o contiene solo spazi, non c'è testo da inviare al provider"
            )
        if len(cleaned) > PROMPT_MAX_CHARS:
            raise ValueError(
                f"è lungo {len(cleaned)} caratteri, il massimo accettato in ingresso è {PROMPT_MAX_CHARS}"
            )
        return cleaned

    @field_validator("task_type", mode="before")
    @classmethod
    def _task_type(cls, value: Any) -> Any:
        """Accetta un LLMTaskType o una delle stringhe note."""
        if isinstance(value, LLMTaskType):
            return value
        if value is None or value == "":
            return LLMTaskType.GENERIC
        label = str(value).strip()
        try:
            return LLMTaskType(label)
        except ValueError:
            allowed = ", ".join(item.value for item in LLMTaskType)
            raise ValueError(f"'{label}' non è un tipo noto. Valori ammessi: {allowed}")

    @field_validator("request_id", mode="before")
    @classmethod
    def _request_id(cls, value: Any) -> Any:
        """Stringa non vuota; se manca ne genera uno."""
        if value is None or (isinstance(value, str) and not value.strip()):
            return str(uuid.uuid4())
        if not isinstance(value, str):
            raise ValueError("deve essere una stringa")
        return value.strip()

    @field_validator("parameters")
    @classmethod
    def _call_parameters(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """temperature e max_tokens, se ci sono, devono essere usabili dal provider."""
        if not isinstance(value, dict):
            raise ValueError("deve essere un oggetto di parametri")
        out = dict(value)
        if "temperature" in out and out["temperature"] is not None:
            raw = out["temperature"]
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise ValueError(
                    f"temperature vale {raw!r}, deve essere un numero tra 0 e 2"
                )
            temp = float(raw)
            if temp < 0 or temp > 2:
                raise ValueError(
                    f"temperature vale {temp}, deve essere un numero tra 0 e 2"
                )
            out["temperature"] = temp
        if "max_tokens" in out and out["max_tokens"] is not None:
            raw = out["max_tokens"]
            if isinstance(raw, bool) or not isinstance(raw, int):
                raise ValueError(
                    f"max_tokens vale {raw!r}, deve essere un intero maggiore di zero"
                )
            if raw < 1:
                raise ValueError(
                    f"max_tokens vale {raw}, deve essere un intero maggiore di zero"
                )
        return out

    def to_dict(self) -> Dict[str, Any]:
        """Serializza la richiesta in dizionario nativo."""
        data = self.model_dump(mode="python")
        if isinstance(self.task_type, LLMTaskType):
            data["task_type"] = self.task_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LLMRequest:
        """Ricostruisce un'istanza da dizionario."""
        if not isinstance(data, dict):
            raise ValueError(
                f"la richiesta deve essere un oggetto JSON, non {type(data).__name__}"
            )
        payload = dict(data)
        if payload.get("request_id") in (None, ""):
            payload.pop("request_id", None)
        return cls.model_validate(payload)


class TokenUsage(BaseModel):
    """Riepilogo del consumo di token associato a una chiamata LLM."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    @model_validator(mode="before")
    @classmethod
    def _fill_missing_total(cls, data: Any) -> Any:
        """Se total_tokens è 0, lo riempie con prompt più completion."""
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        prompt = payload.get("prompt_tokens") or 0
        completion = payload.get("completion_tokens") or 0
        total = payload.get("total_tokens") or 0
        if total == 0 and (prompt or completion):
            payload["total_tokens"] = prompt + completion
        return payload

    @field_validator("prompt_tokens", "completion_tokens", "total_tokens")
    @classmethod
    def _non_negative_tokens(cls, value: int) -> int:
        """Un conteggio di token non può essere negativo."""
        if value < 0:
            raise ValueError(
                f"vale {value}, un conteggio di token non può essere negativo"
            )
        return value

    @model_validator(mode="after")
    def _total_matches_parts(self) -> TokenUsage:
        """Se le parti sono valorizzate, total_tokens deve essere la loro somma."""
        parts_sum = self.prompt_tokens + self.completion_tokens
        if parts_sum > 0 and self.total_tokens != parts_sum:
            raise ValueError(
                f"total_tokens: vale {self.total_tokens} ma prompt_tokens "
                f"({self.prompt_tokens}) + completion_tokens ({self.completion_tokens}) fa {parts_sum}"
            )
        return self

    def to_dict(self) -> Dict[str, int]:
        """Serializza i tre conteggi."""
        return self.model_dump(mode="python")


class LLMResponse(BaseModel):
    """Risultato restituito da un provider a seguito di una richiesta riuscita."""

    # `model` è un campo nostro; senza questo Pydantic lo scambia per il proprio namespace.
    model_config = ConfigDict(protected_namespaces=())

    request_id: str
    content: str
    provider: str
    model: str
    document_id: Optional[str] = None
    parsed_content: Optional[Any] = None
    usage: TokenUsage = Field(default_factory=TokenUsage)
    latency_seconds: float = 0.0
    raw_response: Optional[Dict[str, Any]] = None

    @field_validator("content")
    @classmethod
    def _content(cls, value: str) -> str:
        """Il testo della risposta non può essere vuoto."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError("è vuoto, il provider non ha restituito testo")
        return value

    @field_validator("latency_seconds")
    @classmethod
    def _non_negative_latency(cls, value: float) -> float:
        """La latenza dichiarata non può essere negativa."""
        if value < 0:
            raise ValueError(
                f"vale {value}, un tempo di risposta non può essere negativo"
            )
        return value

    def to_dict(self) -> Dict[str, Any]:
        """Serializza la risposta, usage compreso."""
        data = self.model_dump(mode="python")
        data["usage"] = self.usage.to_dict()
        return data


class TaskExecutionResult(BaseModel):
    """Esito di una richiesta, con il riferimento alla richiesta originale."""

    request: LLMRequest
    status: ExecutionStatus
    response: Optional[LLMResponse] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    is_transient: bool = False
    attempts: int = 1
    total_duration_seconds: float = 0.0

    @model_validator(mode="after")
    def _consistent_outcome(self) -> TaskExecutionResult:
        """success richiede una risposta, failed un errore non vuoto."""
        if self.attempts < 1:
            raise ValueError(
                f"attempts: vale {self.attempts}, deve essere almeno 1"
            )
        if self.total_duration_seconds < 0:
            raise ValueError(
                f"total_duration_seconds: vale {self.total_duration_seconds}, non può essere negativo"
            )
        if self.status == ExecutionStatus.SUCCESS and self.response is None:
            raise ValueError(
                "response: lo stato è success ma manca la risposta del provider"
            )
        if self.status == ExecutionStatus.FAILED and not (self.error and self.error.strip()):
            raise ValueError(
                "error: lo stato è failed ma non c'è il motivo del fallimento"
            )
        return self

    @property
    def is_success(self) -> bool:
        """True se lo stato è success."""
        return self.status == ExecutionStatus.SUCCESS

    @property
    def request_id(self) -> str:
        """request_id della richiesta originale."""
        return self.request.request_id

    @property
    def document_id(self) -> Optional[str]:
        """document_id della richiesta originale, se c'è."""
        return self.request.document_id

    def to_dict(self) -> Dict[str, Any]:
        """Serializza l'esito, richiesta e risposta comprese."""
        return {
            "request_id": self.request.request_id,
            "document_id": self.request.document_id,
            "status": self.status.value,
            "attempts": self.attempts,
            "total_duration_seconds": round(self.total_duration_seconds, 4),
            "response": self.response.to_dict() if self.response else None,
            "error": self.error,
            "error_type": self.error_type,
            "is_transient": self.is_transient,
            "request": self.request.to_dict(),
        }


class BatchMetrics(BaseModel):
    """Metriche aggregate di rendimento e affidabilità dell'elaborazione batch."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_retries: int = 0
    total_tokens: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    wall_clock_seconds: float = 0.0
    avg_latency_seconds: float = 0.0
    min_latency_seconds: float = 0.0
    max_latency_seconds: float = 0.0
    requests_per_second: float = 0.0
    tokens_per_second: float = 0.0
    error_breakdown: Dict[str, int] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _counts_balance(self) -> BatchMetrics:
        """I conteggi non sono negativi e successi più fallimenti fanno il totale."""
        for field_name in (
            "total_requests",
            "successful_requests",
            "failed_requests",
            "total_retries",
            "total_tokens",
            "prompt_tokens",
            "completion_tokens",
        ):
            field_value = getattr(self, field_name)
            if field_value < 0:
                raise ValueError(f"{field_name}: vale {field_value}, non può essere negativo")
        finished = self.successful_requests + self.failed_requests
        if finished != self.total_requests:
            raise ValueError(
                f"total_requests: vale {self.total_requests} ma successful_requests "
                f"({self.successful_requests}) + failed_requests ({self.failed_requests}) fa {finished}"
            )
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Serializza le metriche del batch."""
        return self.model_dump(mode="python")


class BatchResult(BaseModel):
    """Risultato complessivo dell'esecuzione di un batch di richieste.

    L'elenco `results` mantiene l'ordine d'invio, `results_by_id` indicizza per request_id.
    """

    results: List[TaskExecutionResult]
    metrics: BatchMetrics
    results_by_id: Dict[str, TaskExecutionResult] = Field(
        default_factory=dict, exclude=True
    )

    @model_validator(mode="after")
    def _index_results(self) -> BatchResult:
        """Riempie results_by_id a partire da results."""
        self.results_by_id = {res.request.request_id: res for res in self.results}
        return self

    def successful(self) -> List[TaskExecutionResult]:
        """Restituisce esclusivamente le elaborazioni andate a buon fine."""
        return [r for r in self.results if r.is_success]

    def failed(self) -> List[TaskExecutionResult]:
        """Restituisce le elaborazioni fallite con le relative motivazioni d'errore."""
        return [r for r in self.results if not r.is_success]

    def get_by_document(self, document_id: str) -> List[TaskExecutionResult]:
        """Raggruppa le chiamate associate a uno specifico documento sorgente."""
        return [r for r in self.results if r.request.document_id == document_id]

    def to_dict(self) -> Dict[str, Any]:
        """Serializza metriche ed elenco ordinato dei risultati."""
        return {
            "metrics": self.metrics.to_dict(),
            "results": [r.to_dict() for r in self.results],
        }

    def save_json(self, destination_path: Union[str, Path]) -> None:
        """Persiste il risultato completo del batch in formato JSON."""
        target = Path(destination_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
