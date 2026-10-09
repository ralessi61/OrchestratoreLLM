"""Modelli di dati tipizzati per richieste, risposte, esiti e metriche batch."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


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


@dataclass(frozen=True)
class LLMRequest:
    """Richiesta immutabile destinata all'orchestratore per l'inoltro al provider."""

    prompt: str
    task_type: Union[LLMTaskType, str] = LLMTaskType.GENERIC
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    document_id: Optional[str] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        """Serializza la richiesta in dizionario nativo."""
        data = asdict(self)
        if isinstance(self.task_type, LLMTaskType):
            data["task_type"] = self.task_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> LLMRequest:
        """Ricostruisce un'istanza da dizionario."""
        task_type_raw = data.get("task_type", LLMTaskType.GENERIC)
        try:
            task_type = LLMTaskType(task_type_raw)
        except ValueError:
            task_type = str(task_type_raw)

        return cls(
            prompt=data["prompt"],
            task_type=task_type,
            request_id=data.get("request_id") or str(uuid.uuid4()),
            document_id=data.get("document_id"),
            provider=data.get("provider"),
            model=data.get("model"),
            parameters=data.get("parameters", {}),
            metadata=data.get("metadata", {}),
            created_at=data.get("created_at", time.time()),
        )


@dataclass
class TokenUsage:
    """Riepilogo del consumo di token associato a una chiamata LLM."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def __post_init__(self) -> None:
        if self.total_tokens == 0 and (self.prompt_tokens > 0 or self.completion_tokens > 0):
            self.total_tokens = self.prompt_tokens + self.completion_tokens

    def to_dict(self) -> Dict[str, int]:
        return asdict(self)


@dataclass
class LLMResponse:
    """Risultato restituito da un provider a seguito di una richiesta riuscita."""

    request_id: str
    content: str
    provider: str
    model: str
    document_id: Optional[str] = None
    parsed_content: Optional[Any] = None
    usage: TokenUsage = field(default_factory=TokenUsage)
    latency_seconds: float = 0.0
    raw_response: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["usage"] = self.usage.to_dict()
        return data


@dataclass
class TaskExecutionResult:
    """Rappresentazione completa dell'esito di una richiesta elaborata dall'orchestratore.
    
    Conserva il puntatore alla richiesta originale, garantendo l'assoluta
    corrispondenza biunivoca anche in presenza di esecuzioni asincrone fuori ordine.
    """

    request: LLMRequest
    status: ExecutionStatus
    response: Optional[LLMResponse] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    is_transient: bool = False
    attempts: int = 1
    total_duration_seconds: float = 0.0

    @property
    def is_success(self) -> bool:
        return self.status == ExecutionStatus.SUCCESS

    @property
    def request_id(self) -> str:
        return self.request.request_id

    @property
    def document_id(self) -> Optional[str]:
        return self.request.document_id

    def to_dict(self) -> Dict[str, Any]:
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


@dataclass
class BatchMetrics:
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
    error_breakdown: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BatchResult:
    """Risultato complessivo dell'esecuzione di un lotto di richieste.
    
    L'elenco `results` mantiene rigidamente l'ordine d'invio della collezione originale,
    mentre `results_by_id` offre una vista indicizzata per accesso immediato O(1).
    """

    results: List[TaskExecutionResult]
    metrics: BatchMetrics
    results_by_id: Dict[str, TaskExecutionResult] = field(init=False)

    def __post_init__(self) -> None:
        self.results_by_id = {res.request.request_id: res for res in self.results}

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
