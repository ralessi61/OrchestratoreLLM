"""Pipeline di produzione notturna per l'elaborazione di batch di documenti.

Risolve lo scenario del cliente:
- Riceve un batch di documenti di grandi dimensioni;
- Esegue in concorrenza controllata le 3 chiamate per documento:
  1. Classificazione
  2. Riassunto
  3. Estrazione campi
- Applica Rate Limiting e Retry adattivo su eventuali HTTP 429;
- Salva i checkpoint incrementali in tempo reale su JSONL per non perdere mai il lavoro svolto;
- Raggruppa i risultati per documento e genera il report finale con le metriche.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

# Inclusione path di progetto
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from orchestratore_llm import (
    BatchResult,
    LLMOrchestrator,
    LLMRequest,
    LLMTaskType,
    RetryPolicy,
    TaskExecutionResult,
    TokenBucketRateLimiter,
    configure_logging,
    create_default_registry,
)


class CheckpointManager:
    """Gestore della persistenza in streaming dei singoli task completati."""

    def __init__(self, checkpoint_file: Path) -> None:
        self.checkpoint_file = checkpoint_file
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    async def save_checkpoint(self, result: TaskExecutionResult) -> None:
        """Accoda l'esito della chiamata sul file JSONL."""
        line = json.dumps(result.to_dict(), ensure_ascii=False) + "\n"
        with self._lock:
            with open(self.checkpoint_file, "a", encoding="utf-8") as f:
                f.write(line)


def build_document_batch(num_documents: int, provider_name: str) -> List[LLMRequest]:
    """Costruisce le richieste per il batch notturno (3 chiamate LLM per documento)."""
    requests: List[LLMRequest] = []

    for i in range(1, num_documents + 1):
        doc_id = f"DOC-2026-{i:05d}"
        doc_text_sample = (
            f"Fattura fornitura servizi cloud e licenze software #FT-{i:04d}. "
            f"Importo totale imponibile: {120.0 + (i * 15.5):.2f} EUR. Scadenza pagamento: 30gg."
        )

        # 1. Chiamata di Classificazione
        requests.append(
            LLMRequest(
                prompt=f"Determina categoria e priorità per: {doc_text_sample}",
                task_type=LLMTaskType.CLASSIFICATION,
                document_id=doc_id,
                provider=provider_name,
                metadata={"step": 1, "doc_index": i},
            )
        )

        # 2. Chiamata di Riassunto
        requests.append(
            LLMRequest(
                prompt=f"Genera abstract sintetico per: {doc_text_sample}",
                task_type=LLMTaskType.SUMMARY,
                document_id=doc_id,
                provider=provider_name,
                metadata={"step": 2, "doc_index": i},
            )
        )

        # 3. Chiamata di Estrazione Campi Strutturati
        requests.append(
            LLMRequest(
                prompt=f"Estrai entità fiscali, date e totali da: {doc_text_sample}",
                task_type=LLMTaskType.FIELD_EXTRACTION,
                document_id=doc_id,
                provider=provider_name,
                metadata={"step": 3, "doc_index": i},
            )
        )

    return requests


def consolidate_documents(batch_result: BatchResult) -> Dict[str, Dict[str, Any]]:
    """Raggruppa le risposte delle tre chiamate per ciascun documento sorgente."""
    documents: Dict[str, Dict[str, Any]] = {}

    for res in batch_result.results:
        doc_id = res.document_id or "UNKNOWN"
        if doc_id not in documents:
            documents[doc_id] = {
                "document_id": doc_id,
                "classification": None,
                "summary": None,
                "fields": None,
                "all_success": True,
                "errors": [],
            }

        entry = documents[doc_id]
        if not res.is_success:
            entry["all_success"] = False
            entry["errors"].append({"step": res.request.task_type, "error": res.error})
            continue

        resp = res.response
        if res.request.task_type == LLMTaskType.CLASSIFICATION:
            entry["classification"] = resp.parsed_content if resp else None
        elif res.request.task_type == LLMTaskType.SUMMARY:
            entry["summary"] = resp.content if resp else None
        elif res.request.task_type == LLMTaskType.FIELD_EXTRACTION:
            entry["fields"] = resp.parsed_content.get("extracted_fields") if (resp and resp.parsed_content) else None

    return documents


async def run_nightly_batch(
    num_documents: int = 40,
    max_concurrency: int = 15,
    rpm_limit: int = 360,
    simulate_rate_limits: bool = True,
) -> None:
    """Esegue la pipeline notturna completa."""
    run_day = time.strftime("%Y-%m-%d")
    output_dir = ROOT_DIR / f"output_{run_day}"
    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = output_dir / "nightly_execution.log"
    checkpoint_file = output_dir / "checkpoints.jsonl"
    consolidated_file = output_dir / "consolidated_documents.json"
    metrics_file = output_dir / "batch_metrics.json"

    # Reset file checkpoint precedente per pulizia
    if checkpoint_file.exists():
        checkpoint_file.unlink()

    # Inizializzazione logger strutturato (sia console che file)
    logger = configure_logging(level=logging.INFO, json_mode=False, log_file=log_file)
    logger.info("Avvio Pipeline Notturna Elaborazione Documenti")

    # grocky ha una quota di 4 chiamate al secondo e risponde 429; ammollo è quello stabile.
    registry = create_default_registry()
    provider = registry.get("grocky" if simulate_rate_limits else "ammollo")
    provider_name = provider.name

    # Componenti di protezione: Rate Limiter Token Bucket e Retry Policy con Jitter
    rate_limiter = TokenBucketRateLimiter(rate=rpm_limit, capacity=25, time_unit_seconds=60.0)
    retry_policy = RetryPolicy(
        max_retries=4,
        initial_delay=0.2,
        max_delay=3.0,
        backoff_factor=2.0,
        jitter=True,
    )

    checkpoint_mgr = CheckpointManager(checkpoint_file)

    orchestrator = LLMOrchestrator(
        registry=registry,
        max_concurrency=max_concurrency,
        rate_limiter=rate_limiter,
        retry_policy=retry_policy,
        checkpoint_callback=checkpoint_mgr.save_checkpoint,
    )

    # Creazione del batch (40 documenti x 3 chiamate = 120 chiamate LLM)
    requests = build_document_batch(num_documents=num_documents, provider_name=provider_name)
    logger.info(
        f"batch preparato: {num_documents} documenti, {len(requests)} chiamate LLM totali. "
        f"Concorrenza: {max_concurrency}, Rate Limiter: {rpm_limit} RPM."
    )

    start_batch_time = time.monotonic()
    batch_result = await orchestrator.execute_batch(requests)
    total_time = time.monotonic() - start_batch_time

    # Consolidamento dati per ciascun documento
    consolidated_data = consolidate_documents(batch_result)
    with open(consolidated_file, "w", encoding="utf-8") as f:
        json.dump(consolidated_data, f, indent=2, ensure_ascii=False)

    batch_result.save_json(metrics_file)

    # Stampa a terminale del report riassuntivo
    m = batch_result.metrics
    fully_completed_docs = sum(1 for d in consolidated_data.values() if d["all_success"])

    print("\n" + "=" * 75)
    print("           REPORT FINALE BATCH NOTTURNO")
    print("=" * 75)
    print(f" Documenti nel batch        : {num_documents}")
    print(f" Documenti 100% completati  : {fully_completed_docs}/{num_documents} ({fully_completed_docs/num_documents*100:.1f}%)")
    print(f" Chiamate LLM inviate       : {m.total_requests}")
    print(f" Chiamate riuscite          : {m.successful_requests} ({m.successful_requests/m.total_requests*100:.1f}%)")
    print(f" Chiamate fallite           : {m.failed_requests}")
    print(f" Retry recuperati su 429    : {m.total_retries}")
    print(f" Consumo token complessivo  : {m.total_tokens:,} (prompt: {m.prompt_tokens:,}, comp: {m.completion_tokens:,})")
    print(f" Tempo totale esecuzione    : {total_time:.2f} secondi")
    print(f" Latenza media chiamata     : {m.avg_latency_seconds*1000:.1f} ms")
    print(f" Throughput medio           : {m.requests_per_second:.1f} chiamate/sec | {m.tokens_per_second:.1f} token/sec")
    print(f" File Checkpoint JSONL      : {checkpoint_file}")
    print(f" Documenti Consolidati JSON : {consolidated_file}")
    print(f" Report Metriche JSON       : {metrics_file}")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    asyncio.run(
        run_nightly_batch(
            num_documents=30,
            max_concurrency=12,
            rpm_limit=300,
            simulate_rate_limits=True,
        )
    )
