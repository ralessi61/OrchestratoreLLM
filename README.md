# Orchestratore Chiamate LLM

Libreria Python modulare, installabile e resiliente per l'esecuzione concorrente di lotti massivi di richieste a provider LLM (Language Models).

Progettata specificamente per pipeline batch notturne che richiedono:
- **Concorrenza controllata** e non bloccante via `asyncio`;
- **Preservazione assoluta dell'ordine** e corrispondenza biunivoca con le richieste di partenza;
- **Rate limiting asincrono** (Token Bucket e Sliding Window);
- **Retry intelligente** con Exponential Backoff, Full Jitter e supporto all'header `Retry-After`;
- **Tassonomia degli errori** (distinzione tra errori transitori come 429/503 ed errori permanenti come 400/401);
- **Tolleranza ai guasti (Fault Tolerance)** con salvataggio incrementale (checkpointing streaming JSONL);
- **Supporto multi-provider** estendibile tramite Strategy Pattern e Registry (Open/Closed Principle);
- **Logging strutturato** (JSON o console a colori) e metriche di esecuzione dettagliate (tempi, token, throughput);
- **Supporto nativo a Poetry** per la gestione del package, delle dipendenze e dell'ambiente virtuale.

---

## Struttura del Progetto

```text
OrchestratoreChiamate/
├── pyproject.toml                     # Configurazione conforme a Poetry ([tool.poetry])
├── README.md                          # Guida all'installazione e comandi Poetry
├── docs/
│   ├── RELAZIONE_TECNICA.md           # Relazione approfondita con motivazione asyncio vs multiprocessing
│   └── SCHEDA_TECNICA.md              # Scheda tecnica esecutiva di rilascio
├── src/
│   └── orchestratore_llm/             # Package principale
│       ├── __init__.py                # Interfaccia pubblica del package
│       ├── models.py                  # Dataclass tipizzate (Request, Response, Metrics, BatchResult)
│       ├── exceptions.py              # Gerarchia delle eccezioni (Transient vs Permanent)
│       ├── rate_limiter.py            # Token Bucket e Sliding Window asincroni
│       ├── retry.py                   # Algoritmo di retry con Full Jitter e backoff
│       ├── engine.py                  # LLMOrchestrator: coordinamento concorrente
│       ├── logging_config.py          # Logging strutturato JSON e console colorata
│       ├── cli.py                     # Interfaccia da terminale
│       ├── providers/
│           ├── __init__.py
│           ├── base.py                # Interfaccia astratta BaseLLMProvider
│           ├── simulated.py           # Provider simulato per test e benchmark offline
│           └── registry.py            # Registry per registrazione plug-in provider
│       └── tests/                     # Suite di test collocata nel package
│           ├── __init__.py
│           ├── conftest.py
│           ├── run_tests.py           # Test runner autonomo
│           ├── test_engine.py
│           ├── test_models.py
│           ├── test_providers.py
│           ├── test_rate_limiter.py
│           └── test_retry.py
├── examples/
│   └── nightly_batch_pipeline.py      # Caso d'uso cliente: 3 chiamate per documento con checkpoint
└── output_<data>/                     # Una cartella per notte (checkpoints, report, log)
```

---

## Installazione con Poetry

Il progetto adotta **Poetry** come gestore di pacchetti e ambienti virtuali.

Dalla cartella principale del progetto:

```bash
# 1. Installa il pacchetto e configura l'ambiente virtuale
poetry install

# 2. (Facoltativo, PowerShell) Attiva l'ambiente virtuale
Invoke-Expression (poetry env activate)
```

---

## Esecuzione dei Test con Poetry

La suite comprende 16 test rilevati da pytest. Il runner basato sulla libreria standard `unittest` ne esegue 13; i tre test in stile funzione di `test_models.py` vengono rilevati da pytest:

```bash
# Esecuzione con il test runner integrato
poetry run python src/orchestratore_llm/tests/run_tests.py

# Oppure con pytest gestito da Poetry
poetry run pytest
```

Entrambi i comandi vanno eseguiti dalla cartella principale del progetto.

---

## Esecuzione della Pipeline Notturna

Per simulare l'elaborazione del lotto documentale notturno (30 documenti, 90 chiamate LLM eterogenee, gestione 429 e checkpointing in streaming):

```bash
poetry run python examples/nightly_batch_pipeline.py
```

L'esecuzione produrrà:
1. `output_<data>/checkpoints.jsonl`: avanzamento in tempo reale di ogni chiamata;
2. `output_<data>/consolidated_documents.json`: associazione dei 3 esiti per ciascun documento;
3. `output_<data>/batch_metrics.json`: report complessivo su tempi, token e throughput.

`<data>` è il giorno locale del lancio, in formato `AAAA-MM-GG` (es. `output_2026-10-10`). Un secondo lancio nello stesso giorno riscrive quella cartella.

---

## Esecuzione da Riga di Comando (CLI)

Il pacchetto registra l'eseguibile CLI `orchestratore-llm` all'interno dell'ambiente Poetry:

```bash
# Simulazione standard con provider di default ('ammollo')
poetry run orchestratore-llm simulate --docs 30 --concurrency 10 --rpm 300 --output report.json

# Simulazione resilienza con 'grocky' (include 25% di rate limit HTTP 429)
poetry run orchestratore-llm simulate --docs 30 --provider grocky --output report.json

# Simulazione alta velocità con 'gwen'
poetry run orchestratore-llm simulate --docs 30 --provider gwen --output report.json

# Con log in formato JSON per ingestione SIEM/APM
poetry run orchestratore-llm simulate --docs 10 --json-log --output report.json

# Esecuzione su file di richieste pre-esistente
poetry run orchestratore-llm run src/orchestratore_llm/tests/input_requests.json --output results.json --concurrency 15
```

Il file di esempio `src/orchestratore_llm/tests/input_requests.json` contiene una lista JSON valida. Puoi usarlo per provare il comando oppure sostituire il percorso con il tuo file di richieste, sempre codificato in UTF-8 senza BOM.

---

## Utilizzo come Libreria nel Codice Python

```python
import asyncio
from orchestratore_llm import (
    LLMOrchestrator,
    LLMRequest,
    LLMTaskType,
    create_default_registry,
    TokenBucketRateLimiter,
    RetryPolicy,
)

async def main():
    registry = create_default_registry()
    rate_limiter = TokenBucketRateLimiter(rate=180, time_unit_seconds=60.0)
    retry_policy = RetryPolicy(max_retries=3, initial_delay=0.5, jitter=True)

    orchestrator = LLMOrchestrator(
        registry=registry,
        max_concurrency=10,
        rate_limiter=rate_limiter,
        retry_policy=retry_policy,
    )

    requests = [
        LLMRequest(
            prompt="Classifica il testo: Contratto di fornitura",
            task_type=LLMTaskType.CLASSIFICATION,
            document_id="DOC-001",
        ),
        LLMRequest(
            prompt="Riassumi il documento DOC-001",
            task_type=LLMTaskType.SUMMARY,
            document_id="DOC-001",
        ),
        LLMRequest(
            prompt="Estrai importi e date dal documento DOC-001",
            task_type=LLMTaskType.FIELD_EXTRACTION,
            document_id="DOC-001",
        ),
    ]

    batch_result = await orchestrator.execute_batch(requests)

    # I risultati mantengono rigorosamente l'ordine originale d'invio
    for res in batch_result.results:
        print(f"Task: {res.request.task_type} -> Successo: {res.is_success}")

asyncio.run(main())
```
