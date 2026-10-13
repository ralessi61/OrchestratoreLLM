# Orchestratore Chiamate LLM

Libreria Python modulare, installabile e resiliente per l'esecuzione concorrente di batch di richieste di grandi dimensioni a provider LLM (Language Models).

Progettata specificamente per pipeline batch notturne che richiedono:
- **Concorrenza controllata** e non bloccante via `asyncio`;
- **Preservazione assoluta dell'ordine** e corrispondenza biunivoca con le richieste di partenza;
- **Rate limiting asincrono** (Token Bucket e Sliding Window);
- **Retry intelligente** con Exponential Backoff, Full Jitter e supporto all'header `Retry-After`;
- **Tassonomia degli errori** (distinzione tra errori transitori come 429/503 ed errori permanenti come 400/401);
- **Tolleranza ai guasti (Fault Tolerance)** con salvataggio incrementale (checkpointing streaming JSONL);
- **Supporto multi-provider** estendibile tramite Strategy Pattern e Registry (Open/Closed Principle);
- **Logging strutturato** (console a colori o JSON; file JSON ruotato a 1 MiB) e metriche di esecuzione (tempi, token, throughput);
- **Supporto nativo a Poetry** per la gestione del package, delle dipendenze e dell'ambiente virtuale.

## Concorrenza, GIL e processi

Con la strategia predefinita (`AsyncioStrategy`) il numero di chiamate in esecuzione lo decide un `asyncio.Semaphore` (`max_concurrency`): `complete` parte solo se c'è un posto libero. `ThreadingStrategy` usa lo stesso `max_concurrency` come dimensione del pool di thread; le due strategie si passano a `LLMOrchestrator` e sul medesimo batch restituiscono i risultati nello stesso ordine. Gli `asyncio.sleep` nel codice sono la latenza simulata del provider, il backoff del retry e l'attesa del rate limiter quando i permessi al minuto sono finiti. Non sono loro a tenere basso il parallelismo.

In CPython il Global Interpreter Lock (GIL) consente a un solo thread per volta di eseguire il bytecode Python all'interno dello stesso interprete. Durante le operazioni di I/O bloccante, come le richieste di rete, il GIL viene generalmente rilasciato, permettendo ad altri thread di proseguire l'esecuzione. Di conseguenza, l'impiego di un thread pool consente comunque di sovrapporre le latenze delle chiamate al provider remoto: il GIL non costituisce, in questo scenario, il principale fattore limitante dell'I/O di rete.

Il suo impatto riguarda piuttosto le fasi di elaborazione eseguite in Python dopo il completamento delle richieste, come la deserializzazione delle risposte, l'aggiornamento dei contatori e la scrittura dei checkpoint. Queste operazioni, quando eseguite da thread appartenenti allo stesso interprete, sono soggette alla contesa per il GIL. Inoltre, l'accesso concorrente a strutture dati condivise, come bucket di aggregazione, o a file utilizzati per la persistenza dello stato, richiede un'opportuna strategia di sincronizzazione per garantire la correttezza dei dati e prevenire race condition.

Un approccio basato su asyncio e coroutine permette di gestire la concorrenza I/O mediante un event loop, concentrando il cambio di contesto nei punti di sospensione espliciti (await). Questo modello riduce l'overhead associato alla creazione e alla gestione di numerosi thread e facilita il coordinamento delle richieste concorrenti. Un semaforo asincrono consente inoltre di imporre un limite al numero di operazioni contemporaneamente attive, senza dover allocare un thread dedicato per ciascuna richiesta. È tuttavia importante distinguere la concorrenza dalla parallelizzazione: le coroutine non eseguono automaticamente codice Python in parallelo e, nello stesso event loop, il codice sincrono che non restituisce il controllo può bloccare l'avanzamento delle altre attività.

Il multiprocessing non riduce, di per sé, la latenza del provider remoto. Poiché ogni processo dispone di un interprete Python indipendente e di un proprio GIL, il multiprocessing consente di parallelizzare il lavoro CPU-bound. Nel caso in esame, tuttavia, il tempo di esecuzione è dominato dalle attese sulle chiamate remote, non da un carico computazionale locale significativo: la parallelizzazione del codice Python offre quindi benefici potenzialmente marginali rispetto alla gestione efficiente della concorrenza I/O.

L'adozione di processi separati introduce inoltre costi operativi e architetturali aggiuntivi. Per coordinare un limite globale alla concorrenza e mantenere un checkpoint condiviso, occorre predisporre meccanismi di comunicazione e sincronizzazione interprocesso. Il trasferimento di richieste e risposte può richiedere serializzazione e deserializzazione, ad esempio tramite pickle, oltre all'utilizzo di code, pipe o altri canali IPC (Inter-Process Communication). Questi costi aumentano la complessità del sistema senza eliminare il vincolo prestazionale dominante, rappresentato dalla latenza e dalla capacità di elaborazione del provider remoto.

La scelta per il batch notturno del cliente è in `docs/CONCLUSIONI.md`.

---

## Struttura del Progetto

```text
OrchestratoreChiamate/
├── pyproject.toml                     # Configurazione conforme a Poetry ([tool.poetry])
├── README.md                          # Guida all'installazione e comandi Poetry
├── docs/
│   ├── RELAZIONE_TECNICA.md           # Relazione approfondita con motivazione asyncio vs multiprocessing
│   ├── SCHEDA_TECNICA.md              # Scheda tecnica esecutiva di rilascio
│   └── CONCLUSIONI.md                 # Scelta per il batch notturno del cliente
├── src/
│   └── orchestratore_llm/             # Package principale
│       ├── __init__.py                # Interfaccia pubblica del package
│       ├── models.py                  # Modelli Pydantic: richiesta, risposta, esito, metriche
│       ├── exceptions.py              # Gerarchia delle eccezioni (Transient vs Permanent)
│       ├── rate_limiter.py            # Token Bucket e Sliding Window asincroni
│       ├── retry.py                   # Algoritmo di retry con Full Jitter e backoff
│       ├── decorators.py              # @retry, @timing, @logged impilabili sulla chiamata
│       ├── concurrency.py             # AsyncioStrategy e ThreadingStrategy
│       ├── engine.py                  # LLMOrchestrator: coordinamento concorrente
│       ├── logging_config.py          # Console colorata o JSON; file JSON con rotazione
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
│           ├── test_retry.py
│           ├── test_decorators.py
│           ├── test_logging.py
│           └── test_mocked_calls.py
├── examples/
│   ├── benchmark_concorrenza.py       # Stesso batch: sequenziale, asyncio, thread, più max_concurrency
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

La suite comprende 39 test rilevati da pytest. 30 sono metodi di `unittest.TestCase`; le 9 funzioni in `test_models.py` le raccoglie solo pytest:

```bash
# Esecuzione con il test runner integrato
poetry run python src/orchestratore_llm/tests/run_tests.py

# Oppure con pytest gestito da Poetry
poetry run pytest
```

Entrambi i comandi vanno eseguiti dalla cartella principale del progetto.

`test_mocked_calls.py` è una classe `unittest`. `complete` del provider registrato è sostituito con `unittest.mock.AsyncMock` tramite `patch.object`: orchestratore, semaforo e retry sono quelli del package, la chiamata no. I casi coperti sono l'esito positivo al primo tentativo, due `RateLimitError` seguiti dalla risposta, un `InvalidRequestError` senza ripetizione, il rispetto di `max_concurrency`, e un batch i cui completamenti arrivano in ordine diverso dall'invio. Il checkpoint osserva l'ordine di arrivo; `results` resta nell'ordine di invio e ogni risposta conserva il `request_id` della propria richiesta.

`test_response_for_another_request`, nello stesso file, è stato scritto prima del controllo sull'identificativo. La prima esecuzione falliva su `assertFalse(result.is_success)`: una risposta con `request_id` `REQ-ALTRUI` veniva accettata. Il confronto sta in `_call_provider`, subito dopo `complete`. Se i due id non coincidono viene sollevato `ResponseMismatchError`, errore permanente, quindi `complete` viene invocato una sola volta.

---

## Esecuzione della Pipeline Notturna

Per simulare l'elaborazione notturna (30 documenti, 90 chiamate, provider `grocky` con quota di 4 richieste al secondo, checkpoint su JSONL):

```bash
poetry run python examples/nightly_batch_pipeline.py
```

L'esecuzione scrive, nella cartella del giorno:
1. `checkpoints.jsonl`, una riga per chiamata completata;
2. `consolidated_documents.json`, i tre esiti di ciascun documento;
3. `batch_metrics.json`, tempi, token e throughput;
4. `nightly_execution.log`, log JSON ruotato a 1 MiB.

`<data>` è il giorno locale del lancio, in formato `AAAA-MM-GG` (es. `output_2026-10-12`). Un secondo lancio nello stesso giorno riscrive quella cartella. Con la quota di `grocky` una chiamata può esaurire i retry e restare `failed`: il batch prosegue e gli altri documenti vengono comunque consolidati.

---

## Confronto di concorrenza

Sullo stesso batch di 24 richieste, con un simulatore che attende sempre 50 ms e senza rate limiter:

```bash
poetry run python examples/benchmark_concorrenza.py
```

La tabella è in `docs/RELAZIONE_TECNICA.md`. La scelta che ne segue è in `docs/CONCLUSIONI.md`.

---

## Esecuzione da Riga di Comando (CLI)

Il pacchetto registra l'eseguibile CLI `orchestratore-llm` all'interno dell'ambiente Poetry:

```bash
# Simulazione standard con provider di default ('ammollo')
poetry run orchestratore-llm simulate --docs 30 --concurrency 10 --rpm 300 --output report.json

# Simulazione con 'grocky': al massimo 4 chiamate al secondo, poi HTTP 429
poetry run orchestratore-llm simulate --docs 30 --provider grocky --output report.json

# Simulazione con 'gwen': latenza fra 0,4 s e 1,4 s, timeout se supera 1 s
poetry run orchestratore-llm simulate --docs 30 --provider gwen --output report.json

# Con log in formato JSON per ingestione SIEM/APM
poetry run orchestratore-llm simulate --docs 10 --json-log --output report.json

# Esecuzione su file di richieste pre-esistente
poetry run orchestratore-llm run src/orchestratore_llm/tests/input_requests.json --output results.json --concurrency 15
```

Il file di esempio `src/orchestratore_llm/tests/input_requests.json` contiene una lista JSON valida. Puoi usarlo per provare il comando oppure sostituire il percorso con il tuo file di richieste, sempre codificato in UTF-8 senza BOM.

---

## Decoratori sulla chiamata

`@retry`, `@timing` e `@logged` stanno in `orchestratore_llm.decorators` e si compongono su una coroutine. L'ordine è quello di Python: il decoratore più in basso è il più interno. Sull'engine la pila è `@logged`, poi `@timing`, poi `@retry`: `@retry` wrappa solo `complete`, `@timing` misura anche le attese di backoff, `@logged` vede un solo esito.

`@retry(policy)` usa la stessa `RetryPolicy` di `execute_with_retry` (di default quella della classe). Dopo la chiamata, `last_attempts` sul wrapper dice quanti tentativi sono partiti. `@timing` scrive `last_elapsed` in secondi. Se ti serve il numero da fuori, `wrapped_attr` scende la catena di `__wrapped__` finché lo trova. `@logged` scrive sul logger del modulo della funzione, a DEBUG se non passi un livello; le righe con `request_id` restano quelle dell'engine. Se la chiamata fallisce ancora dopo i retry, `@logged` scrive un warning con il tipo dell'eccezione e la rilancia tale e quale.

`functools.wraps` copia `__name__` e `__doc__` a ogni strato, quindi dopo la pila la funzione conserva il nome originale.

## Validazione dei dati

Richiesta, risposta, esito della singola chiamata e risultato del batch sono modelli Pydantic. In ingresso `LLMRequest` scarta un prompt vuoto o più lungo di 100.000 caratteri, una `temperature` fuori dall'intervallo 0–2 e un `max_tokens` che non sia un intero positivo. In uscita `TokenUsage` rifiuta un `total_tokens` diverso dalla somma di prompt e completion, quando quella somma è positiva; uno stato `success` senza risposta, o `failed` senza motivo, non viene costruito.

Il comando `run` valida l'intero file prima di eseguire. Il file deve essere una lista. Il messaggio indica il campo e il motivo, per esempio `richiesta[1].prompt: manca ed è obbligatorio`.

```python
from orchestratore_llm import logged, retry, timing, RetryPolicy

@logged
@timing
@retry(RetryPolicy(max_retries=3, initial_delay=0.5))
async def completa(request):
    """Una chiamata, con retry e durata."""
    return await provider.complete(request)
```

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
