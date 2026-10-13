# Scheda tecnica — Orchestratore di chiamate LLM

## 1. Panoramica

`orchestratore_llm` è un pacchetto Python (versione `0.1.0`) per elaborare batch di richieste verso LLM. Gestisce la concorrenza, limita la frequenza delle chiamate quando viene configurato un rate limiter e riprova le operazioni che falliscono per errori temporanei.

Il progetto nasce per l'elaborazione di documenti in batch, ad esempio un'esecuzione notturna che richiede classificazione, riassunto ed estrazione di campi per ciascun documento. L'uso di chiamate concorrenti riduce i tempi di attesa dovuti alla rete e ai servizi remoti; la gestione isolata degli errori evita che il fallimento di una richiesta interrompa l'intero batch.

## 2. Funzionalità principali

`LLMOrchestrator` riceve una lista di oggetti `LLMRequest` e restituisce un `BatchResult` con un esito per ciascuna richiesta, nello stesso ordine.

- **Concorrenza controllata:** la strategia predefinita è `AsyncioStrategy`. Un semaforo limita le richieste contemporanee (`max_concurrency`, default 15) e gli esiti vengono depositati man mano che le coroutine terminano. `ThreadingStrategy(n)` usa un pool di n thread; `max_concurrency` è la dimensione del pool. In entrambi i casi `results` resta nell'ordine di invio.
- **Limitazione della frequenza opzionale:** un rate limiter può regolare l'avvio delle chiamate nel tempo. Sono disponibili gli algoritmi Token Bucket e Sliding Window.
- **Retry selettivi:** gli errori transitori possono essere ritentati con backoff esponenziale e jitter. Gli errori permanenti terminano la richiesta senza ulteriori tentativi.
- **Isolamento degli errori:** il fallimento di una richiesta viene registrato nel relativo risultato; le altre richieste del batch proseguono.
- **Checkpoint tramite callback:** al completamento di una richiesta è possibile invocare un callback sincrono o asincrono.

Per codice asincrono si usa `await orchestrator.execute_batch(requests)`. `run_batch(requests)` è il wrapper sincrono basato su `asyncio.run`.

## 3. Struttura del pacchetto

Il codice si trova in `src/orchestratore_llm`.

| Modulo | Responsabilità |
| --- | --- |
| `models.py` | Modelli Pydantic di richiesta, risposta, esito e metriche, con validazione in ingresso e in uscita |
| `exceptions.py` | Gerarchia degli errori permanenti e transitori |
| `engine.py` | Esecuzione concorrente, ordinamento dei risultati, metriche e callback di checkpoint |
| `concurrency.py` | `AsyncioStrategy` (semaforo, risultati man mano che arrivano, cancellazione) e `ThreadingStrategy` (pool di thread) |
| `decorators.py` | `@retry`, `@timing` e `@logged` sulla chiamata al provider |
| `rate_limiter.py` | Implementazioni Token Bucket e Sliding Window |
| `retry.py` | Policy e ciclo di retry |
| `providers/` | Contratto dei provider, registro e tre provider simulati |
| `logging_config.py` | Console colorata o JSON; file JSON con rotazione a 1 MiB (cinque copie) |
| `cli.py` | Comandi da terminale `simulate` e `run` |

Il progetto richiede Python `>=3.9,<4` e usa Poetry per la gestione dell'ambiente e delle dipendenze.

I tipi di attività previsti sono `classification`, `summary`, `field_extraction` e `generic`. Il tipo di attività non modifica il flusso di esecuzione dell'orchestratore; determina invece il contenuto prodotto dal simulatore e il modo in cui l'esempio notturno raggruppa i risultati per `document_id`.

## 4. Provider

Il progetto non include al momento un client HTTP per un servizio LLM reale. `create_default_registry()` registra tre provider simulati:

| Nome | Comportamento |
| --- | --- |
| `ammollo` | Provider predefinito. Latenza bassa (10–40 ms), senza timeout e senza quota |
| `gwen` | Lento. Latenza estratta fra 0,4 s e 1,4 s; se supera 1 s la chiamata va in timeout |
| `grocky` | Quota di 4 richieste al secondo. Oltre la quota risponde HTTP 429 con `retry_after` fino alla fine della finestra |

I simulatori sono usati dalla CLI e dai test. Per aggiungere un provider reale occorre implementare `BaseLLMProvider` (`name`, `complete`, `estimate_tokens`, `check_health`) e registrare l'istanza. L'orchestratore risolve il provider tramite `LLMRequest.provider`; se il nome non è specificato, usa il provider predefinito del registro.

## 5. Installazione, test ed esecuzione

Installazione delle dipendenze:

```powershell
poetry install
```

Esecuzione della suite configurata in `pyproject.toml`:

```powershell
poetry run pytest
```

Pytest cerca i test in `src/orchestratore_llm/tests` (39 test). Il runner `unittest` nello stesso package ne esegue 30; le 9 funzioni di `test_models.py` le raccoglie solo pytest:

```powershell
poetry run python src/orchestratore_llm/tests/run_tests.py
```

### Simulazione di un batch

Questo comando genera 90 richieste (30 documenti, tre attività per documento), con un massimo di 10 richieste concorrenti e un rate limit configurato a 300 richieste al minuto:

```powershell
poetry run orchestratore-llm simulate --docs 30 --concurrency 10 --rpm 300 --output report.json
```

È possibile selezionare `grocky` per osservare i 429 della quota (4 richieste al secondo) e `gwen` per latenza variabile e timeout. `--json-log` produce log JSON sulla console.

### Elaborazione da file

Per elaborare un file JSON contenente una lista di richieste:

```powershell
poetry run orchestratore-llm run input_requests.json --output results.json --concurrency 15
```

Il comando `run` valida il JSON con Pydantic prima di eseguire: il file deve essere una lista di richieste, e l'errore nomina il campo (`richiesta[i].campo`) insieme al motivo. Non configura un rate limiter: applica la concorrenza indicata, la policy di retry predefinita e nessun limite temporale alle chiamate.

### Esempio di batch notturno

`examples/nightly_batch_pipeline.py` mostra un flusso più completo. Usa il registro predefinito, seleziona `grocky` (quota di 4 richieste al secondo, poi HTTP 429) oppure `ammollo` per un'esecuzione stabile, configura un Token Bucket con capacità pari a 25 e scrive checkpoint in `output_<data>/checkpoints.jsonl`. Al termine produce i risultati consolidati per documento, le metriche del batch e `nightly_execution.log`.

All'avvio l'esempio elimina il file di checkpoint precedente. Il checkpoint registra i completamenti, ma non consente da solo di riprendere un'esecuzione interrotta: il processo non legge gli ID già presenti per escluderli dal nuovo batch.

### Confronto sullo stesso batch

`examples/benchmark_concorrenza.py` cronometra 24 richieste con latenza fissa di 50 ms, senza errori e senza rate limiter. La tabella è in `docs/RELAZIONE_TECNICA.md`. La scelta per il cliente è in `docs/CONCLUSIONI.md`.

## 6. Risultati e metriche

Per ogni indice `i`, `BatchResult.results[i]` corrisponde alla richiesta `requests[i]`, anche se le operazioni terminano in ordine diverso. Ogni `TaskExecutionResult` contiene la richiesta originale, lo stato (`success` o `failed`), l'eventuale risposta o errore, il numero di tentativi e la durata.

- `results_by_id` consente di recuperare il risultato tramite `request_id`.
- `get_by_document` raggruppa i risultati con lo stesso `document_id`, ad esempio le tre attività previste per un documento.
- Le metriche del batch includono successi, fallimenti, retry, token dichiarati dal provider, tempo totale, latenze minime/medie/massime e throughput di richieste e token.
- `save_json` salva il risultato del batch in formato JSON.

In ingresso `LLMRequest` rifiuta un prompt vuoto o oltre 100.000 caratteri, una `temperature` fuori da 0–2 e un `max_tokens` che non sia un intero positivo. In uscita `TokenUsage` richiede che `total_tokens`, se le parti sono valorizzate, coincida con `prompt_tokens + completion_tokens`. Uno stato `success` senza risposta, o `failed` senza testo di errore, non supera la validazione.

## 7. Checkpoint ed esempio notturno

Il motore non dipende dal file system: quando una richiesta termina, può invocare il callback configurato. Nell'esempio, `CheckpointManager` serializza `result.to_dict()` come una riga JSON e la aggiunge a `output_<data>/checkpoints.jsonl`. Il lock è un `threading.Lock`, quindi le scritture restano serializzate sia con le coroutine sia con i thread di `ThreadingStrategy`.

Il file è un registro dei completamenti del run corrente, non una coda di ripresa. Non viene letto per saltare le richieste già elaborate; inoltre il lock non coordina processi distinti che scrivono sullo stesso file. L'apertura e chiusura del file per ogni riga svuota il buffer Python, ma non equivale a `fsync`: un'interruzione durante la scrittura può lasciare incompleta l'ultima riga. Chi legge il JSONL dovrebbe gestire una riga finale troncata.

Al termine del batch, `consolidate_documents` raggruppa i risultati per `document_id` e popola `classification`, `summary` e `fields`. Se una o più attività falliscono, `all_success` è falso e l'errore viene registrato insieme al tipo di attività corrispondente.

## 8. Monitoraggio delle esecuzioni

Ogni directory `output_<data>` raccoglie gli artefatti di un'esecuzione. Per confrontare più notti, il flusso suggerito è archiviare una riga di metriche per batch in una tabella SQL, ad esempio `batch_runs` su PostgreSQL o SQLite, e collegare Grafana a tale sorgente.

Gli artefatti hanno scopi diversi:

| Artefatto | Uso |
| --- | --- |
| `batch_metrics.json` | Metriche aggregate del batch e dettaglio `error_breakdown`; utile per confrontare le esecuzioni |
| `consolidated_documents.json` | Individuazione dei documenti per cui `all_success` è falso |
| `nightly_execution.log` | Ricerca di eventi e dettagli operativi, come un `document_id` o un errore 429 |

Grafana può visualizzare le metriche aggregate tramite una sorgente SQL. Se si raccolgono i log con Loki, è possibile cercare eventi e dettagli operativi nello stesso intervallo temporale dei grafici. I report JSON rimangono artefatti da consultare per analisi puntuali; non sono la sorgente consigliata per i grafici storici.

Prometheus è progettato per raccogliere serie temporali da servizi che espongono metriche nel tempo. In questo caso il dato principale è il riepilogo prodotto a fine batch: per conservarlo e confrontare le esecuzioni è più diretto archiviarlo in una tabella SQL e leggerlo da Grafana.

## 9. Limiti e note operative

- **Concorrenza e frequenza sono controlli distinti.** Con `AsyncioStrategy` il semaforo limita quante richieste sono attive contemporaneamente; con `ThreadingStrategy` lo fa la dimensione del pool. Il rate limiter regola quante chiamate possono essere avviate nel tempo. Se il provider applica entrambe le quote, vanno configurati entrambi.
- **Capacità predefinita del Token Bucket.** Nella CLI `simulate`, se non si specifica una capacità, il bucket parte pieno con una capacità pari a `rate`. Con `--rpm 300`, quindi, le prime 300 acquisizioni possono essere immediate; nella raffica iniziale `max_concurrency` è l'altro controllo. L'esempio notturno imposta esplicitamente la capacità a 25.
- **Ripresa dei batch.** I checkpoint non implementano il recupero automatico dopo un'interruzione. La logica di lettura dei checkpoint e di esclusione delle richieste già completate deve essere aggiunta dal programma che avvia il job.
- **Stima dei token simulati.** Il simulatore usa una stima basata sulla lunghezza del testo (`len(testo) // 4`), non un tokenizer reale. Questi valori non rappresentano il consumo effettivo di un modello.
- **Log.** L'engine registra `request_id`, `document_id`, provider, tentativi e il tipo di eccezione. Non scrive il testo del prompt. Il file, se configurato, ruota a 1 MiB e conserva cinque copie. La CLI e l'esempio notturno stampano ancora a video il riepilogo numerico di fine batch.
- **Metriche variabili.** Durata, throughput e numero di errori recuperati dipendono dai parametri di esecuzione e dalla casualità del simulatore. I valori osservati non sono garanzie prestazionali.
