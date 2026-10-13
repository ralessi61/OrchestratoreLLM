# Note di architettura

`orchestratore_llm` 0.1.0 esegue batch di chiamate verso un provider LLM. Non c'è un client HTTP: i tre provider del registro sono simulati e non serve una chiave API.

## Concorrenza

La strategia predefinita è `AsyncioStrategy`. Per ogni `execute_batch` crea un `asyncio.Semaphore(max_concurrency)`, lancia un task per richiesta e deposita l'esito nell'indice di partenza appena la coroutine termina (`asyncio.as_completed`). Se il batch viene annullato, i task ancora aperti vengono cancellati. `CancelledError` non è una `Exception`: `execute_request` non lo cattura.

`ThreadingStrategy(n)` è intercambiabile. `max_concurrency` è `max_workers` del pool, non un semaforo. Ogni thread ha un event loop proprio, perché `complete` è asincrono. I lock del rate limiter e del checkpoint di esempio sono `threading.Lock`, così restano utilizzabili anche da quei thread.

Il multiprocessing non è usato. Numeri e scelta per il cliente sono in `docs/CONCLUSIONI.md`.

## Provider di prova

| Nome | Comportamento |
| --- | --- |
| `ammollo` | Default. Latenza 10–40 ms, senza timeout e senza quota |
| `gwen` | Latenza fra 0,4 s e 1,4 s. Se il valore estratto supera 1 s, `NetworkTimeoutError` |
| `grocky` | 4 richieste al secondo, poi HTTP 429 con `retry_after` fino alla fine della finestra |

Un provider reale implementa `BaseLLMProvider` e va registrato. L'engine non va modificato.

## Dati e log

Richiesta, risposta, esito e metriche sono modelli Pydantic. Il prompt vuoto, una temperatura fuori da 0–2 e un totale token incoerente vengono rifiutati prima dell'esecuzione o prima della scrittura del risultato.

Il log dell'engine porta `request_id` e il tipo di eccezione, non il testo del prompt. Se si passa un file, ruota a 1 MiB e conserva cinque copie, in JSON. La console può restare colorata.

## Checkpoint

Il JSONL dell'esempio notturno registra i completamenti del run e viene azzerato all'avvio. Non riprende un job interrotto: gli id già scritti non vengono esclusi dal batch successivo.

## Test

`poetry run pytest` esegue 39 test. `poetry run python src/orchestratore_llm/tests/run_tests.py` ne esegue 30 (`unittest`). Le chiamate esterne coperte da `test_mocked_calls.py` usano `unittest.mock.AsyncMock`. Il controllo su `response.request_id` (`ResponseMismatchError`) è stato aggiunto dopo il test che lo richiedeva.
