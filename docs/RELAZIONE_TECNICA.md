# Come è fatto l'orchestratore

Note per chi deve modificare `src/orchestratore_llm` o attaccarci un provider vero. 

## Asyncio

`complete` del provider fa `await` (nel simulatore è uno `asyncio.sleep` al posto della HTTP). Finché la coroutine è sospesa, l'event loop può far avanzare le altre.

I thread di sistema gestiscono l'I/O, ma ognuno ha uno stack e, appena si condividono metriche e il file di checkpoint, servono lock. Con asyncio il lock serve lo stesso sul rate limiter e sul file, però i punti in cui il codice cede il controllo sono gli `await`, il passaggio tra coroutine avviene normalmente quando si incontra un await.

Attenzione che ad esempio una time.sleep(2) si blocca tutto l'orchestratore per due secondi. Invece `await asyncio.sleep(2)` sospende solo la coroutine corrente, e l'event loop può far girare le altre.

## Schema di una richiesta

`execute_request` fa sempre cinque passi, anche quando la richiesta poi fallisce.

1. `registry.get(request.provider)`. Se il nome non c'è, l'eccezione viene catturata e la funzione ritorna un `TaskExecutionResult` con `status=failed`, `is_transient=False`, un tentativo. Il batch non si interrompe.
2. `_call_provider` è decorata con `@logged`, `@timing` e `@retry`. Il più interno è il retry, che chiama `execute_with_retry`. Se c'è un rate limiter, `acquire(1)` sta dentro la coroutine, quindi ogni tentativo consuma un permesso. Poi `provider.complete(request)`. Se `response.request_id` non coincide con quello della richiesta, viene sollevato `ResponseMismatchError` (permanente, nessun altro tentativo).
3. A fine chiamata il numero di tentativi è `last_attempts` sul wrapper del retry, letto con `wrapped_attr`. Vale 1 se il primo tentativo è andato a buon fine.
4. Successo o fallimento diventano comunque un `TaskExecutionResult`. L'eccezione non esce da `execute_request`, salvo bug fuori da quei `try`.
5. Se è stato passato `checkpoint_callback`, viene chiamato con il risultato. Se ritorna una coroutine, viene attesa. Un'eccezione del callback viene loggata e ingoiata: il risultato della richiesta resta valido, il checkpoint di quella riga no.

Il log usa `extra` con `request_id`, `document_id`, `provider`, e in retry anche `attempt` e `retry_delay`. Il formatter di console stampa solo un sottoinsieme di quelle chiavi; il formatter JSON le mette tutte sotto `context`.

## Batch e ordine dei risultati

`execute_batch` su lista vuota ritorna subito, con metriche a zero.

Altrimenti misura `time.monotonic()` all'ingresso e alloca `ordered_results = [None] * N`. Ogni job è `execute_request` della richiesta in quella posizione. `strategy.run` esegue i job e, appena uno termina, chiama il callback che scrive `ordered_results[index]`.

La strategia predefinita è `AsyncioStrategy`. Per ogni `execute_batch` crea un `asyncio.Semaphore(max_concurrency)` locale: due batch sovrapposti sullo stesso orchestratore non condividono il semaforo. Il rate limiter sì, perché sta sull'istanza. I task sono creati tutti subito con `asyncio.create_task`; gli esiti arrivano da `asyncio.as_completed` e vengono scritti nell'indice di partenza, non nell'ordine di completamento. Su 90 o 120 chiamate il numero di task non pesa. Su decine di migliaia il costo è la lista di task già schedulati: a quel punto conviene un pool che pesca da una coda.

`CancelledError` non è una `Exception` (da Python 3.8 deriva da `BaseException`). `execute_request` non lo cattura. `AsyncioStrategy`, alla cancellazione, annulla i task ancora aperti e rilancia. `ThreadingStrategy` segnala lo stop ai worker e annulla i task ancora in esecuzione.

`ThreadingStrategy(n)` è l'alternativa sullo stesso orchestratore: un `ThreadPoolExecutor` con `max_workers=n`. Ogni thread apre un event loop proprio, perché `complete` è async. `max_concurrency` è la dimensione del pool, non un semaforo. I lock del rate limiter sono `threading.Lock`, altrimenti non si possono condividere fra quei loop.

`LLMRequest` è un modello Pydantic frozen. Il risultato tiene il riferimento alla richiesta, quindi `document_id` e `request_id` restano quelli di ingresso. `results_by_id` è riempito da un validatore del modello, con chiave `request_id`. Se due richieste hanno lo stesso id, in mappa resta l'ultima.

`run_batch` non va chiamato da una coroutine già in esecuzione: `asyncio.run` vuole un loop nuovo.

## Errori

La base è `OrchestratorError`. Da lì si eredita.

Permanenti, niente retry: `InvalidRequestError`, `AuthenticationError`, `AuthorizationError`, `ModelNotFoundError`, `ContextWindowExceededError`, `ProviderNotFoundError`. Il commento nel codice li associa ai 400/422, 401, 403, 404 e allo sforamento della finestra di contesto. La classe non contiene lo status HTTP: è il provider che deve gestire il tipo giusto. Se un client HTTP fa raise di un `httpx.HTTPStatusError` , `is_retryable` è false il retry si ferma e rilancia subito l’errore. Non viene fatto un altro tentativo.

Transitori: `RateLimitError` (ha `retry_after` opzionale, in secondi), `ServerTemporarilyUnavailableError`, `NetworkTimeoutError`, `ConnectionDroppedError`.

`RetryPolicy.is_retryable` rifiuta prima qualunque `PermanentError`, anche se fosse  nella tupla `retryable_exceptions`. Poi controlla `isinstance` sulla tupla, default `(TransientError,)`.

## Retry

Default: 3 retry, delay iniziale 1 s, `max_delay` 30 s, fattore 2, jitter on.

Il loop in `execute_with_retry` usa un contatore `attempt` che parte da 0. Al successo ritorna `(risultato, attempt + 1)`, quindi 1 se è riuscita al primo tentativo. Se l'eccezione non è ritentabile, oppure `attempt >= max_retries`, rilancia l'eccezione originale. Con `max_retries=3` le chiamate a `operation` sono quattro: la prima più tre ripetizioni.

Il delay, quando non c'è un `retry_after`:

```text
ceiling = min(max_delay, initial_delay * backoff_factor ** attempt)
attesa  = random.uniform(0, ceiling)     se jitter
attesa  = ceiling                        altrimenti
```

`attempt` qui è ancora il valore prima dell'incremento, quindi il primo retry usa l'esponente 0 e il max  vale `initial_delay`. Il secondo usa l'esponente 1, e così via. Con jitter l'attesa può essere 0. Va bene per i test; contro un 429 vero, uno zero di fatto raddoppia subito il carico. Sui 429 il ramo che conta è l'altro: se l'eccezione è `RateLimitError` e `retry_after` non è `None`, il delay è `max(0.1, retry_after + 0.1)` se il provider comunica quanto aspettare prima di riprovare (retry_after), il programma usa quel tempo e aggiunge 0,1 secondi di margine.

`on_retry(attempt + 1, exc, delay)` scatta solo quando un retry è stato deciso. L'ultimo fallimento, quello che esaurisce i tentativi, non passa dal callback: si rilancia prima.

Se tutti i tentativi falliscono, il conteggio li riporta correttamente. Per esempio, dopo 4 tentativi falliti, il risultato e' 4, non viene perso il primo.
È importante se quel conteggio viene usato per calcolare costi o attivare allarmi.

La CLI `simulate` restringe la policy: 4 retry, delay iniziale 0,15 s, `max_delay` 3 s. L'esempio notturno usa 4 retry, 0,2 s, `max_delay` 3 s. Il default della classe, 1 s fino a 30 s, è quello di `run` se si costruisce l'orchestratore senza passare una policy.

## Rate limit

Ci sono due controlli, uno dopo l'altro, se il rate limiter è configurato:
1. Concorrenza: con `AsyncioStrategy` un semaforo riserva uno slot per la richiesta; con `ThreadingStrategy` lo slot è un worker del pool. In entrambi i casi il retry resta in quello slot anche durante il backoff.
2. Rate limiter: aspetta il permesso prima di chiamare il provider. Limita quante richieste partono nel tempo.
Poi parte `complete`. Al tentativo successivo si chiede un nuovo permesso al rate limiter.

### Token bucket

`rate` è il numero di permessi nell'intervallo `time_unit_seconds` (default 60, quindi si ragiona in RPM). Internamente diventa `rate_per_sec = rate / time_unit_seconds`.

Il bucket è come un salvadanaio di permessi di effettuare una chiamata al provider. Ogni chiamata consuma un permesso; col tempo il bucket ne recupera altri.
- rate è la velocità con cui si recuperano i permessi.
- capacity è quanti permessi può contenere il salvadanaio, cioè quante chiamate possono partire subito come burst.
Se non si specifica capacity, il codice la imposta uguale a rate, non a rate_per_sec. Quindi, con 300 RPM, parte con 300 permessi già disponibili e le prime 300 chiamate possono passare subito. In quel momento `max_concurrency` (semaforo o pool) è ciò che limita quante chiamate procedono insieme. Se il provider ha una quota più stretta, conviene impostare una capacity più bassa, come 25 nell’esempio.
Il lock protegge il conteggio dei permessi, ma non viene tenuto durante l'attesa. Se a una chiamata mancano permessi, il codice calcola quanto attendere, libera il lock ed esegue `asyncio.sleep`. Nel frattempo le altre chiamate possono controllare e consumare i permessi disponibili.
Quando l'attesa termina, la chiamata controlla di nuovo quanti permessi ci sono. Due chiamate possono uscire dall'attesa insieme, ma solo una trova i permessi necessari: l'altra ricalcola e riprova. Per questo il controllo è dentro un ciclo.

Il rate limiter funziona come un contenitore di token. Ogni chiamata ne consuma alcuni; col tempo se ne ricaricano, fino al massimo stabilito da capacity.
Se chiedi più token di quanti il contenitore possa mai averne, la richiesta è impossibile: acquire(tokens) solleva subito ValueError. Per esempio, con capacity=5, chiedere 6 token non può riuscire. È un errore di configurazione, non temporaneo: il sistema non aspetta né riprova, ma restituisce un risultato fallito.

### Sliding window

`SlidingWindowRateLimiter(max_requests, window_seconds=60)` tiene la lista dei timestamp delle acquire accettate e elimina quelli più vecchi. Se non c'è posto, attende fino alla scadenza del timestamp più vecchio, più 5 ms, fuori dal lock, e riprova.

Questo rate limiter funziona con una finestra mobile: tiene l’orario delle chiamate recenti e, prima di ammetterne una, controlla quante ne sono avvenute nella finestra. Perciò non consente di superare max_requests chiamate in quel periodo, anche se le chiamate arrivano tutte insieme. Per poche centinaia di chiamate al minuto, controllare la lista degli orari a ogni richiesta è semplice e va bene.
C’è però un caso limite: se si chiede un numero di chiamate (tokens) maggiore di max_requests, la richiesta non potrà mai essere accolta. Il metodo dovrebbe rifiutarla subito, ma non lo fa: con la finestra vuota può provare a leggere il primo orario da una lista vuota e causare un IndexError.
Al momento il package chiama il metodo sempre con tokens=1, quindi non incontra il problema. Ma siccome il metodo è pubblico, un chiamante esterno potrebbe passare un valore più grande di max_requests. In quel caso, il codice alza un IndexError, che non è un errore transitorio: la chiamata non può riuscire. Il chiamante deve gestire l’eccezione e non riprovare.

## Provider e registry

ProviderRegistry è come una rubrica: associa un nome a un provider, per esempio "openai" alla relativa istanza.
- Il primo provider registrato diventa quello predefinito, a meno che un altro non venga indicato con set_as_default=True.
- get(None) restituisce il predefinito.
- get("nome") restituisce il provider con quel nome. Se non esiste, alza ProviderNotFoundError, un errore che non viene ritentato.
Simulatore
`SimulatedLLMProvider` simula la chiamata a un modello. Se ha una quota (`max_requests_per_window`), conta le richieste accettate nella finestra: oltre la quota solleva `RateLimitError` e `retry_after` è il tempo che manca alla scadenza della più vecchia, senza attendere la latenza. Poi estrae un tempo casuale in `latency_range`. Se `timeout_after` è impostato e il tempo estratto lo supera, attende solo fino alla soglia e solleva `NetworkTimeoutError`. Altrimenti attende il tempo estratto.
L’ordine dei controlli successivi è questo:
1. Se è configurato per fallire i primi N tentativi di quell’ID richiesta, solleva RateLimitError. Il contatore resta nella stessa istanza del provider, quindi i retry successivi possono poi riuscire.
2. Se il prompt è vuoto o contiene solo spazi, solleva InvalidRequestError.
3. Se il prompt contiene FAIL_PERMANENT o FAIL_AUTH, simula un errore permanente o di autenticazione. Sono parole speciali per i test: se compaiono per caso nel prompt, fanno scattare l’errore.
4. Poi estrae a sorte gli errori configurati: prima 429, poi 503, poi 400. Se scatta il 429, per quella chiamata il simulatore non controlla più gli altri due.
Se non viene simulato nessun errore, genera una risposta simulata. Classificazione ed estrazione producono dati strutturati; il riassunto produce testo e un piccolo dizionario. Anche `raw_response` contiene dati simulati.

Nel registro predefinito i tre comportamenti sono questi:

- `ammollo`, il default: latenza 10–40 ms, senza timeout e senza quota.
- `gwen`: latenza estratta fra 0,4 s e 1,4 s. Se il valore supera `timeout_after` (1 s) la chiamata alza `NetworkTimeoutError` dopo quell'attesa.
- `grocky`: al massimo 4 richieste al secondo. Oltre la quota solleva `RateLimitError` e `retry_after` è il tempo che manca alla fine della finestra. Il 429 non è casuale.

La stima del simulatore è grossolana: circa un token ogni quattro caratteri, con un minimo di uno per il testo non vuoto. Le metriche del batch sommano i token delle richieste riuscite. Sono numeri di prova, non il consumo effettivo di un modello.
`check_health` del simulatore restituisce sempre `True`; al momento l'engine non lo chiama.
Quando si collega un provider reale, `complete` deve convertire le risposte del servizio nelle eccezioni del progetto, copiare l'eventuale `Retry-After` in `RateLimitError` e usare i dati reali per `TokenUsage`, se disponibili. Il nome del provider deve essere stabile, perché serve a identificarlo nel registro e nelle richieste.

## Checkpoint

Un checkpoint è il salvataggio dei risultati man mano che il batch procede, così si possono consultare anche prima che tutte le richieste siano finite.
Il motore non gestisce direttamente i file: chiama una funzione quando una richiesta termina. Nell'esempio, `CheckpointManager` aggiunge il risultato a `checkpoints.jsonl`, una riga JSON per richiesta completata. Il lock è un `threading.Lock`: copre sia le coroutine sia i thread di `ThreadingStrategy`. Il file viene chiuso dopo ogni riga; se il programma si interrompe proprio mentre scrive, l'ultima riga può restare incompleta.
Il file registra i risultati del run corrente e non permette da solo di riprendere il lavoro. All'avvio del batch l'esempio lo cancella; il motore non legge gli id già salvati per saltare le richieste completate. Per riprendere davvero servirebbe quella logica. Il lock non coordina processi diversi.
Quando il batch è terminato, `consolidate_documents` raccoglie i risultati per documento e compila classificazione, riassunto ed estrazione. Se uno di questi passaggi è fallito, registra l'errore e il tipo di attività.

## Metriche

`_calculate_metrics` distingue due orologi.

`wall_clock_seconds` è il tempo di `execute_batch`, dall'avvio della strategia alla sua chiusura. Include attese di semaforo o del pool, bucket e backoff. `requests_per_second` è `total / wall_clock` su tutte le righe, fallite comprese. Sotto un decimillesimo di secondo il divisore viene alzato a `0.0001` per non dividere per zero.

La latenza min/media/max usa `response.latency_seconds` delle sole risposte riuscite. Nel simulatore quel numero è il tempo interno di `complete` (l'attesa di `asyncio.sleep` più il resto della funzione), non `total_duration_seconds` del task. Quest'ultimo comprende i retry e le attese di rate limit, e finisce nel JSON di riga, ma non nella media. Un batch che ha passato metà del tempo in backoff può quindi mostrare una latenza media bassa e un wall clock alto. 

I token si sommano solo sui successi. I fallimenti incrementano `error_breakdown` con il nome della classe (`error_type`), oppure `UnknownError` se manca.

`total_retries` è la somma di `max(0, attempts - 1)`. `attempts` include il primo tentativo, anche quando i retry si esauriscono: il valore arriva da `last_attempts` del decoratore `@retry`.

## Log

configure_logging prepara il sistema di log del package:
- Imposta il logger principale orchestratore_llm, rimuovendo gli handler già collegati e impedendo ai messaggi di propagarsi ai logger superiori.
- I moduli, come orchestratore_llm.engine, usano logger con nomi più specifici. I loro messaggi risalgono al logger principale, che li gestisce.
- Se richiami configure_logging, la configurazione precedente viene sostituita. È utile nei test per ripartire da una configurazione pulita, ma può rimuovere un handler che un altro pezzo di codice aveva collegato allo stesso logger.
Con `json_mode=True`, la console mostra log JSON. Se si passa `log_file`, il file è sempre JSON, anche quando la console è colorata, ed è un `RotatingFileHandler`: a 1 MiB il corrente diventa `.1`, `.2`, … e ne restano al massimo cinque. `max_bytes=0` disattiva la rotazione. I colori della console usano codici ANSI e possono comparire anche quando l'output viene reindirizzato. I messaggi dell'engine portano `request_id`, `document_id` e il tipo di eccezione; non contengono il testo del prompt.

## Confronto sullo stesso batch

`examples/benchmark_concorrenza.py` prepara 24 `LLMRequest` e un `SimulatedLLMProvider` con `latency_range=(0.05, 0.05)`: ogni `complete` attende 50 ms e non fallisce. Non c'è rate limiter. La policy di retry ha `max_retries=0`. Il sequenziale chiama `execute_request` in un ciclo. Le altre righe passano da `execute_batch` con `AsyncioStrategy` o `ThreadingStrategy`, `max_concurrency` 1, 4, 12 e 24. Ogni cella è la mediana di tre misure con `time.perf_counter`. Una chiamata singola, scartata, precede la tabella.

Misure su questa macchina (Windows, 12 ottobre 2026):

| Modalità | max_concurrency | Mediana (s) | Min | Max |
| --- | ---: | ---: | ---: | ---: |
| sequenziale | — | 1,447 | 1,439 | 1,456 |
| asyncio | 1 | 1,461 | 1,447 | 1,476 |
| asyncio | 4 | 0,360 | 0,360 | 0,373 |
| asyncio | 12 | 0,122 | 0,117 | 0,125 |
| asyncio | 24 | 0,063 | 0,061 | 0,065 |
| threading | 1 | 1,479 | 1,452 | 1,500 |
| threading | 4 | 0,378 | 0,371 | 0,390 |
| threading | 12 | 0,154 | 0,153 | 0,156 |
| threading | 24 | 0,096 | 0,088 | 0,109 |

La lettura di queste misure, e la scelta per il batch del cliente, è in `docs/CONCLUSIONI.md`.

## Test

Dalla cartella principale:

- `poetry run pytest` raccoglie 39 test in `src/orchestratore_llm/tests`.
- `poetry run python src/orchestratore_llm/tests/run_tests.py` ne esegue 30, quelli scritti come `unittest.TestCase`. Le 9 funzioni di `test_models.py` le vede solo pytest.

Oltre ai casi sull'engine, sul retry e sul rate limiter, la suite copre i decoratori, la rotazione del file di log, i tre comportamenti dei provider registrati e la chiamata esterna sostituita con `unittest.mock.AsyncMock`. `test_response_for_another_request` è stato scritto prima del controllo su `response.request_id`: la prima esecuzione accettava una risposta con l'id di un'altra richiesta. Il controllo, aggiunto dopo, solleva `ResponseMismatchError`.

Nei test del simulatore gli errori si impostano in modo prevedibile: primi N tentativi di una richiesta, parole `FAIL_PERMANENT` e `FAIL_AUTH` nel prompt, `timeout_after` sotto la latenza estratta, oppure una quota `max_requests_per_window`. Le probabilità di errore casuale restano a zero, così l'esito non dipende dal caso.