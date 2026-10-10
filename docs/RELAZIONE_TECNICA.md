# Come è fatto l'orchestratore

Note per chi deve modificare `src/orchestratore_llm` o attaccarci un provider vero. 

## Asyncio

`complete` del provider fa `await` (nel simulatore è uno `asyncio.sleep` al posto della HTTP). Finché la coroutine è sospesa, l'event loop può far avanzare le altre.

I thread di sistema gestiscono l'I/O, ma ognuno ha uno stack e, appena si condividono metriche e il file di checkpoint, servono lock. Con asyncio il lock serve lo stesso sul rate limiter e sul file, però i punti in cui il codice cede il controllo sono gli `await`, il passaggio tra coroutine avviene normalmente quando si incontra un await.

Attenzione che ad esempio una time.sleep(2) si blocca tutto l'orchestratore per due secondi. Invece `await asyncio.sleep(2)` sospende solo la coroutine corrente, e l'event loop può far girare le altre.

## Schema di una richiesta

`execute_request` fa sempre cinque passi, anche quando la richiesta poi fallisce.

1. `registry.get(request.provider)`. Se il nome non c'è, l'eccezione viene catturata e la funzione ritorna un `TaskExecutionResult` con `status=failed`, `is_transient=False`, un tentativo. Il batch non si interrompe.
2. Si chiude `_call_provider`. Se c'è un rate limiter, `acquire(1)` sta qui, quindi ogni tentativo consuma un permesso. Poi `provider.complete(request)`.
3. `execute_with_retry` gira su quella coroutine.
4. Successo o fallimento diventano comunque un `TaskExecutionResult`. L'eccezione non esce da `execute_request`, salvo bug fuori da quei `try`.
5. Se è stato passato `checkpoint_callback`, viene chiamato con il risultato. Se ritorna una coroutine, viene attesa. Un'eccezione del callback viene loggata e ingoiata: il risultato della richiesta resta valido, il checkpoint di quella riga no.

Il log usa `extra` con `request_id`, `document_id`, `provider`, e in retry anche `attempt` e `retry_delay`. Il formatter di console stampa solo un sottoinsieme di quelle chiavi; il formatter JSON le mette tutte sotto `context`.

## Batch e l'ordine delle richieste nella lista passata a execute_batch

`execute_batch` su lista vuota ritorna subito, metriche a zero.

Altrimenti:

- misura `time.monotonic()` all'ingresso;
- crea un `asyncio.Semaphore(max_concurrency)` locale a questa chiamata. Due `execute_batch` sovrapposti sullo stesso orchestratore non condividono il semaforo. Il rate limiter sì, perché sta sull'istanza;
- alloca `ordered_results = [None] * N`;
- per ogni indice lancia `asyncio.create_task` di un worker che prende il semaforo, chiama `execute_request` e scrive `ordered_results[index]`.

`asyncio.gather` aspetta tutti i task. L'ordine di completamento non conta: la posizione è l'indice di partenza. `LLMRequest` è frozen, e il risultato contiene il riferimento alla richiesta, quindi anche `document_id` e `request_id` restano quelli di ingresso. `BatchResult.results_by_id` è un dict costruito in `__post_init__` su `request_id`. Se due richieste arrivano con lo stesso id, vince l'ultima.

Tutti i task vengono creati prima che il semaforo ne faccia entrare uno. Per il lotto dell'esempio (90 o 120 chiamate) è irrilevante. Per decine di migliaia di richieste il costo è la lista di `Task` già schedulati, non le socket. Il punto in cui intervenire, e nel lotto ci sono molte richieste, questa riga crea subito una coroutine pianificata per ciascuna: 

tasks = [asyncio.create_task(process(item)) for item in batch]

Un pool fisso di worker che pesca da una coda terrebbe il semaforo bloccato e abbasserebbe il numero di task running.

asyncio.gather(...) raccoglie i risultati di tutte le operazioni e li restituisce in una lista. Se alcune operazioni possono restituire None, il codice può filtrare quei risultati. `execute_request` cattura `Exception`, quindi un errore normale diventa una riga `failed` e la cella viene riempita. `CancelledError` no: in Python 3.8 asyncio.CancelledError deriva da BaseException, non da Exception, il `try` non lo prende, il worker cessa e `gather` propaga la cancellazione.

`run_batch` non va chiamato da una coroutine già in esecuzione: `asyncio.run` vuole un loop nuovo.

## Errori

La base è `OrchestratorError`. Da lì si eredita.

Permanenti, niente retry: `InvalidRequestError`, `AuthenticationError`, `AuthorizationError`, `ModelNotFoundError`, `ContextWindowExceededError`, `ProviderNotFoundError`. Il commento nel codice li associa ai 400/422, 401, 403, 404 e allo sforamento della finestra di contesto. La classe non contiene lo status HTTP: è il provider che deve gestire il tipo giusto. Se un client HTTP fa raise di un `httpx.HTTPStatusError` , `is_retryable` è false il retry si ferma e rilancia subito l’errore. Non viene fatto un altro tentativo.

Transitori: `RateLimitError` (ha `retry_after` opzionale, in secondi), `ServerTemporarilyUnavailableError`, `NetworkTimeoutError`, `ConnectionDroppedError`.

`RetryPolicy.is_retryable` rifiuta prima qualunque `PermanentError`, anche se fosse  nella tupla `retryable_exceptions`. Poi controlla `isinstance` sulla tupla, default `(TransientError,)`.

## Retry

Default: 3 retry, delay iniziale 1 s, tetto 30 s, fattore 2, jitter on.

Il loop in `execute_with_retry` usa un contatore `attempt` che parte da 0. Al successo ritorna `(risultato, attempt + 1)`, quindi 1 se è andata al primo colpo. Se l'eccezione non è ritentabile, oppure `attempt >= max_retries`, rilancia l'eccezione originale. Con `max_retries=3` le chiamate a `operation` sono quattro: la prima più tre ripetizioni.

Il delay, quando non c'è un `retry_after`:

```text
ceiling = min(max_delay, initial_delay * backoff_factor ** attempt)
sleep   = random.uniform(0, ceiling)     se jitter
sleep   = ceiling                        altrimenti
```

`attempt` qui è ancora il valore prima dell'incremento, quindi il primo retry usa l'esponente 0 e il max  vale `initial_delay`. Il secondo usa l'esponente 1, e così via. Con jitter il sleep può essere 0. Va bene per i test; contro un 429 vero, uno zero di fatto raddoppia subito il carico. Sui 429 il ramo che conta è l'altro: se l'eccezione è `RateLimitError` e `retry_after` non è `None`, il delay è `max(0.1, retry_after + 0.1)` se il provider comunica quanto aspettare prima di riprovare (retry_after), il programma usa quel tempo e aggiunge 0,1 secondi di margine.

`on_retry(attempt + 1, exc, delay)` scatta solo quando un retry è stato deciso. L'ultimo fallimento, quello che esaurisce i tentativi, non passa dal callback: si rilancia prima.

Se tutti i tentativi falliscono, il conteggio li riporta correttamente. Per esempio, dopo 4 tentativi falliti, il risultato e' 4, non viene perso il primo.
È importante se quel conteggio viene usato per calcolare costi o attivare allarmi.

La CLI `simulate` restringe la policy: 4 retry, delay iniziale 0,15 s, tetto 3 s. L'esempio notturno usa 4 retry, 0,2 s, tetto 3 s. Il default della classe, 1 s fino a 30 s, è quello di `run` se si costruisce l'orchestratore senza passare una policy.

## Rate limit

Ci sono due controlli, uno dopo l’altro, se il rate limiter è configurato:
1. Semaforo: riserva uno slot per quella richiesta. Limita quante richieste possono essere gestite contemporaneamente.
2. Rate limiter: aspetta il permesso prima di chiamare il provider. Limita quante richieste partono nel tempo.
Poi parte complete, cioè la chiamata al provider.
Se la chiamata fallisce e viene ripetuta, il retry resta nello stesso slot del semaforo anche mentre aspetta il tempo di backoff. Al tentativo successivo chiede un nuovo permesso al rate limiter.
In breve: il semaforo limita le richieste contemporanee; il rate limiter regola quando possono partire le chiamate

### Token bucket

`rate` è il numero di permessi nell'intervallo `time_unit_seconds` (default 60, quindi si ragiona in RPM). Internamente diventa `rate_per_sec = rate / time_unit_seconds`.

Il bucket è come un salvadanaio di permessi di effettuare una chiamata al provider. Ogni chiamata consuma un permesso; col tempo il bucket ne recupera altri.
- rate è la velocità con cui si recuperano i permessi.
- capacity è quanti permessi può contenere il salvadanaio, cioè quante chiamate possono partire subito come burst.
Se non si specifica capacity, il codice la imposta uguale a rate, non a rate_per_sec. Quindi, con 300 RPM, parte con 300 permessi già disponibili e le prime 300 chiamate possono passare subito. In quel momento è il semaforo a limitare quante possono procedere contemporaneamente. Se il provider ha una quota più stretta, conviene impostare una capacity più bassa, come 25 nell’esempio.
Il lock protegge il conteggio dei permessi, ma non viene tenuto durante l’attesa. Se a una chiamata mancano permessi, il codice calcola quanto aspettare, libera il lock e dova in sleep. Nel frattempo le altre chiamate possono controllare e consumare i permessi disponibili.
Quando la chiamata si risveglia, controlla di nuovo quanti permessi ci sono. Due chiamate potrebbero svegliarsi insieme, ma solo una potrebbe trovare i permessi necessari: l’altra ricalcola l’attesa e riprova. Per questo il controllo è dentro un ciclo.

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
SimulatedLLMProvider finge di chiamare un modello: aspetta un tempo casuale tra i valori di latency_range, poi controlla se deve simulare un errore.
L’ordine dei controlli è questo:
1. Se è configurato per fallire i primi N tentativi di quell’ID richiesta, solleva RateLimitError. Il contatore resta nella stessa istanza del provider, quindi i retry successivi possono poi riuscire.
2. Se il prompt è vuoto o contiene solo spazi, solleva InvalidRequestError.
3. Se il prompt contiene FAIL_PERMANENT o FAIL_AUTH, simula un errore permanente o di autenticazione. Sono parole speciali per i test: se compaiono per caso nel prompt, fanno scattare l’errore.
4. Poi estrae a sorte gli errori configurati: prima 429, poi 503, poi 400. Se scatta il 429, per quella chiamata il simulatore non controlla più gli altri due.
Se non viene simulato nessun errore, genera una risposta finta. Classificazione ed estrazione producono dati strutturati; il riassunto produce testo e un piccolo dizionario. Anche raw_response contiene dati finti.
Token e metriche
La stima del simulatore è grossolana: circa un token ogni quattro caratteri, con un minimo di uno per il testo non vuoto. Le metriche del batch sommano i token delle richieste riuscite. Sono numeri di prova, non il consumo effettivo di un modello.
check_health del simulatore restituisce sempre True; al momento l’engine non lo chiama.
Quando si collega un provider reale:
Il suo metodo complete deve convertire le risposte del servizio nelle eccezioni del progetto, copiare l’eventuale Retry-After in RateLimitError e usare i dati reali per TokenUsage, se disponibili. Il nome del provider deve essere stabile, perché serve a identificarlo nel registro e nelle richieste.

## Checkpoint

Un checkpoint qui è un salvataggio dei risultati man mano che il lotto viene elaborato, così si puo' consultarli anche prima che tutte le richieste siano finite.
Il motore non gestisce direttamente i file: chiama una funzione (callback) quando una richiesta termina. Nell’esempio, CheckpointManager usa quella funzione per aggiungere il risultato a checkpoints.jsonl, una riga JSON per richiesta completata. Un lock impedisce a due coroutine dello stesso processo di scrivere insieme. Il file viene chiuso dopo ogni riga; se il programma si interrompe proprio mentre scrive, l’ultima riga potrebbe essere incompleta.
Il file registra i risultati del run corrente, ma non permette da solo di riprendere il lavoro. All’avvio del batch, l’esempio lo cancella; inoltre il motore non legge gli ID già salvati per saltare le richieste completate. Per riprendere davvero servirebbe aggiungere questa logica. Il lock protegge le scritture tra coroutine di un processo, non tra processi diversi.
Quando il lotto è terminato, consolidate_documents raccoglie i risultati per documento e compila i campi di classificazione, riassunto ed estrazione. Se uno di questi passaggi è fallito, registra l’errore e il relativo tipo di attività.

## Metriche

`_calculate_metrics` distingue due orologi.

`wall_clock_seconds` è il tempo di `execute_batch`, dal prima dei task al dopo il gather. Include attese di semaforo, bucket e backoff. `requests_per_second` è `total / wall_clock` su tutte le righe, fallite comprese. Sotto un decimillesimo di secondo il divisore viene alzato a `0.0001` per non dividere per zero.

La latenza min/media/max usa `response.latency_seconds` delle sole risposte riuscite. Nel simulatore quel numero è il tempo interno di `complete` (lo sleep più il resto della funzione), non `total_duration_seconds` del task. Quest'ultimo comprende i retry e le attese di rate limit, e finisce nel JSON di riga, ma non nella media. Un batch che ha passato metà del tempo in backoff può quindi mostrare una latenza media bassa e un wall clock alto. 

I token si sommano solo sui successi. I fallimenti incrementano `error_breakdown` con il nome della classe (`error_type`), oppure `UnknownError` se manca.

`total_retries` è la somma di `max(0, attempts - 1)`. Sul caso in cui i retry si esauriscono vale lo scarto descritto sopra, quindi il totale retry può essere corto di uno per ogni richiesta morta dopo l'ultimo tentativo.

## Log

configure_logging prepara il sistema di log del package:
- Imposta il logger principale orchestratore_llm, rimuovendo gli handler già collegati e impedendo ai messaggi di propagarsi ai logger superiori.
- I moduli, come orchestratore_llm.engine, usano logger con nomi più specifici. I loro messaggi risalgono al logger principale, che li gestisce.
- Se richiami configure_logging, la configurazione precedente viene sostituita. È utile nei test per ripartire da una configurazione pulita, ma può rimuovere un handler che un altro pezzo di codice aveva collegato allo stesso logger.
Con json_mode=True, la console mostra log JSON strutturati. Se specifichi log_file, anche il file contiene sempre JSON, indipendentemente dal formato della console. I colori della console usano codici ANSI e, con l’implementazione attuale, possono comparire anche quando l’output viene reindirizzato a un file o a un altro programma.

## Test

Ci sono due modi per lanciare i test:
- Con pytest: il progetto gli dice di cercare i test in src/orchestratore_llm/tests. Puoi avviarli dalla cartella principale con poetry run pytest.
- Con lo script run_tests.py: è un programma alternativo che usa gli strumenti di test inclusi in Python, senza pytest. Per lanciarlo dalla cartella principale: poetry run python src/orchestratore_llm/tests/run_tests.py.
Nei test del simulatore, gli errori vengono impostati in modo prevedibile: si può chiedere al simulatore di fallire per i primi tentativi di una richiesta, usare parole speciali nel prompt oppure mettere a zero le probabilità di errore casuale. Così il test verifica un comportamento preciso e non dipende dal caso.

Questi sono i 13 test eseguiti dal runner unittest:
Engine (4)
1. test_checkpoint_callback_execution
2. test_concurrency_limit_enforced
3. test_fault_tolerance_no_work_lost_on_partial_failures
4. test_order_preservation_with_heterogeneous_latencies
Provider (3)
5. test_provider_registry
6. test_simulated_provider_permanent_error_injection
7. test_simulated_provider_tasks
Rate limiter (2)
8. test_sliding_window_limiting
9. test_token_bucket_burst_and_refill
Retry (4)
10. test_execute_with_retry_eventual_success
11. test_execute_with_retry_permanent_failure_aborts_immediately
12. test_retry_after_precedence
13. test_retry_policy_classification