# Conclusioni per lo scenario del cliente

Il batch è notturno: per ogni documento tre chiamate (classificazione, riassunto, estrazione), `max_concurrency` sulle chiamate contemporanee, retry sui 429, checkpoint su JSONL appena una chiamata finisce, risultati raggruppati per `document_id`.

La strategia da usare è `AsyncioStrategy`. il tempo indicato per il batch misura quanto si aspetta che il provider elabori la richiesta. Un solo event loop sovrappone quelle attese con un semaforo. I thread, sullo stesso batch, scalano ma restano più lenti: ogni worker apre un event loop, esegue `complete` e lo chiude. Usare più processi non rende più rapido il tempo di risposta del provider: la parte remota resta uguale. In più, i processi devono scambiarsi dati e stato — richieste, risposte, limiti alle chiamate e checkpoint — aggiungendo complessità e lavoro di coordinamento.

Le misure sono in `docs/RELAZIONE_TECNICA.md` (24 richieste, attesa fissa 50 ms, senza quota, mediana di tre giri, 12 ottobre 2026). In sequenza circa 1,45 s. Ventiquattro attese da 50 ms farebbero 1,20 s: circa 10 ms a chiamata. 
- Con una sola richiesta alla volta, asyncio e l’altra modalità hanno prestazioni simili.
- Aumentando la concorrenza di asyncio a 4, 12 e 24, il tempo diminuisce. Con 24 richieste in parallelo si stabilizza intorno a 0,06 secondi.
- I thread si fermano invece intorno a 0,10 secondi.
- Passando da 12 a 24, i thread diventano circa 1,6 volte più veloci; asyncio, sullo stesso passaggio, resta vicino a un guadagno di 2 volte.
In sintesi, in questo benchmark asyncio sfrutta meglio l’aumento della concorrenza e raggiunge un tempo finale più basso.
Durante `asyncio.sleep` il GIL (Global Interpreter Lock, un solo thread alla volta esegue bytecode Python) è rilasciato in entrambi i casi: il divario è l'avvio del loop e del pool. `max_concurrency` aumenta finché è minore del numero di richieste. Oltre, non resta un'altra chiamata da far partire insieme. Su un altro PC i tempi assoluti cambiano di qualche millisecondo; l'andamento resta questo: il tempo scende con `max_concurrency` e si ferma quando il valore eguaglia il numero di richieste.

Con una quota sul provider il semaforo smette di essere il limite. `grocky` accetta 4 richieste al secondo; le chiamate in più prendono 429 e il tempo lo decide la finestra, non `max_concurrency`. Il token bucket va tarato su quella quota. Se non si imposta `capacity`, il codice la pone uguale a `rate`: con 300 RPM il bucket parte con 300 permessi. Nell'esempio notturno la capacità è 25.

I 429, i timeout e i 503 si ritentano, con `retry_after` quando il provider lo comunica. Un 400 o un 401 chiude quella richiesta. Il batch prosegue: un esito `failed` non cancella le altre righe né il checkpoint già scritto.

Il client HTTP del provider vero deve essere asincrono. Una chiamata sincrona tiene fermo l'event loop per tutta la durata della risposta, e il semaforo non sovrappone più nulla.

`run_batch` va chiamato da codice sincrono. Da una coroutine già in esecuzione si usa `await execute_batch`.

## Soluzione operativa consigliata

1. Una `LLMRequest` per chiamata, tre per documento (classificazione, riassunto, estrazione), con lo stesso `document_id` e `request_id` distinti.
2. `AsyncioStrategy`. `max_concurrency` non supera il numero di richieste, né le chiamate che il provider accetta insieme.
3. Token bucket con `rate` uguale agli RPM del contratto e `capacity` uguale al burst ammesso. Senza `capacity` il bucket parte pieno con un permesso per ogni RPM: a 300 RPM sono 300 chiamate subito. L'esempio notturno usa 25.
4. Se la quota è stretta, come 4 richieste al secondo, il tempo del batch lo decide il rate limiter. Alzare `max_concurrency` oltre quel ritmo produce 429.
5. Retry su 429, timeout e 503, usando `retry_after` quando c'è. Un 400 o un 401 chiude solo quella richiesta.
6. `complete` del provider vero è asincrono. Una HTTP sincrona blocca l'event loop.
7. `checkpoint_callback` scrive il JSONL a ogni esito. Un `failed` non cancella le righe già scritte né ferma il batch.
8. A fine batch, `get_by_document` raggruppa le tre chiamate. Da uno script si usa `run_batch`; da una coroutine, `await execute_batch`.
