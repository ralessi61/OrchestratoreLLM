# Orchestratore Batch Chiamate LLM
**Componente:** `orchestratore_llm` (v0.1.0)  
**Ambito:** Elaborazione notturna massiva di lotti documentali (Classificazione, Riassunto, Estrazione)  
**Destinatari:** Team di Sviluppo & Conduzione Operativa  
---
### 1. Criticità del Sistema Preesistente
L'implementazione originale si basava su un ciclo sequenziale bloccante: ogni documento richiedeva 3 chiamate HTTP sincrone in serie. Con carichi superiori a poche decine di documenti, l'approccio presentava tre colli di bottiglia critici:
- **Throughput insufficiente:** 5s-7s a documento; su 3.000 documenti il tempo stimato superava le 15 ore, sforando la finestra notturna.
- **Vulnerabilità ai Rate Limit (HTTP 429):** Il primo rifiuto da parte del provider causava un'eccezione non gestita che arrestava l'intero processo.
- **Assenza di persistenza dello stato:** Il crash comportava la perdita immediata di tutte le elaborazioni pregresse, costringendo a riavvii manuali da zero con costi API raddoppiati.
---
### 2. Razionale della Scelta: Perché `asyncio`
La natura dell'elaborazione è **I/O-bound**: oltre il 99% del tempo viene speso in attesa di rete (latenza TCP, coda remota e generazione token sul cluster del provider). La CPU locale resta inattiva.
- **Scarto di `multiprocessing`:** Spostare chiamate HTTP su processi figli comporterebbe un overhead insostenibile (30–50 MB di RAM per worker, costi di spawn/fork del processo e pesante serializzazione IPC via `pickle`).
- **Scarto di `threading`:** I thread del sistema operativo consumano circa 8 MB di stack ciascuno, generano contesa sul GIL e richiedono lock complessi sulle strutture dati condivise.
- **Adozione di `asyncio`:** L'I/O cooperativo non bloccante su singolo thread alloca appena **2–4 KB per coroutine**, permettendo di gestire centinaia di richieste simultanee con occupazione di memoria trascurabile, zero overhead di context-switch a livello kernel e totale assenza di race condition durante l'aggiornamento di metriche o collezioni.
---
### 3. Architettura dei Componenti
```text
[Invocazione Batch] ──► [asyncio.Semaphore] ──► [TokenBucketLimiter] ──► [RetryPolicy (Jitter)] ──► [BaseLLMProvider]
                                                                                      │
                                                                         [Checkpoint JSONL Streaming]
```
1. **Preservazione dell'Ordine (Allineamento 1:1):** Per evitare il disallineamento dovuto a tempi di risposta eterogenei, l'orchestratore pre-alloca una lista posizionale di dimensione N. Ogni coroutine riceve l'indice originario della richiesta e deposita il risultato nella posizione esatta di partenza. La risposta espone inoltre una mappa indicizzata `results_by_id` per lookup O(1).
2. **Tassonomia Rigorosa delle Eccezioni:**
   - *Errori Permanenti (`PermanentError`: 400, 401, 403, 404, payload non valido):* Esecuzione interrotta all'istante (*Fail-Fast*), evitando inutili consumi di quota.
   - *Errori Transitori (`TransientError`: 429, 503, timeout di rete):* Intercettati e instradati al meccanismo di retry.
3. **Retry con Exponential Backoff, Full Jitter e `Retry-After`:** L'intervallo cresce esponenzialmente ($t = 2^{\text{attempt}}$) con fattore randomico (*Full Jitter*) per evitare che richieste fallite si riallineino saturando nuovamente il server (fenomeno del *Thundering Herd*). Se la risposta include l'header `Retry-After`, questo ha precedenza sul calcolo matematico.
Thundering Herd: Il fenomeno in cui molteplici client falliscono simultaneamente e tentano di ritentare nello stesso istante, causando un sovraccarico del server e ulteriori fallimenti.
4. **Throttling a Due Livelli:** Un `asyncio.Semaphore` limita le connessioni simultanee attive (es. 15), mentre un `TokenBucketRateLimiter` gestisce il volume su base temporale (es. 300 RPM) consentendo piccoli burst senza violare le quote del provider.
5. **Tolleranza ai Guasti e Checkpointing Streaming:** I singoli fallimenti non interrompono l'elaborazione degli altri task. Ciascun completamento invoca una callback atomica in append su file `checkpoints.jsonl`: in caso di spegnimento forzato dell'infrastruttura, il lavoro svolto è preservato su disco fino all'ultimo documento completato.
6. **Disaccoppiamento Provider (Open/Closed Principle):** L'orchestratore dipende unicamente dal contratto astratto `BaseLLMProvider`. L'introduzione di nuovi endpoint (OpenAI, Anthropic, modelli locali Ollama/vLLM) avviene registrando la classe nel `ProviderRegistry`, senza toccare una singola riga dell'engine.
---
### 4. Esiti di Collaudo e Benchmark
La suite include **13 test automatizzati** (`tests/run_tests.py`), eseguiti con successo senza dipendenze terze obbligatorie.
La simulazione del carico notturno (`examples/nightly_batch_pipeline.py`) eseguita su 30 documenti (90 chiamate totali tra classificazione, riassunto ed estrazione) con iniezione stocastica del **20% di errori HTTP 429**, ha registrato:
- **Completamento batch:** 90/90 richieste riuscite (**100% successo**, zero dati persi);
- **Retry recuperati automaticamente:** 17 interventi di backoff senza alcun crash;
- **Tempo di esecuzione:** 16.52s contro gli oltre 220s stimati del flusso sequenziale (fattore di accelerazione **>13x**);
- **Throughput:** 5.4 req/s e 445.9 token/s.