"""Interfaccia a riga di comando (CLI) per l'orchestratore di chiamate LLM."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import List

# Bootstrap per esecuzione diretta del file cli.py senza -m
if __package__ is None or __package__ == "":
    src_dir = Path(__file__).resolve().parent.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    import orchestratore_llm
    __package__ = "orchestratore_llm"

from .engine import LLMOrchestrator
from .logging_config import configure_logging
from .models import LLMRequest, LLMTaskType
from .providers.registry import create_default_registry
from .rate_limiter import TokenBucketRateLimiter
from .retry import RetryPolicy


def build_parser() -> argparse.ArgumentParser:
    """Configura gli argomenti della riga di comando."""
    parser = argparse.ArgumentParser(
        prog="orchestratore-llm",
        description="Esecuzione concorrente e resiliente di lotti di chiamate LLM",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Sottocomando 'simulate'
    sim_parser = subparsers.add_parser(
        "simulate",
        help="Esegue una simulazione del carico notturno tipico (3 task per documento)",
    )
    sim_parser.add_argument(
        "--docs",
        type=int,
        default=30,
        help="Numero di documenti simulati (default: 30 docs x 3 chiamate = 90 chiamate)",
    )
    sim_parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Limite di concorrenza massima (default: 10)",
    )
    sim_parser.add_argument(
        "--rpm",
        type=int,
        default=300,
        help="Limite massimo richieste al minuto (default: 300)",
    )
    sim_parser.add_argument(
        "--provider",
        "-p",
        type=str,
        default=None,
        help="Nome del provider registrato da utilizzare (default: 'ammollo', opzioni: 'ammollo', 'grocky', 'gwen')",
    )
    sim_parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="batch_report.json",
        help="File JSON di destinazione per i risultati e le metriche (default: batch_report.json)",
    )
    sim_parser.add_argument(
        "--json-log",
        action="store_true",
        help="Abilita output di log in formato JSON strutturato",
    )
    sim_parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Imposta livello di log su DEBUG",
    )

    # Sottocomando 'run'
    run_parser = subparsers.add_parser(
        "run",
        help="Esegue un file JSON di richieste predisposte",
    )
    run_parser.add_argument(
        "input_file",
        type=str,
        help="Percorso al file JSON contenente l'elenco delle richieste",
    )
    run_parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="run_report.json",
        help="Percorso al file JSON di output",
    )
    run_parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Concorrenza massima",
    )
    run_parser.add_argument(
        "--json-log",
        action="store_true",
        help="Abilita log JSON",
    )

    return parser


def cmd_simulate(args: argparse.Namespace) -> int:
    """Genera ed esegue il lotto notturno di test con 3 chiamate per documento."""
    level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    configure_logging(level=level, json_mode=args.json_log)

    registry = create_default_registry()

    # Risoluzione del provider: esplicito o predefinito dal registry
    if getattr(args, "provider", None):
        provider_name = args.provider
        # Verifica preventiva esistenza
        registry.get(provider_name)
    else:
        provider_name = registry.get().name

    print(f"\n{'='*70}")
    print(f" Avvio simulazione batch: {args.docs} documenti ({args.docs * 3} chiamate LLM)")
    print(f" Provider selezionato: '{provider_name}' | Concorrenza max: {args.concurrency} | Rate limit: {args.rpm} RPM")
    print(f"{'='*70}\n")

    requests: List[LLMRequest] = []
    for doc_idx in range(1, args.docs + 1):
        doc_id = f"DOC-{doc_idx:04d}"

        # 1. Chiamata classificazione
        requests.append(
            LLMRequest(
                prompt=f"Classifica la tipologia del documento contrattuale numero {doc_id}",
                task_type=LLMTaskType.CLASSIFICATION,
                document_id=doc_id,
                provider=provider_name,
            )
        )
        # 2. Chiamata riassunto
        requests.append(
            LLMRequest(
                prompt=f"Genera un abstract esecutivo di massimo tre righe per il documento {doc_id}",
                task_type=LLMTaskType.SUMMARY,
                document_id=doc_id,
                provider=provider_name,
            )
        )
        # 3. Chiamata estrazione campi
        requests.append(
            LLMRequest(
                prompt=f"Estrai entità chiave, date e importi monetari dal documento {doc_id}",
                task_type=LLMTaskType.FIELD_EXTRACTION,
                document_id=doc_id,
                provider=provider_name,
            )
        )

    rate_limiter = TokenBucketRateLimiter(rate=args.rpm, time_unit_seconds=60.0)
    retry_policy = RetryPolicy(max_retries=4, initial_delay=0.15, max_delay=3.0, jitter=True)

    orchestrator = LLMOrchestrator(
        registry=registry,
        max_concurrency=args.concurrency,
        rate_limiter=rate_limiter,
        retry_policy=retry_policy,
    )

    batch_result = orchestrator.run_batch(requests)
    batch_result.save_json(args.output)

    # Stampa riassuntiva
    m = batch_result.metrics
    print(f"\n{'-'*70}")
    print(f" RIEPILOGO ESECUZIONE")
    print(f"{'-'*70}")
    print(f" Totale richieste inviate : {m.total_requests}")
    print(f" Riuscite con successo    : {m.successful_requests} ({m.successful_requests/m.total_requests*100:.1f}%)")
    print(f" Fallite definitivamente  : {m.failed_requests}")
    print(f" Retry eseguiti (backoff) : {m.total_retries}")
    print(f" Token totali stimati     : {m.total_tokens:,} (prompt: {m.prompt_tokens:,}, comp: {m.completion_tokens:,})")
    print(f" Tempo totale batch       : {m.wall_clock_seconds:.2f} s")
    print(f" Latenza media richiesta  : {m.avg_latency_seconds*1000:.1f} ms (min: {m.min_latency_seconds*1000:.1f}ms, max: {m.max_latency_seconds*1000:.1f}ms)")
    print(f" Throughput richieste     : {m.requests_per_second:.1f} req/s")
    print(f" Throughput token         : {m.tokens_per_second:.1f} tok/s")
    if m.error_breakdown:
        print(f" Dettaglio errori         : {m.error_breakdown}")
    print(f" Report completo salvato  : {args.output}")
    print(f"{'='*70}\n")

    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Elabora un file JSON di richieste."""
    configure_logging(level=logging.INFO, json_mode=args.json_log)
    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Errore: il file di input '{input_path}' non esiste.", file=sys.stderr)
        return 1

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print("Errore: il file JSON deve contenere una lista di oggetti richiesta.", file=sys.stderr)
        return 1

    requests = [LLMRequest.from_dict(item) for item in data]
    orchestrator = LLMOrchestrator(max_concurrency=args.concurrency)
    result = orchestrator.run_batch(requests)
    result.save_json(args.output)
    print(f"Elaborazione completata. Risultati salvati in: {args.output}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Punto d'ingresso principale della CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "simulate":
        return cmd_simulate(args)
    elif args.command == "run":
        return cmd_run(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
