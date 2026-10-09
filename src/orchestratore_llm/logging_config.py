"""Configurazione del logging strutturato per l'orchestratore.

Supporta formattazione JSON per ingestion automatica in sistemi SIEM/APM
e output da terminale con evidenziazione sintattica delle operazioni critiche.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


class StructuredJsonFormatter(logging.Formatter):
    """Formatter che serializza ogni record di log in una stringa JSON compatta."""

    def format(self, record: logging.LogRecord) -> str:
        log_payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
        }

        # Estrazione attributi di contesto strutturati passati tramite extra
        standard_fields = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "message"
        }
        extra_data = {
            k: v for k, v in record.__dict__.items() if k not in standard_fields
        }
        if extra_data:
            log_payload["context"] = extra_data

        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload, default=str)


class ColoredConsoleFormatter(logging.Formatter):
    """Formatter leggibile con codici ANSI per sviluppo e monitoraggio a console."""

    COLORS = {
        "DEBUG": "\033[36m",     # Ciano
        "INFO": "\033[32m",      # Verde
        "WARNING": "\033[33m",   # Giallo
        "ERROR": "\033[31m",     # Rosso
        "CRITICAL": "\033[1;31m" # Rosso grassetto
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        time_str = datetime.fromtimestamp(record.created).strftime("%H:%M:%S.%f")[:-3]
        color = self.COLORS.get(record.levelname, self.RESET)

        # Formatta eventuali attributi di contesto chiave
        context_parts = []
        for key in ("request_id", "document_id", "provider", "attempt", "status"):
            val = getattr(record, key, None)
            if val is not None:
                context_parts.append(f"{key}={val}")
        ctx_str = f" [{', '.join(context_parts)}]" if context_parts else ""

        prefix = f"{time_str} {color}{record.levelname:<7}{self.RESET} [{record.name}]"
        msg = record.getMessage()
        formatted = f"{prefix}{ctx_str} {msg}"

        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        return formatted


def configure_logging(
    level: int = logging.INFO,
    json_mode: bool = False,
    log_file: Optional[str | Path] = None,
    logger_name: str = "orchestratore_llm",
) -> logging.Logger:
    """Inizializza e configura la gerarchia di logging per la libreria."""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    if json_mode:
        console_handler.setFormatter(StructuredJsonFormatter())
    else:
        console_handler.setFormatter(ColoredConsoleFormatter())
    logger.addHandler(console_handler)

    # Eventuale File Handler rotante o continuo
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(file_path), encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(StructuredJsonFormatter())
        logger.addHandler(file_handler)

    return logger
