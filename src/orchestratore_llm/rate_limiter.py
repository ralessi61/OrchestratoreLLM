"""Algoritmi di Rate Limiting asincroni basati su Token Bucket e Sliding Window."""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Optional


class BaseRateLimiter(ABC):
    """Contratto astratto per componenti di controllo della frequenza di chiamata."""

    @abstractmethod
    async def acquire(self, tokens: int = 1) -> float:
        """Acquisisce il diritto di eseguire una richiesta, attendendo se necessario.
        
        Restituisce il tempo effettivo in secondi trascorso in attesa.
        """
        pass


class TokenBucketRateLimiter(BaseRateLimiter):
    """Implementazione dell'algoritmo Token Bucket per rate limiting asincrono.
    
    Permette raffiche controllate fino alla capacità massima (burst),
    ricaricando costantemente i token a una velocità prefissata (rate per secondo).
    """

    def __init__(
        self,
        rate: float,
        capacity: Optional[float] = None,
        time_unit_seconds: float = 60.0,
    ) -> None:
        """Inizializza il rate limiter.
        
        Args:
            rate: Numero di permessi ammessi nell'intervallo `time_unit_seconds` (es. 120 RPM).
            capacity: Capacità massima di accumulo per gestire picchi/burst. Se None, coincide con `rate`.
            time_unit_seconds: Intervallo di riferimento (default: 60.0 secondi).
        """
        if rate <= 0:
            raise ValueError("Il rate deve essere strettamente positivo")
        
        self.rate_per_sec = float(rate) / float(time_unit_seconds)
        self.capacity = float(capacity) if capacity is not None else float(rate)
        self.tokens = self.capacity
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        """Attende fino a quando non sono disponibili i token richiesti.
        
        Returns:
            Tempo totale speso in sleep asincrono.
        """
        if tokens > self.capacity:
            raise ValueError(f"Richiesti {tokens} token su una capacità massima di {self.capacity}")

        total_waited = 0.0

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.last_update = now

                # Ricarica progressiva dei token
                self.tokens = min(self.capacity, self.tokens + (elapsed * self.rate_per_sec))

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return total_waited

                # Calcolo del tempo necessario per rigenerare i token mancanti
                deficit = tokens - self.tokens
                wait_time = deficit / self.rate_per_sec

            # L'attesa avviene fuori dal blocco lock per consentire ad altre coroutine
            # di consultare lo stato senza serializzare l'event loop
            await asyncio.sleep(wait_time)
            total_waited += wait_time


class SlidingWindowRateLimiter(BaseRateLimiter):
    """Rate limiter a finestra mobile per garantire una distribuzione uniforme dei task."""

    def __init__(self, max_requests: int, window_seconds: float = 60.0) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests deve essere positivo")
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.timestamps: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> float:
        total_waited = 0.0

        while True:
            async with self._lock:
                now = time.monotonic()
                boundary = now - self.window_seconds

                # Rimozione dei timestamp scaduti fuori dalla finestra mobile
                self.timestamps = [t for t in self.timestamps if t > boundary]

                if len(self.timestamps) + tokens <= self.max_requests:
                    for _ in range(tokens):
                        self.timestamps.append(now)
                    return total_waited

                # Attesa del momento in cui il task più vecchio scadrà liberando slot
                oldest = self.timestamps[0]
                wait_time = (oldest + self.window_seconds) - now + 0.005

            if wait_time > 0:
                await asyncio.sleep(wait_time)
                total_waited += wait_time
