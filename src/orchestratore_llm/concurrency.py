"""Esecuzione parallela del batch: asyncio oppure thread."""

from __future__ import annotations

import asyncio
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Awaitable, Callable, List, Sequence, Tuple, TypeVar, Union

T = TypeVar("T")
Job = Callable[[], Awaitable[T]]
OnResult = Callable[[int, T], Union[None, Awaitable[None]]]


async def _emit(on_result: OnResult, index: int, value: T) -> None:
    out = on_result(index, value)
    if asyncio.iscoroutine(out):
        await out


class ConcurrencyStrategy(ABC):
    """Come far girare le chiamate del lotto. Il risultato resta nell'ordine di partenza,
    ma chi riceve on_result lo vede quando quella chiamata è finita, non a lotto chiuso.
    """

    def __init__(self, max_concurrency: int) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency deve essere strettamente positivo")
        self.max_concurrency = max_concurrency

    @abstractmethod
    async def run(self, jobs: Sequence[Job], on_result: OnResult) -> None:
        pass


class AsyncioStrategy(ConcurrencyStrategy):
    """Coroutine sul loop corrente. Il semaforo è il tetto di chiamate in volo."""

    async def run(self, jobs: Sequence[Job], on_result: OnResult) -> None:
        if not jobs:
            return

        semaphore = asyncio.Semaphore(self.max_concurrency)

        async def _one(index: int, job: Job) -> Tuple[int, T]:
            async with semaphore:
                return index, await job()

        tasks = [asyncio.create_task(_one(i, job)) for i, job in enumerate(jobs)]
        try:
            for finished in asyncio.as_completed(tasks):
                index, value = await finished
                await _emit(on_result, index, value)
        except asyncio.CancelledError:
            await _drop_tasks(tasks)
            raise
        except Exception:
            await _drop_tasks(tasks)
            raise


class ThreadingStrategy(ConcurrencyStrategy):
    """Un thread per chiamata, fino a max_concurrency. Ogni thread ha un loop suo
    perché complete() è async; il limite è la dimensione del pool, non un semaforo.
    """

    async def run(self, jobs: Sequence[Job], on_result: OnResult) -> None:
        if not jobs:
            return

        executor = ThreadPoolExecutor(
            max_workers=self.max_concurrency,
            thread_name_prefix="orch",
        )
        stop = threading.Event()
        guard = threading.Lock()
        inflight: List[Tuple[asyncio.AbstractEventLoop, asyncio.Task]] = []

        def _run(index: int, job: Job):
            inner = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(inner)

                async def _body():
                    task = asyncio.current_task()
                    with guard:
                        if stop.is_set():
                            raise asyncio.CancelledError()
                        inflight.append((inner, task))
                    try:
                        return await job()
                    finally:
                        with guard:
                            try:
                                inflight.remove((inner, task))
                            except ValueError:
                                pass

                return index, inner.run_until_complete(_body())
            finally:
                try:
                    inner.close()
                finally:
                    asyncio.set_event_loop(None)

        futures = [executor.submit(_run, i, job) for i, job in enumerate(jobs)]
        wrapped = [asyncio.wrap_future(fut) for fut in futures]
        for item in wrapped:
            item.add_done_callback(_swallow)

        try:
            pending = set(wrapped)
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for item in done:
                    index, value = item.result()
                    await _emit(on_result, index, value)
        except asyncio.CancelledError:
            _stop_threads(stop, guard, inflight, futures)
            raise
        except Exception:
            _stop_threads(stop, guard, inflight, futures)
            raise
        finally:
            executor.shutdown(wait=True, cancel_futures=True)


def _swallow(fut: asyncio.Future) -> None:
    if fut.cancelled():
        return
    try:
        fut.exception()
    except asyncio.CancelledError:
        pass


async def _drop_tasks(tasks: Sequence[asyncio.Task]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _stop_threads(stop, guard, inflight, futures) -> None:
    with guard:
        stop.set()
        snapshot = list(inflight)
    for loop, task in snapshot:
        if loop.is_closed():
            continue
        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:
            pass
    for fut in futures:
        fut.cancel()
