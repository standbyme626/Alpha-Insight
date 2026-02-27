from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass
class RuntimeLimits:
    per_chat_per_minute: int = 20
    max_watch_jobs_per_chat: int = 10
    global_concurrency: int = 8
    notification_max_retry: int = 3


class GlobalConcurrencyGate:
    def __init__(self, limit: int):
        self._sem = asyncio.Semaphore(max(1, int(limit)))

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        await self._sem.acquire()
        try:
            yield
        finally:
            self._sem.release()
