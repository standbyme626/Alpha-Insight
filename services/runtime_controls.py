from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass
class RuntimeLimits:
    per_chat_per_minute: int = 20
    nl_per_chat_per_minute: int = 12
    nl_per_chat_per_day: int = 200
    nl_parse_timeout_seconds: float = 2.5
    nl_parse_max_retries: int = 2
    llm_degrade_window_minutes: int = 10
    llm_degrade_min_samples: int = 8
    llm_degrade_fail_rate_threshold: float = 0.5
    llm_recover_fail_rate_threshold: float = 0.25
    chart_degrade_window_minutes: int = 10
    chart_degrade_min_samples: int = 6
    chart_degrade_fail_rate_threshold: float = 0.5
    chart_recover_fail_rate_threshold: float = 0.2
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
