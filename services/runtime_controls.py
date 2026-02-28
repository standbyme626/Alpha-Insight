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
    analysis_command_timeout_seconds: float = 90.0
    analysis_snapshot_timeout_seconds: float = 90.0
    analysis_recovery_timeout_seconds: float = 180.0
    photo_send_timeout_seconds: float = 20.0
    typing_heartbeat_seconds: float = 4.0
    session_singleflight_ttl_seconds: int = 120
    send_progress_updates: bool = False
    conversation_archive_keep_recent: int = 8
    conversation_archive_min_batch: int = 8
    max_watch_jobs_per_chat: int = 10
    global_concurrency: int = 8
    notification_max_retry: int = 3
    critical_fast_lane_immediate_retries: int = 1


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
