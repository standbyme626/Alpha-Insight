from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Protocol

from services.reliability_governor import ReliabilityGovernor
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutionResult, WatchExecutor


@dataclass
class SchedulerTickResult:
    claimed_jobs: int
    executed_jobs: int
    pushed_notifications: int
    dedupe_suppressed_count: int
    retried_notifications: int
    pulse_notifications: int = 0


class PulsePublisher(Protocol):
    async def publish_due(self, *, now: datetime) -> int:
        ...


class TelegramWatchScheduler:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        executor: WatchExecutor,
        governor: ReliabilityGovernor | None = None,
        pulse_publisher: PulsePublisher | None = None,
        poll_interval_seconds: float = 1.0,
        batch_size: int = 20,
        now_provider: Callable[[], datetime] | None = None,
    ):
        self._store = store
        self._executor = executor
        self._governor = governor or ReliabilityGovernor(store=store)
        self._pulse_publisher = pulse_publisher
        self._poll_interval_seconds = poll_interval_seconds
        self._batch_size = batch_size
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    async def run_once(self) -> SchedulerTickResult:
        jobs = self._store.claim_due_watch_jobs(now=self._now_provider(), limit=self._batch_size)
        pushed_notifications = 0
        dedupe_suppressed_count = 0
        executed_jobs = 0

        for job in jobs:
            try:
                outcome: WatchExecutionResult = await self._executor.execute_job(job)
                executed_jobs += 1
                pushed_notifications += outcome.pushed_count
                dedupe_suppressed_count += outcome.dedupe_suppressed_count
                self._store.mark_watch_job_error(job_id=job.job_id, error=None)
            except Exception as exc:  # pragma: no cover
                self._store.mark_watch_job_error(job_id=job.job_id, error=str(exc))

        retried_notifications = 0
        if hasattr(self._executor, "process_retry_queue"):
            retried_notifications = await self._executor.process_retry_queue(limit=self._batch_size)
        self._governor.evaluate(now=self._now_provider())
        pulse_notifications = 0
        if self._pulse_publisher is not None:
            pulse_notifications = max(0, int(await self._pulse_publisher.publish_due(now=self._now_provider())))

        return SchedulerTickResult(
            claimed_jobs=len(jobs),
            executed_jobs=executed_jobs,
            pushed_notifications=pushed_notifications,
            dedupe_suppressed_count=dedupe_suppressed_count,
            retried_notifications=retried_notifications,
            pulse_notifications=pulse_notifications,
        )

    async def run_forever(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(self._poll_interval_seconds)
