from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.scheduler import TelegramWatchScheduler
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutionResult


class _FakeExecutor:
    def __init__(self) -> None:
        self.executed_job_ids: list[str] = []
        self.retry_calls: int = 0

    async def execute_job(self, job) -> WatchExecutionResult:  # noqa: ANN001
        self.executed_job_ids.append(job.job_id)
        return WatchExecutionResult(
            job_id=job.job_id,
            symbol=job.symbol,
            triggered_count=1,
            pushed_count=2,
            dedupe_suppressed_count=1,
        )

    async def process_retry_queue(self, *, limit: int) -> int:
        self.retry_calls += 1
        return 3


class _FakeGovernor:
    def __init__(self) -> None:
        self.calls: list[datetime] = []

    def evaluate(self, *, now: datetime) -> None:
        self.calls.append(now)


class _FakePulsePublisher:
    async def publish_due(self, *, now: datetime) -> int:
        _ = now
        return 1


@pytest.mark.asyncio
async def test_scheduler_smoke_run_once_claims_and_executes_due_jobs(tmp_path) -> None:  # noqa: ANN001
    now = datetime(2026, 3, 8, 12, 0, 0, tzinfo=timezone.utc)
    tick_now = now + timedelta(minutes=2)
    store = TelegramTaskStore(tmp_path / "scheduler.db")
    store.upsert_telegram_chat(chat_id="chat-1", user_id="u-1", username="demo")
    store.create_watch_job(
        chat_id="chat-1",
        symbol="AAPL",
        interval_sec=60,
        now=now,
    )

    executor = _FakeExecutor()
    governor = _FakeGovernor()
    scheduler = TelegramWatchScheduler(
        store=store,
        executor=executor,
        governor=governor,
        pulse_publisher=_FakePulsePublisher(),
        now_provider=lambda: tick_now,
    )

    result = await scheduler.run_once()

    assert result.claimed_jobs == 1
    assert result.executed_jobs == 1
    assert result.pushed_notifications == 2
    assert result.dedupe_suppressed_count == 1
    assert result.retried_notifications == 3
    assert result.pulse_notifications == 1
    assert len(executor.executed_job_ids) == 1
    assert governor.calls == [tick_now]
