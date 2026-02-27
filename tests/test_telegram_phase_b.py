from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agents.scanner_engine import WatchSignal, build_scan_trigger
from agents.telegram_command_router import CommandError, parse_telegram_command
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.scheduler import TelegramWatchScheduler
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import DueWatchJob, TelegramTaskStore
from services.watch_executor import WatchExecutor


class FakeSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


@pytest.mark.parametrize(
    ("raw", "expected_name"),
    [
        ("/monitor TSLA 1h", "monitor"),
        ("/list", "list"),
        ("/stop TSLA", "stop"),
    ],
)
def test_parse_phase_b_commands(raw: str, expected_name: str) -> None:
    parsed = parse_telegram_command(raw)
    assert not isinstance(parsed, CommandError)
    assert parsed.name == expected_name


def test_parse_monitor_invalid_interval() -> None:
    parsed = parse_telegram_command("/monitor TSLA 10x")
    assert isinstance(parsed, CommandError)
    assert "Invalid interval" in parsed.message


@pytest.mark.asyncio
async def test_monitor_list_stop_lifecycle_via_gateway(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "unused", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    monitor_update = {
        "update_id": 3001,
        "message": {
            "chat": {"id": "chat-b1"},
            "from": {"id": 9, "username": "alpha"},
            "text": "/monitor TSLA 1h",
        },
    }
    list_update = {
        "update_id": 3002,
        "message": {
            "chat": {"id": "chat-b1"},
            "from": {"id": 9, "username": "alpha"},
            "text": "/list",
        },
    }
    stop_update = {
        "update_id": 3003,
        "message": {
            "chat": {"id": "chat-b1"},
            "from": {"id": 9, "username": "alpha"},
            "text": "/stop TSLA",
        },
    }

    assert await gateway.process_update(monitor_update)
    active_jobs = store.list_watch_jobs(chat_id="chat-b1")
    assert len(active_jobs) == 1
    assert active_jobs[0].symbol == "TSLA"

    assert await gateway.process_update(list_update)
    assert any("Active monitor jobs" in item[1] for item in sender.messages)

    assert await gateway.process_update(stop_update)
    assert store.list_watch_jobs(chat_id="chat-b1") == []


@pytest.mark.asyncio
async def test_scheduler_restores_and_executes_due_jobs(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-b2", user_id="u-1", username="beta")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-b2", symbol="AAPL", interval_sec=3600, now=base_time)

    calls: list[str] = []

    class FakeExecutor:
        async def execute_job(self, job: DueWatchJob):  # noqa: ANN001
            calls.append(job.job_id)
            class _Result:
                pushed_count = 1
                dedupe_suppressed_count = 0
            return _Result()

    scheduler = TelegramWatchScheduler(
        store=store,
        executor=FakeExecutor(),
        now_provider=lambda: base_time + timedelta(hours=2),
        poll_interval_seconds=0.01,
    )

    out = await scheduler.run_once()
    assert out.claimed_jobs == 1
    assert out.executed_jobs == 1
    assert out.pushed_notifications == 1
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_watch_executor_dedupes_events_within_bucket(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-b3", user_id="u-2", username="gamma")

    now = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    job = store.create_watch_job(chat_id="chat-b3", symbol="TSLA", interval_sec=3600, now=now)
    due_jobs = store.claim_due_watch_jobs(now=now + timedelta(hours=2), limit=5)
    assert len(due_jobs) == 1
    due_job = due_jobs[0]

    sender = FakeSender()
    signal_ts = datetime(2026, 2, 27, 2, 0, tzinfo=timezone.utc)

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=200.0,
            pct_change=0.05,
            rsi=72.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Tesla",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-1",
            trigger_type="scheduled",
            trigger_id="t-1",
            trigger_time=signal_ts,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=signal.symbol,
                company_name=signal.company_name,
                timestamp=signal.timestamp,
                price=signal.price,
                pct_change=signal.pct_change,
                rsi=signal.rsi,
                priority=signal.priority,
                reason=signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        return type("RunOut", (), {
            "trigger": build_scan_trigger(trigger_time=signal_ts),
            "signals": [signal],
            "selected_alerts": [signal],
            "snapshots": [snapshot],
            "notifications": [],
            "runtime_metrics": {},
            "failure_events": [],
            "failure_clusters": {},
            "alarms": [],
        })()

    executor = WatchExecutor(store=store, notifier=sender, scan_runner=fake_scan_runner)

    out1 = await executor.execute_job(due_job)
    out2 = await executor.execute_job(due_job)

    assert out1.pushed_count == 1
    assert out2.dedupe_suppressed_count == 1
    assert len(sender.messages) == 1
    assert store.count_watch_events() == 1
    assert store.count_delivered_notifications() == 1
