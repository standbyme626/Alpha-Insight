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
        self.keyboards: list[tuple[str, dict[str, object]]] = []

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        self.messages.append((chat_id, text))
        if reply_markup is not None:
            self.keyboards.append((chat_id, reply_markup))
        return {"ok": True}


class FlakySender(FakeSender):
    def __init__(self, *, fail_times: int = 1) -> None:
        super().__init__()
        self._fail_times = fail_times

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("simulated_send_failure")
        return await super().send_text(chat_id, text, reply_markup)


@pytest.mark.parametrize(
    ("raw", "expected_name"),
    [
        ("/monitor TSLA 1h", "monitor"),
        ("/monitor TSLA 1h rsi email_only", "monitor"),
        ("/monitor TSLA 1h rsi email_only alert-only", "monitor"),
        ("/monitor TSLA 1h rsi research-only", "monitor"),
        ("/route list", "route"),
        ("/list", "list"),
        ("/stop TSLA", "stop"),
    ],
)
def test_parse_phase_b_commands(raw: str, expected_name: str) -> None:
    parsed = parse_telegram_command(raw)
    assert not isinstance(parsed, CommandError)
    assert parsed.name == expected_name


@pytest.mark.parametrize("strategy", ["email_only", "wecom_only", "multi_channel"])
def test_parse_monitor_extended_route_strategies(strategy: str) -> None:
    parsed = parse_telegram_command(f"/monitor TSLA 1h rsi {strategy}")
    assert not isinstance(parsed, CommandError)
    assert parsed.args["route_strategy"] == strategy


@pytest.mark.parametrize("tier", ["research-only", "alert-only", "execution-ready"])
def test_parse_monitor_strategy_tier(tier: str) -> None:
    parsed = parse_telegram_command(f"/monitor TSLA 1h rsi email_only {tier}")
    assert not isinstance(parsed, CommandError)
    assert parsed.args["route_strategy"] == "email_only"
    assert parsed.args["strategy_tier"] == tier


def test_parse_monitor_strategy_tier_without_explicit_route_strategy() -> None:
    parsed = parse_telegram_command("/monitor TSLA 1h rsi research-only")
    assert not isinstance(parsed, CommandError)
    assert parsed.args["route_strategy"] == "dual_channel"
    assert parsed.args["strategy_tier"] == "research-only"


def test_parse_monitor_invalid_strategy_tier() -> None:
    parsed = parse_telegram_command("/monitor TSLA 1h rsi email_only sandbox-ready")
    assert isinstance(parsed, CommandError)
    assert "strategy tier" in parsed.message.lower()


def test_parse_route_invalid_channel() -> None:
    parsed = parse_telegram_command("/route set sms 123")
    assert isinstance(parsed, CommandError)
    assert "channel" in parsed.message.lower()


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


@pytest.mark.asyncio
async def test_long_polling_backlog_replay_consumes_without_duplicate_execution(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {"run_id": f"run-{calls['n']}", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    def _u(update_id: int, symbol: str) -> dict[str, object]:
        return {
            "update_id": update_id,
            "message": {
                "chat": {"id": "chat-b4"},
                "from": {"id": 10, "username": "delta"},
                "text": f"/analyze {symbol}",
            },
        }

    first_batch = [_u(4101, "AAPL"), _u(4102, "TSLA")]
    backlog_batch = [_u(4101, "AAPL"), _u(4102, "TSLA"), _u(4103, "MSFT")]

    handled_first = await gateway.process_updates(first_batch)
    handled_backlog = await gateway.process_updates(backlog_batch)

    assert handled_first == 2
    assert handled_backlog == 1
    assert calls["n"] == 3
    stats = store.verification_counts()
    assert stats["processed_updates"] == 3
    assert stats["distinct_updates"] == 3


@pytest.mark.asyncio
async def test_process_updates_isolates_single_update_failure(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FlakySender(fail_times=1)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-failure-isolation", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    updates = [
        {"update_id": 4201, "message": {"chat": {"id": "chat-b6"}, "from": {"id": 12}, "text": "/help"}},
        {"update_id": 4202, "message": {"chat": {"id": "chat-b6"}, "from": {"id": 12}, "text": "/help"}},
    ]
    handled = await gateway.process_updates(updates)

    assert handled == 1
    with store._connect() as conn:  # noqa: SLF001
        first = conn.execute("SELECT status FROM bot_updates WHERE update_id = 4201").fetchone()
        second = conn.execute("SELECT status FROM bot_updates WHERE update_id = 4202").fetchone()
    assert first is not None and first["status"] == "failed"
    assert second is not None and second["status"] == "processed"
    assert gateway._offset == 4203  # noqa: SLF001


@pytest.mark.asyncio
async def test_process_pending_updates_isolates_single_failure(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FlakySender(fail_times=1)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-pending-failure-isolation", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    await gateway.enqueue_update({"update_id": 4301, "message": {"chat": {"id": "chat-b7"}, "from": {"id": 13}, "text": "/help"}})
    await gateway.enqueue_update({"update_id": 4302, "message": {"chat": {"id": "chat-b7"}, "from": {"id": 13}, "text": "/help"}})

    handled = await gateway.process_pending_updates(limit=10)
    assert handled == 1

    with store._connect() as conn:  # noqa: SLF001
        first = conn.execute("SELECT status FROM bot_updates WHERE update_id = 4301").fetchone()
        second = conn.execute("SELECT status FROM bot_updates WHERE update_id = 4302").fetchone()
    assert first is not None and first["status"] == "failed"
    assert second is not None and second["status"] == "processed"


@pytest.mark.asyncio
async def test_webhook_durable_insert_supports_restart_recovery_without_duplicate(tmp_path) -> None:  # noqa: ANN001
    db_path = tmp_path / "telegram.db"
    store_1 = TelegramTaskStore(db_path)
    sender_1 = FakeSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {"run_id": f"run-webhook-{calls['n']}", **kwargs}

    actions_1 = TelegramActions(store=store_1, notifier=sender_1, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway_1 = TelegramGateway(store=store_1, actions=actions_1)

    update = {
        "update_id": 5101,
        "message": {
            "chat": {"id": "chat-b5"},
            "from": {"id": 11, "username": "echo"},
            "text": "/analyze NVDA",
        },
    }

    inserted_id = await gateway_1.enqueue_update(update)
    duplicated = await gateway_1.enqueue_update(update)
    assert inserted_id == 5101
    assert duplicated is None

    # Simulate crash/restart: new gateway instance restores from durable pending rows.
    store_2 = TelegramTaskStore(db_path)
    sender_2 = FakeSender()
    actions_2 = TelegramActions(store=store_2, notifier=sender_2, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway_2 = TelegramGateway(store=store_2, actions=actions_2)

    handled = await gateway_2.process_pending_updates(limit=10)
    handled_again = await gateway_2.process_pending_updates(limit=10)

    assert handled == 1
    assert handled_again == 0
    assert calls["n"] == 1
    stats = store_2.verification_counts()
    assert stats["processed_updates"] == 1
    assert stats["distinct_updates"] == 1
