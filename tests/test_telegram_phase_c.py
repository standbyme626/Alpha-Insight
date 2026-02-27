from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agents.scanner_engine import WatchSignal, build_scan_trigger
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.reliability_governor import GovernorConfig, ReliabilityGovernor
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


class FakeSender:
    def __init__(self, *, fail_times: int = 0) -> None:
        self.messages: list[tuple[str, str]] = []
        self._fail_times = fail_times

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("telegram down")
        return {"ok": True}


@pytest.mark.asyncio
async def test_per_chat_rate_limit_hits(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-1", **kwargs}

    limits = RuntimeLimits(per_chat_per_minute=1)
    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, limits=limits, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    update1 = {"update_id": 7001, "message": {"chat": {"id": "chat-c1"}, "text": "/help"}}
    update2 = {"update_id": 7002, "message": {"chat": {"id": "chat-c1"}, "text": "/help"}}

    assert await gateway.process_update(update1)
    assert await gateway.process_update(update2)

    stats = store.build_phase_c_run_report()
    assert stats["command_success_rate"] < 1.0
    assert store.count_audit_events(event_type="rate_limited") == 1


@pytest.mark.asyncio
async def test_retry_then_success(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-c2", user_id="u1", username="c2")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    job = store.create_watch_job(chat_id="chat-c2", symbol="AAPL", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    sender = FakeSender(fail_times=1)

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.05,
            rsi=70.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Apple",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-c2",
            trigger_type="scheduled",
            trigger_id="t-c2",
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

    executor = WatchExecutor(
        store=store,
        notifier=sender,
        scan_runner=fake_scan_runner,
        limits=RuntimeLimits(notification_max_retry=3),
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 0
    assert store.count_retry_queue_depth() == 1

    with store._connect() as conn:  # noqa: SLF001
        conn.execute("UPDATE notifications SET next_retry_at = ?", ((base_time - timedelta(minutes=1)).isoformat(),))

    recovered = await executor.process_retry_queue(limit=5)
    assert recovered == 1
    assert store.count_delivered_notifications() == 1


@pytest.mark.asyncio
async def test_retry_to_dlq(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-c3", user_id="u1", username="c3")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-c3", symbol="TSLA", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    sender = FakeSender(fail_times=10)

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=200.0,
            pct_change=0.05,
            rsi=70.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Tesla",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-c3",
            trigger_type="scheduled",
            trigger_id="t-c3",
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

    executor = WatchExecutor(
        store=store,
        notifier=sender,
        scan_runner=fake_scan_runner,
        limits=RuntimeLimits(notification_max_retry=1),
    )

    await executor.execute_job(due_job)
    assert store.count_dlq() == 1


def test_auto_degrade_trigger_and_recover(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    governor = ReliabilityGovernor(
        store=store,
        config=GovernorConfig(
            push_success_threshold=0.99,
            analysis_p95_threshold_ms=1000,
            trigger_window_minutes=10,
            recovery_window_minutes=30,
        ),
    )

    now = datetime(2026, 2, 27, 12, 0, tzinfo=timezone.utc)
    for _ in range(10):
        store.record_metric(metric_name="push_attempt", metric_value=1, created_at=now - timedelta(minutes=5))
    for _ in range(8):
        store.record_metric(metric_name="push_success", metric_value=1, created_at=now - timedelta(minutes=5))
    for _ in range(5):
        store.record_metric(metric_name="analysis_latency_ms", metric_value=2500, created_at=now - timedelta(minutes=5))

    governor.evaluate(now=now)
    assert store.is_degradation_active(state_key="no_monitor_push")
    assert store.is_degradation_active(state_key="disable_critical_research")

    recover_time = now + timedelta(minutes=31)
    for _ in range(20):
        store.record_metric(metric_name="push_attempt", metric_value=1, created_at=recover_time - timedelta(minutes=1))
        store.record_metric(metric_name="push_success", metric_value=1, created_at=recover_time - timedelta(minutes=1))
    for _ in range(10):
        store.record_metric(metric_name="analysis_latency_ms", metric_value=200, created_at=recover_time - timedelta(minutes=1))

    governor.evaluate(now=recover_time)
    assert not store.is_degradation_active(state_key="no_monitor_push")
    assert not store.is_degradation_active(state_key="disable_critical_research")


@pytest.mark.asyncio
async def test_degraded_state_skips_and_audits(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.set_degradation_state(state_key="no_monitor_push", status="active", reason="test")

    sender = FakeSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-c5", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    monitor_update = {
        "update_id": 9001,
        "message": {
            "chat": {"id": "chat-c5"},
            "from": {"id": 1},
            "text": "/monitor TSLA 1h",
        },
    }
    assert await gateway.process_update(monitor_update)
    assert store.count_audit_events(event_type="degrade_skip") >= 1

    store.upsert_telegram_chat(chat_id="chat-c5", user_id="1", username="u")
    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-c5", symbol="TSLA", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=200.0,
            pct_change=0.05,
            rsi=70.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Tesla",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-c5",
            trigger_type="scheduled",
            trigger_id="t-c5",
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
    out = await executor.execute_job(due_job)
    assert out.pushed_count == 0
    assert store.count_audit_events(event_type="degrade_skip") >= 2
