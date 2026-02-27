from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from agents.scanner_engine import WatchSignal, build_scan_trigger
from agents.telegram_command_router import CommandError, parse_telegram_command
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.notification_channels import MultiChannelNotifier
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


class FakeChatSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


class FakeTargetSender:
    def __init__(self, *, fail: bool = False) -> None:
        self.messages: list[tuple[str, str]] = []
        self._fail = fail

    async def send_text(self, target: str, text: str) -> dict[str, object]:
        self.messages.append((target, text))
        if self._fail:
            raise RuntimeError("channel down")
        return {"ok": True}


@pytest.mark.parametrize(
    ("raw", "name"),
    [
        ("/report run-abc123", "report"),
        ("/digest daily", "digest"),
        ("/monitor TSLA 1h rsi", "monitor"),
    ],
)
def test_parse_phase_d_commands(raw: str, name: str) -> None:
    parsed = parse_telegram_command(raw)
    assert not isinstance(parsed, CommandError)
    assert parsed.name == name


@pytest.mark.asyncio
async def test_help_contains_compliance_and_monitor_template(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-help", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 12001, "message": {"chat": {"id": "chat-d1"}, "from": {"id": 1}, "text": "/help"}}
    assert await gateway.process_update(update)
    message = sender.messages[-1][1]
    assert "no auto-trading" in message
    assert "/monitor <symbol> <interval> [volatility|price|rsi]" in message


@pytest.mark.asyncio
async def test_monitor_template_report_and_digest(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-d1abc",
            "fused_insights": {"summary": "Trend up with moderate momentum"},
            "metrics": {"data_close": 188.12, "technical_rsi_14": 63.3},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    monitor_update = {"update_id": 12011, "message": {"chat": {"id": "chat-d2"}, "from": {"id": 2}, "text": "/monitor TSLA 1h rsi"}}
    analyze_update = {"update_id": 12012, "message": {"chat": {"id": "chat-d2"}, "from": {"id": 2}, "text": "/analyze TSLA"}}
    report_update = {"update_id": 12013, "message": {"chat": {"id": "chat-d2"}, "from": {"id": 2}, "text": "/report run-d1abc"}}
    digest_update = {"update_id": 12014, "message": {"chat": {"id": "chat-d2"}, "from": {"id": 2}, "text": "/digest daily"}}

    assert await gateway.process_update(monitor_update)
    jobs = store.list_watch_jobs(chat_id="chat-d2")
    assert jobs[0].mode == "rsi_extreme"
    assert jobs[0].threshold == 70.0

    assert await gateway.process_update(analyze_update)
    assert await gateway.process_update(report_update)
    assert "Trend up with moderate momentum" in sender.messages[-1][1]

    assert await gateway.process_update(digest_update)
    assert "Daily digest (last 24h)" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_multi_channel_routing_keeps_single_channel_failure_isolated(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-d3", user_id="3", username="u3")
    store.upsert_notification_route(chat_id="chat-d3", channel="email", target="ops@example.com", enabled=True)

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-d3", symbol="AAPL", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

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
            snapshot_id="snap-d3",
            trigger_type="scheduled",
            trigger_id="t-d3",
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

    telegram_sender = FakeTargetSender()
    email_sender = FakeTargetSender(fail=True)
    multi = MultiChannelNotifier(telegram=telegram_sender, email=email_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 1
    assert len(telegram_sender.messages) == 1
    assert len(email_sender.messages) == 1
    assert store.count_retry_queue_depth() == 1


@pytest.mark.asyncio
async def test_gray_release_blocks_non_allowlist_chat(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-gray", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, gray_release_enabled=True)

    update = {"update_id": 12031, "message": {"chat": {"id": "chat-d4"}, "from": {"id": 4}, "text": "/help"}}
    assert await gateway.process_update(update)
    assert "gray release active" in sender.messages[-1][1]
    assert store.count_audit_events(event_type="gray_release_denied") == 1

