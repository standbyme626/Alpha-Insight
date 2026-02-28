from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agents.scanner_engine import WatchSignal, build_scan_trigger
from agents.telegram_command_router import CommandError, parse_telegram_command
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.notification_channels import MultiChannelNotifier, TelegramChannelAdapter
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


class FakeChatSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.photos: list[tuple[str, str, str]] = []
        self.keyboards: list[tuple[str, dict[str, object]]] = []
        self.chat_actions: list[tuple[str, str]] = []
        self.edits: list[tuple[str, int, str]] = []
        self._message_id_counter = 0
        self._message_index_by_id: dict[int, int] = {}

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        self._message_id_counter += 1
        message_id = self._message_id_counter
        self._message_index_by_id[message_id] = len(self.messages)
        self.messages.append((chat_id, text))
        if reply_markup is not None:
            self.keyboards.append((chat_id, reply_markup))
        return {"ok": True, "result": {"message_id": message_id}}

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.edits.append((chat_id, int(message_id), text))
        idx = self._message_index_by_id.get(int(message_id))
        if idx is not None and 0 <= idx < len(self.messages):
            self.messages[idx] = (chat_id, text)
        if reply_markup is not None:
            self.keyboards.append((chat_id, reply_markup))
        return {"ok": True, "result": {"message_id": int(message_id)}}

    async def send_photo(self, chat_id: str, image_path: str, caption: str = "") -> dict[str, object]:
        self.photos.append((chat_id, image_path, caption))
        return {"ok": True}

    async def send_chat_action(self, chat_id: str, action: str = "typing") -> dict[str, object]:
        self.chat_actions.append((chat_id, action))
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


class FlakyTargetSender:
    def __init__(self, *, fail_times: int = 0) -> None:
        self.messages: list[tuple[str, str]] = []
        self._fail_times = fail_times

    async def send_text(self, target: str, text: str) -> dict[str, object]:
        self.messages.append((target, text))
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("channel down")
        return {"ok": True}


class SlowPhotoChatSender(FakeChatSender):
    async def send_photo(self, chat_id: str, image_path: str, caption: str = "") -> dict[str, object]:
        await asyncio.sleep(0.05)
        return await super().send_photo(chat_id, image_path, caption)


@pytest.mark.parametrize(
    ("raw", "name"),
    [
        ("/report run-abc123", "report"),
        ("/digest daily", "digest"),
        ("/monitor TSLA 1h rsi", "monitor"),
        ("/stop", "stop"),
        ("/start", "start"),
        ("/new", "new"),
        ("/status", "status"),
    ],
)
def test_parse_phase_d_commands(raw: str, name: str) -> None:
    parsed = parse_telegram_command(raw)
    assert not isinstance(parsed, CommandError)
    assert parsed.name == name
    if raw == "/stop":
        assert parsed.args["target_type"] == "execution"


def test_extract_news_from_fused_raw_news_items() -> None:
    payload = {
        "fused_insights": {
            "raw": {
                "news_items": [
                    {"source": "YahooFinanceRSS", "title": "one"},
                    {"source": "GoogleNewsRSS#1", "title": "two"},
                ]
            }
        }
    }
    count, window, source = TelegramActions._extract_news(payload, default_days=7)  # noqa: SLF001
    assert count == 2
    assert window == "近7天"
    assert source == "YahooFinanceRSS,GoogleNewsRSS#1"


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
    assert "不支持自动交易" in message
    assert "/monitor <symbol> <interval> [volatility|price|rsi]" in message


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ["你好", "你会什么", "你可以做什么", "怎么用"])
async def test_phase_d_general_conversation_returns_capability_card(tmp_path, text) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-greet", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update(
        {"update_id": 12005 + len(text), "message": {"chat": {"id": "chat-greet"}, "from": {"id": 9}, "text": text}}
    )
    latest = sender.messages[-1][1]
    assert "能力卡" in latest
    assert "示例提问" in latest
    assert "请求已拒绝" not in latest
    assert sender.keyboards
    keyboard = sender.keyboards[-1][1]
    assert keyboard.get("inline_keyboard")
    callbacks = [str(btn.get("callback_data", "")) for row in keyboard.get("inline_keyboard", []) for btn in row]
    assert "guide|new" in callbacks
    assert "guide|status" in callbacks


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
    assert "每日报告（近24小时）" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_status_command_returns_runtime_summary(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-status", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 12020, "message": {"chat": {"id": "chat-status"}, "text": "/status"}})
    assert "运行状态（7x24）" in sender.messages[-1][1]
    assert "活跃监控任务" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_upgrade8_p2_status_card_and_pulse_subscription_are_visible(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-status-p2", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    store.record_metric(metric_name="push_attempt", metric_value=1.0)
    store.record_metric(metric_name="push_attempt", metric_value=1.0)
    store.record_metric(metric_name="push_success", metric_value=1.0)
    store.record_metric(metric_name="lane_dispatch_latency_ms", metric_value=128.5, tags={"channel": "telegram", "lane": "fast"})
    store.set_degradation_state(state_key="chart_text_only", status="active", reason="chart_render_fail_rate")
    store.set_degradation_state(state_key="chart_text_only", status="recovered", reason="chart_recovered")

    assert await gateway.process_update({"update_id": 12021, "message": {"chat": {"id": "chat-status-p2"}, "text": "/pref pulse 1h"}})
    assert await gateway.process_update({"update_id": 12022, "message": {"chat": {"id": "chat-status-p2"}, "text": "/status"}})
    text = sender.messages[-1][1]
    assert "24h投递成功率" in text
    assert "数据源状态" in text
    assert "最近投递延迟" in text
    assert "最近异常" in text
    assert "最近恢复" in text
    assert "脉冲订阅" in text
    assert "1h" in text


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
async def test_phase_d_route_command_set_list_disable_email(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-route", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 12020, "message": {"chat": {"id": "chat-route"}, "text": "/route set email ops@example.com"}})
    assert await gateway.process_update({"update_id": 12021, "message": {"chat": {"id": "chat-route"}, "text": "/route list"}})
    assert "email target=ops@example.com enabled=True" in sender.messages[-1][1]

    assert await gateway.process_update({"update_id": 12022, "message": {"chat": {"id": "chat-route"}, "text": "/route disable email ops@example.com"}})
    assert await gateway.process_update({"update_id": 12023, "message": {"chat": {"id": "chat-route"}, "text": "/route list"}})
    assert "email target=ops@example.com enabled=False" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_d_email_only_route_strategy_dispatches_email_without_telegram(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-email-only", user_id="32", username="u32")
    store.upsert_notification_route(chat_id="chat-email-only", channel="email", target="ops@example.com", enabled=True)

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(
        chat_id="chat-email-only",
        symbol="AAPL",
        interval_sec=300,
        route_strategy="email_only",
        now=base_time,
    )
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
            snapshot_id="snap-email-only",
            trigger_type="scheduled",
            trigger_id="t-email-only",
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
    email_sender = FakeTargetSender()
    multi = MultiChannelNotifier(telegram=telegram_sender, email=email_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 1
    assert len(email_sender.messages) == 1
    assert len(telegram_sender.messages) == 0


@pytest.mark.asyncio
async def test_phase_d_research_only_strategy_tier_guards_notification_dispatch(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-tier-research", user_id="41", username="u41")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(
        chat_id="chat-tier-research",
        symbol="AAPL",
        interval_sec=300,
        strategy_tier="research-only",
        now=base_time,
    )
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.05,
            rsi=70.0,
            priority="critical",
            reason="price_move",
            company_name="Apple",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-tier-research",
            trigger_type="scheduled",
            trigger_id="t-tier-research",
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
            research_status="triggered",
            research_run_id="run-tier-research",
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
    multi = MultiChannelNotifier(telegram=telegram_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 0
    assert len(telegram_sender.messages) == 0
    assert store.count_audit_events(event_type="strategy_tier_guarded") >= 1
    assert store.count_audit_events(event_type="strategy_tier_decision") >= 1

    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            """
            SELECT n.state, n.suppressed_reason, we.strategy_tier
            FROM notifications n
            JOIN watch_events we ON we.event_id = n.event_id
            ORDER BY n.updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    assert row is not None
    assert str(row["state"]) == "suppressed"
    assert str(row["suppressed_reason"]).startswith("strategy_tier_guard")
    assert str(row["strategy_tier"]) == "research-only"


@pytest.mark.asyncio
async def test_phase_d_alert_only_strategy_tier_disables_triggered_research_but_keeps_alerts(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-tier-alert", user_id="42", username="u42")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(
        chat_id="chat-tier-alert",
        symbol="TSLA",
        interval_sec=300,
        strategy_tier="alert-only",
        now=base_time,
    )
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]
    captured: dict[str, object] = {}

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        captured.update(kwargs)
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.04,
            rsi=68.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Tesla",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-tier-alert",
            trigger_type="scheduled",
            trigger_id="t-tier-alert",
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
    multi = MultiChannelNotifier(telegram=telegram_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 1
    assert len(telegram_sender.messages) == 1
    assert captured.get("enable_triggered_research") is False
    assert captured.get("strategy_tier") == "alert-only"

@pytest.mark.asyncio
async def test_phase_d_critical_fast_lane_dispatches_before_high(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-fast-1", user_id="31", username="u31")
    store.upsert_chat_preferences(chat_id="chat-fast-1", summary_mode="full")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-fast-1", symbol="AAPL", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        high_signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.04,
            rsi=67.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Apple",
        )
        critical_signal = WatchSignal(
            symbol="TSLA",
            timestamp=signal_ts,
            price=210.0,
            pct_change=0.09,
            rsi=76.0,
            priority="critical",
            reason="price_move",
            company_name="Tesla",
        )
        high_snapshot = AlertSnapshot(
            snapshot_id="snap-fast-1-high",
            trigger_type="scheduled",
            trigger_id="t-fast-1-high",
            trigger_time=signal_ts,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=high_signal.symbol,
                company_name=high_signal.company_name,
                timestamp=high_signal.timestamp,
                price=high_signal.price,
                pct_change=high_signal.pct_change,
                rsi=high_signal.rsi,
                priority=high_signal.priority,
                reason=high_signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        critical_snapshot = AlertSnapshot(
            snapshot_id="snap-fast-1-critical",
            trigger_type="scheduled",
            trigger_id="t-fast-1-critical",
            trigger_time=signal_ts,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=critical_signal.symbol,
                company_name=critical_signal.company_name,
                timestamp=critical_signal.timestamp,
                price=critical_signal.price,
                pct_change=critical_signal.pct_change,
                rsi=critical_signal.rsi,
                priority=critical_signal.priority,
                reason=critical_signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        return type("RunOut", (), {
            "trigger": build_scan_trigger(trigger_time=signal_ts),
            "signals": [high_signal, critical_signal],
            "selected_alerts": [high_signal, critical_signal],
            "snapshots": [high_snapshot, critical_snapshot],
            "notifications": [],
            "runtime_metrics": {},
            "failure_events": [],
            "failure_clusters": {},
            "alarms": [],
        })()

    telegram_sender = FakeTargetSender()
    multi = MultiChannelNotifier(telegram=telegram_sender)
    executor = WatchExecutor(store=store, notifier=FakeChatSender(), scan_runner=fake_scan_runner, multi_channel_notifier=multi)

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 2
    assert len(telegram_sender.messages) == 2
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            """
            SELECT we.priority
            FROM notifications n
            JOIN watch_events we ON we.event_id = n.event_id
            WHERE n.state = 'delivered'
            ORDER BY n.updated_at ASC
            """
        ).fetchall()
    assert [str(item["priority"]) for item in rows] == ["critical", "high"]
    assert store.count_metric_events(metric_name="lane_dispatch_attempt") == 2


@pytest.mark.asyncio
async def test_phase_d_critical_fast_lane_immediate_retry_recovers_without_queue(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-fast-2", user_id="32", username="u32")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-fast-2", symbol="TSLA", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=210.0,
            pct_change=0.08,
            rsi=75.0,
            priority="critical",
            reason="price_move",
            company_name="Tesla",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-fast-2",
            trigger_type="scheduled",
            trigger_id="t-fast-2",
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

    telegram_sender = FlakyTargetSender(fail_times=1)
    multi = MultiChannelNotifier(telegram=telegram_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
        limits=RuntimeLimits(critical_fast_lane_immediate_retries=1, notification_max_retry=3),
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 1
    assert len(telegram_sender.messages) == 2
    assert store.count_retry_queue_depth() == 0
    assert store.count_metric_events(metric_name="fast_lane_immediate_retry_total") == 1
    assert store.count_metric_events(metric_name="fast_lane_immediate_retry_recovered_total") == 1


@pytest.mark.asyncio
async def test_phase_d_retry_queue_prioritizes_critical_fast_lane(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-fast-3", user_id="33", username="u33")
    store.upsert_chat_preferences(chat_id="chat-fast-3", summary_mode="full")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-fast-3", symbol="AAPL", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        high_signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.04,
            rsi=67.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Apple",
        )
        critical_signal = WatchSignal(
            symbol="TSLA",
            timestamp=signal_ts,
            price=210.0,
            pct_change=0.09,
            rsi=76.0,
            priority="critical",
            reason="price_move",
            company_name="Tesla",
        )
        high_snapshot = AlertSnapshot(
            snapshot_id="snap-fast-3-high",
            trigger_type="scheduled",
            trigger_id="t-fast-3-high",
            trigger_time=signal_ts,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=high_signal.symbol,
                company_name=high_signal.company_name,
                timestamp=high_signal.timestamp,
                price=high_signal.price,
                pct_change=high_signal.pct_change,
                rsi=high_signal.rsi,
                priority=high_signal.priority,
                reason=high_signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        critical_snapshot = AlertSnapshot(
            snapshot_id="snap-fast-3-critical",
            trigger_type="scheduled",
            trigger_id="t-fast-3-critical",
            trigger_time=signal_ts,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=critical_signal.symbol,
                company_name=critical_signal.company_name,
                timestamp=critical_signal.timestamp,
                price=critical_signal.price,
                pct_change=critical_signal.pct_change,
                rsi=critical_signal.rsi,
                priority=critical_signal.priority,
                reason=critical_signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        return type("RunOut", (), {
            "trigger": build_scan_trigger(trigger_time=signal_ts),
            "signals": [high_signal, critical_signal],
            "selected_alerts": [high_signal, critical_signal],
            "snapshots": [high_snapshot, critical_snapshot],
            "notifications": [],
            "runtime_metrics": {},
            "failure_events": [],
            "failure_clusters": {},
            "alarms": [],
        })()

    telegram_sender = FlakyTargetSender(fail_times=2)
    multi = MultiChannelNotifier(telegram=telegram_sender)
    executor = WatchExecutor(
        store=store,
        notifier=FakeChatSender(),
        scan_runner=fake_scan_runner,
        multi_channel_notifier=multi,
        limits=RuntimeLimits(critical_fast_lane_immediate_retries=0, notification_max_retry=3),
    )

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 0
    assert store.count_retry_queue_depth() == 2

    with store._connect() as conn:  # noqa: SLF001
        critical_row = conn.execute(
            """
            SELECT n.notification_id
            FROM notifications n
            JOIN watch_events we ON we.event_id = n.event_id
            WHERE we.job_id = ? AND we.priority = 'critical'
            LIMIT 1
            """,
            (due_job.job_id,),
        ).fetchone()
        high_row = conn.execute(
            """
            SELECT n.notification_id
            FROM notifications n
            JOIN watch_events we ON we.event_id = n.event_id
            WHERE we.job_id = ? AND we.priority <> 'critical'
            LIMIT 1
            """,
            (due_job.job_id,),
        ).fetchone()
        assert critical_row is not None and high_row is not None
        conn.execute(
            "UPDATE notifications SET next_retry_at = ? WHERE notification_id = ?",
            ((base_time - timedelta(minutes=1)).isoformat(), str(critical_row["notification_id"])),
        )
        conn.execute(
            "UPDATE notifications SET next_retry_at = ? WHERE notification_id = ?",
            ((base_time - timedelta(minutes=2)).isoformat(), str(high_row["notification_id"])),
        )

    telegram_sender.messages.clear()
    recovered = await executor.process_retry_queue(limit=5)
    assert recovered == 2
    assert len(telegram_sender.messages) == 2
    with store._connect() as conn:  # noqa: SLF001
        rows = conn.execute(
            """
            SELECT we.priority
            FROM notification_state_transitions t
            JOIN watch_events we ON we.event_id = t.event_id
            WHERE t.from_state = 'retrying' AND t.to_state = 'delivered'
            ORDER BY t.created_at ASC
            """
        ).fetchall()
    assert [str(item["priority"]) for item in rows] == ["critical", "high"]


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


def test_parse_d456_commands() -> None:
    parsed_alerts = parse_telegram_command("/alerts failed 20")
    assert not isinstance(parsed_alerts, CommandError)
    assert parsed_alerts.name == "alerts"
    assert parsed_alerts.args["view"] == "failed"

    parsed_bulk = parse_telegram_command("/bulk interval all 30m")
    assert not isinstance(parsed_bulk, CommandError)
    assert parsed_bulk.name == "bulk"
    assert parsed_bulk.args["action"] == "interval"
    assert parsed_bulk.args["value"] == "1800"

    parsed_pref = parse_telegram_command("/pref priority critical")
    assert not isinstance(parsed_pref, CommandError)
    assert parsed_pref.name == "pref"
    assert parsed_pref.args["value"] == "critical"

    parsed_pulse_pref = parse_telegram_command("/pref pulse 1h")
    assert not isinstance(parsed_pulse_pref, CommandError)
    assert parsed_pulse_pref.name == "pref"
    assert parsed_pulse_pref.args["setting"] == "pulse"
    assert parsed_pulse_pref.args["value"] == "1h"


@pytest.mark.asyncio
async def test_alert_hub_and_bulk_operations(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-d4", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 13001, "message": {"chat": {"id": "chat-d6"}, "text": "/monitor TSLA 1h"}})
    assert await gateway.process_update({"update_id": 13002, "message": {"chat": {"id": "chat-d6"}, "text": "/bulk interval all 30m"}})
    assert await gateway.process_update({"update_id": 13003, "message": {"chat": {"id": "chat-d6"}, "text": "/bulk threshold all 0.07"}})

    jobs = store.list_watch_jobs(chat_id="chat-d6")
    assert len(jobs) == 1
    assert jobs[0].interval_sec == 1800
    assert jobs[0].threshold == 0.07

    store.record_watch_event_if_new(
        job_id=jobs[0].job_id,
        symbol="TSLA",
        trigger_ts=datetime(2026, 2, 27, 1, 0, tzinfo=timezone.utc),
        price=101.0,
        pct_change=0.04,
        reason="price_or_rsi",
        rule="price_or_rsi",
        priority="high",
        run_id=None,
    )
    event = store.list_alert_hub(chat_id="chat-d6", view="triggered", limit=5)
    # No delivery state yet -> empty alert hub rows
    assert not event


@pytest.mark.asyncio
async def test_watchlist_group_webhook_only_routing(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-d7", user_id="7", username="u7")
    hook = store.upsert_outbound_webhook(chat_id="chat-d7", url="https://example.com/hook", secret="abc")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    group = store.create_or_replace_watchlist_group(chat_id="chat-d7", name="grp", symbols=["AAPL", "MSFT"])
    store.create_watch_job(
        chat_id="chat-d7",
        symbol="AAPL",
        scope="group",
        group_id=group.group_id,
        route_strategy="webhook_only",
        interval_sec=300,
        now=base_time,
    )
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[1],
            timestamp=signal_ts,
            price=120.0,
            pct_change=0.06,
            rsi=72.0,
            priority="critical",
            reason="price_or_rsi",
            company_name="MSFT",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-d5",
            trigger_type="scheduled",
            trigger_id="t-d5",
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
    webhook_sender = FakeTargetSender()
    multi = MultiChannelNotifier(telegram=telegram_sender, webhook=webhook_sender)
    executor = WatchExecutor(store=store, notifier=FakeChatSender(), scan_runner=fake_scan_runner, multi_channel_notifier=multi)

    out = await executor.execute_job(due_job)
    assert out.pushed_count == 1
    assert len(telegram_sender.messages) == 0
    assert len(webhook_sender.messages) == 1
    assert webhook_sender.messages[0][0] == hook.webhook_id


@pytest.mark.asyncio
async def test_quiet_hours_and_priority_preference_suppresses_non_critical(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.upsert_telegram_chat(chat_id="chat-d8", user_id="8", username="u8")
    store.upsert_chat_preferences(chat_id="chat-d8", quiet_hours="00-00", min_priority="critical")

    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    store.create_watch_job(chat_id="chat-d8", symbol="AAPL", interval_sec=300, now=base_time)
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=99.0,
            pct_change=0.03,
            rsi=61.0,
            priority="high",
            reason="price_or_rsi",
            company_name="Apple",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-d6",
            trigger_type="scheduled",
            trigger_id="t-d6",
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

    executor = WatchExecutor(store=store, notifier=FakeChatSender(), scan_runner=fake_scan_runner)
    out = await executor.execute_job(due_job)
    assert out.pushed_count == 0
    suppressed = store.list_alert_hub(chat_id="chat-d8", view="suppressed", limit=5)
    assert suppressed
    assert suppressed[0].suppressed_reason in {"quiet_hours", "preference_priority"}


@pytest.mark.asyncio
async def test_phase_a_high_risk_nl_requires_confirm_before_execute(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-a1", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 20001, "message": {"chat": {"id": "chat-a1"}, "from": {"id": 1}, "text": "帮我盯 TSLA 每小时"}}
    assert await gateway.process_update(update)

    pending = store.get_pending_confirm_request(chat_id="chat-a1")
    assert pending is not None
    assert pending.intent == "create_monitor"
    assert store.count_active_watch_jobs(chat_id="chat-a1") == 0


@pytest.mark.asyncio
async def test_phase_a_confirm_callback_binds_request_id_and_no_cross_apply(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-a2", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    first_id = "nlr-first001"
    second_id = "nlr-second01"
    base_payload = {
        "intent": "create_monitor",
        "slots": {
            "symbol": "TSLA",
            "interval": "1h",
            "interval_sec": 3600,
            "template": "volatility",
            "mode": "anomaly",
            "threshold": 0.03,
            "route_strategy": "dual_channel",
        },
        "confidence": 0.95,
        "needs_confirm": True,
        "status": "pending_confirm",
        "text_dedupe_key": "k1",
        "intent_dedupe_key": "ik1",
        "normalized_text": "盯 TSLA 每小时",
        "normalized_request": "create_monitor",
        "action_version": "v1",
        "risk_level": "high",
        "raw_text_hash": "h1",
        "intent_candidate": "create_monitor",
    }
    store.create_nl_request(request_id=first_id, update_id=1, chat_id="chat-a2", **base_payload)
    store.create_nl_request(
        request_id=second_id,
        update_id=2,
        chat_id="chat-a2",
        **{
            **base_payload,
            "slots": {**base_payload["slots"], "symbol": "AAPL"},
            "text_dedupe_key": "k2",
            "intent_dedupe_key": "ik2",
            "raw_text_hash": "h2",
        },
    )

    callback = {
        "update_id": 20002,
        "callback_query": {"id": "cb-1", "data": f"yes|{second_id}", "message": {"chat": {"id": "chat-a2"}}},
    }
    assert await gateway.process_update(callback)
    first = store.get_nl_request(request_id=first_id)
    second = store.get_nl_request(request_id=second_id)
    assert first is not None and second is not None
    assert first.status == "pending_confirm"
    assert second.status == "completed"
    jobs = store.list_watch_jobs(chat_id="chat-a2")
    assert len(jobs) == 1
    assert jobs[0].symbol == "AAPL"


@pytest.mark.asyncio
async def test_phase_a_nl_dedupe_prevents_duplicate_monitor_execution(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-a3", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    first = {"update_id": 20003, "message": {"chat": {"id": "chat-a3"}, "text": "帮我盯 TSLA 每小时"}}
    assert await gateway.process_update(first)
    pending = store.get_pending_confirm_request(chat_id="chat-a3")
    assert pending is not None

    confirm = {
        "update_id": 20004,
        "callback_query": {"id": "cb-2", "data": f"yes|{pending.request_id}", "message": {"chat": {"id": "chat-a3"}}},
    }
    assert await gateway.process_update(confirm)
    assert store.count_active_watch_jobs(chat_id="chat-a3") == 1

    dup = {"update_id": 20005, "message": {"chat": {"id": "chat-a3"}, "text": "帮我盯 TSLA 每小时"}}
    assert await gateway.process_update(dup)
    assert store.count_active_watch_jobs(chat_id="chat-a3") == 1


@pytest.mark.asyncio
async def test_phase_a_monitor_command_compatibility_no_regression(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-a4", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 20006, "message": {"chat": {"id": "chat-a4"}, "text": "/monitor TSLA 1h rsi"}}
    assert await gateway.process_update(update)
    jobs = store.list_watch_jobs(chat_id="chat-a4")
    assert len(jobs) == 1
    assert jobs[0].mode == "rsi_extreme"


@pytest.mark.asyncio
async def test_phase_b_analyze_snapshot_nl_returns_text_and_photo(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    chart = Path(tmp_path / "chart_week3.png")
    chart.write_bytes(b"\x89PNG\r\n" + b"A" * 1024)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-b1",
            "fused_insights": {"summary": "Tencent momentum is positive."},
            "metrics": {"data_close": 321.0, "technical_rsi_14": 58.3},
            "sandbox_artifacts": {"stdout": f"ARTIFACT_PNG={chart}\n"},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 21001, "message": {"chat": {"id": "chat-b1"}, "text": "我要看腾讯一个月涨跌，发K线图和综合分析"}}
    assert await gateway.process_update(update)
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = ?", (21001,)).fetchone()
    assert row is not None and row["request_id"]
    request_id = str(row["request_id"])
    assert await gateway.process_update(
        {"update_id": 21006, "callback_query": {"id": "cb-b1", "data": f"pick|{request_id}|0700.HK", "message": {"chat": {"id": "chat-b1"}}}}
    )

    latest = next(item[1] for item in reversed(sender.messages) if "Snapshot Analysis" in item[1])
    assert "Snapshot Analysis" in latest
    assert "run_id=" not in latest
    assert "request_id=" not in latest
    assert sender.photos
    assert sender.photos[-1][0] == "chat-b1"
    req = store.get_nl_request(request_id=request_id)
    assert req is not None
    assert req.intent == "analyze_snapshot"
    assert req.slots["symbol"] in {"0700.HK", "TCEHY"}


@pytest.mark.asyncio
async def test_phase_b_analyze_snapshot_chart_failure_degrades_to_text(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    huge_chart = Path(tmp_path / "huge_chart.png")
    huge_chart.write_bytes(b"\x89PNG\r\n" + b"B" * (6 * 1024 * 1024))

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-b2",
            "fused_insights": {"summary": "Chart unavailable should fallback."},
            "metrics": {"data_close": 320.0},
            "sandbox_artifacts": {"stdout": f"ARTIFACT_PNG={huge_chart}\n"},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 21002, "message": {"chat": {"id": "chat-b2"}, "text": "分析腾讯一个月走势并发图"}}
    assert await gateway.process_update(update)
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = ?", (21002,)).fetchone()
    assert row is not None and row["request_id"]
    request_id = str(row["request_id"])
    assert await gateway.process_update(
        {"update_id": 21007, "callback_query": {"id": "cb-b2", "data": f"pick|{request_id}|0700.HK", "message": {"chat": {"id": "chat-b2"}}}}
    )
    assert sender.messages
    assert sender.photos == []
    fail_metrics = store.metric_values(metric_name="chart_render_fail_rate")
    assert fail_metrics


@pytest.mark.asyncio
async def test_phase_b_analyze_snapshot_uses_output_files_chart_fallback(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    chart = Path(tmp_path / "chart_from_output_files.png")
    chart.write_bytes(b"\x89PNG\r\n" + b"D" * 1024)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-b2-fallback",
            "fused_insights": {"summary": "Chart comes from output_files."},
            "metrics": {"data_close": 123.4, "technical_rsi_14": 52.1},
            "sandbox_output_files": [str(chart)],
            "sandbox_artifacts": {"stdout": ""},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 21008, "message": {"chat": {"id": "chat-b2-fallback"}, "text": "分析 TSLA 一个月走势并发图"}}
    assert await gateway.process_update(update)
    assert sender.photos
    assert sender.photos[-1][0] == "chat-b2-fallback"


@pytest.mark.asyncio
async def test_phase_b_data_empty_chart_failure_skips_artifact_retry(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-b2-data-empty",
            "fused_insights": {"summary": "No data available."},
            "metrics": {},
            "sandbox_artifacts": {"stdout": ""},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {"update_id": 21009, "message": {"chat": {"id": "chat-b2-data-empty"}, "text": "分析 TSLA 一个月走势并发图"}}
    assert await gateway.process_update(update)
    assert any("缺少可绘图的数据区间" in str(item[1]) for item in sender.messages)
    assert not any("首次未取到图表产物" in str(item[1]) for item in sender.messages)


@pytest.mark.asyncio
async def test_phase_b_analyze_snapshot_singleflight_reuses_equivalent_followup(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {
            "run_id": f"run-b3-{calls['n']}",
            "fused_insights": {"summary": "ok"},
            "metrics": {"data_close": 100.0},
            "sandbox_artifacts": {"stdout": ""},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    first = {"update_id": 21003, "message": {"chat": {"id": "chat-b3"}, "text": "TSLA 一个月涨跌"}}
    second = {"update_id": 21004, "message": {"chat": {"id": "chat-b3"}, "text": "分析 TSLA 一个月走势"}}
    third = {"update_id": 21005, "message": {"chat": {"id": "chat-b3"}, "text": "TSLA 一个月涨跌"}}

    assert await gateway.process_update(first)
    assert await gateway.process_update(second)
    assert calls["n"] == 1
    assert "复用" in sender.messages[-1][1]
    assert await gateway.process_update(third)
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_phase_d_analyze_snapshot_ack_then_result_order(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        await asyncio.sleep(0.01)
        return {
            "run_id": "run-order",
            "fused_insights": {"summary": "Ordered response check"},
            "metrics": {"data_close": 101.0, "technical_rsi_14": 56.0},
            "sandbox_artifacts": {"stdout": ""},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 22001, "message": {"chat": {"id": "chat-order"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = ?", (22001,)).fetchone()
    assert row is not None and row["request_id"]
    texts = [item[1] for item in sender.messages]
    ack_idx = next(index for index, text in enumerate(texts) if "已受理请求，开始分析" in text)
    result_idx = next(index for index, text in enumerate(texts) if "Snapshot Analysis" in text)
    assert ack_idx < result_idx


@pytest.mark.asyncio
async def test_phase_d_typing_heartbeat_start_and_stop(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        await asyncio.sleep(0.05)
        return {
            "run_id": "run-typing",
            "fused_insights": {"summary": "typing heartbeat"},
            "metrics": {"data_close": 88.0},
            "sandbox_artifacts": {"stdout": ""},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 22002, "message": {"chat": {"id": "chat-typing"}, "text": "分析 TSLA 一个月走势"}})
    action_count = len(sender.chat_actions)
    assert action_count >= 1

    await asyncio.sleep(0.05)
    assert len(sender.chat_actions) == action_count


@pytest.mark.asyncio
async def test_phase_d_singleflight_reuses_inflight_snapshot_request(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-singleflight", "metrics": {"data_close": 10.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    existing_request_id = "nlr-sf-inflight"
    assert store.create_nl_request(
        request_id=existing_request_id,
        update_id=22999,
        chat_id="chat-sf",
        intent="analyze_snapshot",
        slots={
            "symbol": "TSLA",
            "period": "1mo",
            "interval": "1d",
            "need_chart": False,
            "need_news": False,
            "_context_scope_key": "chat:chat-sf",
            "_schema_version": "telegram_nlu_plan_v2",
        },
        confidence=0.98,
        needs_confirm=False,
        status="executing",
        text_dedupe_key="sf1-t",
        intent_dedupe_key="sf1-i",
        normalized_text="sf1",
        normalized_request="sf1",
        action_version="v2",
        risk_level="low",
        raw_text_hash="sf1-h",
        intent_candidate="analyze_snapshot",
    )

    assert await gateway.process_update({"update_id": 23001, "message": {"chat": {"id": "chat-sf"}, "text": "分析 TSLA 一个月走势"}})
    assert "同会话已有相同分析进行中" in sender.messages[-1][1]
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT command, request_id FROM bot_updates WHERE update_id = 23001").fetchone()
        count = conn.execute("SELECT COUNT(*) AS c FROM nl_requests WHERE chat_id = 'chat-sf'").fetchone()
    assert row is not None
    assert row["command"] == "nl_singleflight_inflight"
    assert row["request_id"] == existing_request_id
    assert count is not None and int(count["c"]) == 1


@pytest.mark.asyncio
async def test_phase_d_singleflight_reuses_completed_snapshot_request(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-singleflight-completed", "metrics": {"data_close": 10.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    existing_request_id = "nlr-sf-completed"
    assert store.create_nl_request(
        request_id=existing_request_id,
        update_id=23002,
        chat_id="chat-sf2",
        intent="analyze_snapshot",
        slots={
            "symbol": "TSLA",
            "period": "1mo",
            "interval": "1d",
            "need_chart": False,
            "need_news": True,
            "_context_scope_key": "chat:chat-sf2",
            "_schema_version": "telegram_nlu_plan_v2",
        },
        confidence=0.98,
        needs_confirm=False,
        status="completed",
        text_dedupe_key="sf2-t",
        intent_dedupe_key="sf2-i",
        normalized_text="sf2",
        normalized_request="sf2",
        action_version="v2",
        risk_level="low",
        raw_text_hash="sf2-h",
        intent_candidate="analyze_snapshot",
    )
    store.upsert_analysis_report(
        run_id="run-sf2",
        request_id=existing_request_id,
        chat_id="chat-sf2",
        symbol="TSLA",
        summary="cached result",
        key_metrics={"data_close": 99.0},
    )

    assert await gateway.process_update({"update_id": 23003, "message": {"chat": {"id": "chat-sf2"}, "text": "分析 TSLA 一个月走势"}})
    assert "复用最近分析结果" in sender.messages[-1][1]
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT command, request_id FROM bot_updates WHERE update_id = 23003").fetchone()
    assert row is not None
    assert row["command"] == "nl_singleflight_reuse"
    assert row["request_id"] == existing_request_id


def test_phase_d_upsert_analysis_report_is_idempotent_on_request_id(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")

    store.upsert_analysis_report(
        run_id="run-first",
        request_id="req-same",
        chat_id="chat-upsert",
        symbol="TSLA",
        summary="first summary",
        key_metrics={"data_close": 10.0},
    )
    store.upsert_analysis_report(
        run_id="run-second",
        request_id="req-same",
        chat_id="chat-upsert",
        symbol="TSLA",
        summary="second summary",
        key_metrics={"data_close": 20.0},
    )

    report = store.get_analysis_report(report_id="req-same", chat_id="chat-upsert")
    assert report is not None
    assert report.run_id == "run-second"
    assert report.summary == "second summary"
    assert report.key_metrics.get("data_close") == 20.0

    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT COUNT(*) AS c FROM analysis_reports WHERE request_id = 'req-same'").fetchone()
    assert row is not None and int(row["c"]) == 1


@pytest.mark.asyncio
async def test_phase_d_send_progress_updates_can_be_disabled(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    limits = RuntimeLimits(send_progress_updates=False)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-no-progress",
            "fused_insights": {"summary": "no progress text"},
            "metrics": {"data_close": 120.0},
            **kwargs,
        }

    actions = TelegramActions(
        store=store,
        notifier=sender,
        research_runner=fake_runner,
        analysis_timeout_seconds=5,
        limits=limits,
    )
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    assert await gateway.process_update({"update_id": 23004, "message": {"chat": {"id": "chat-no-progress"}, "text": "分析 TSLA 一个月走势"}})
    messages = [item[1] for item in sender.messages]
    assert any("已受理请求，开始分析" in item for item in messages)
    assert not any("阶段进度" in item for item in messages)


@pytest.mark.asyncio
async def test_upgrade8_p0_progress_uses_single_message_with_edit_updates(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    limits = RuntimeLimits(send_progress_updates=True)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-progress-edit",
            "fused_insights": {"summary": "progress update"},
            "metrics": {"data_close": 110.0, "window_low": 95.0, "window_high": 115.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5, limits=limits)
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    assert await gateway.process_update({"update_id": 230045, "message": {"chat": {"id": "chat-progress-edit"}, "text": "分析 TSLA 一个月走势"}})
    progress_msgs = [text for _, text in sender.messages if "阶段进度" in text]
    assert len(progress_msgs) == 1
    assert "阶段进度 4/4：图表处理" in progress_msgs[0]
    assert len(sender.edits) >= 1


@pytest.mark.asyncio
async def test_upgrade8_p0_final_card_is_idempotent_on_request_and_schema(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-final-idem",
            "fused_insights": {"summary": "idempotent final card"},
            "metrics": {
                "data_close": 88.0,
                "window_low": 80.0,
                "window_high": 92.0,
                "return_30d": 0.05,
            },
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    await actions.handle_analyze_snapshot(
        chat_id="chat-final-idem",
        symbol="TSLA",
        period="1mo",
        interval="1d",
        need_chart=False,
        need_news=True,
        request_id="req-final-idem",
    )
    await actions.handle_analyze_snapshot(
        chat_id="chat-final-idem",
        symbol="TSLA",
        period="1mo",
        interval="1d",
        need_chart=False,
        need_news=True,
        request_id="req-final-idem",
    )
    final_cards = [text for _, text in sender.messages if "Card A｜区间表现" in text]
    assert len(final_cards) == 1


@pytest.mark.asyncio
async def test_phase_d_photo_send_timeout_isolated_from_research_timeout(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = SlowPhotoChatSender()
    chart = Path(tmp_path / "chart_timeout.png")
    chart.write_bytes(b"\x89PNG\r\n" + b"T" * 1024)
    limits = RuntimeLimits(photo_send_timeout_seconds=0.01, analysis_snapshot_timeout_seconds=5.0)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-photo-timeout",
            "fused_insights": {"summary": "photo timeout"},
            "metrics": {"data_close": 66.0},
            "sandbox_artifacts": {"stdout": f"ARTIFACT_PNG={chart}\n"},
            **kwargs,
        }

    actions = TelegramActions(
        store=store,
        notifier=sender,
        research_runner=fake_runner,
        limits=limits,
    )
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    assert await gateway.process_update({"update_id": 23005, "message": {"chat": {"id": "chat-photo-timeout"}, "text": "分析 TSLA 一个月走势并发图"}})
    assert sender.photos == []
    assert any("图表发送失败" in item[1] for item in sender.messages)


@pytest.mark.asyncio
async def test_phase_d_telegram_channel_adapter_supports_progress_and_photo_fallback() -> None:
    sender = FakeChatSender()
    adapter = TelegramChannelAdapter(sender)

    progress = await adapter.send_progress(chat_id="chat-a", text="progress", reply_markup=None)
    photo = await adapter.send_photo(chat_id="chat-a", image_path="/tmp/not-used.png", caption="c")
    assert progress.delivered is True
    assert photo.delivered is True

    class TextOnlySender:
        async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:  # noqa: ARG002
            return {"ok": True}

    adapter_text_only = TelegramChannelAdapter(TextOnlySender())
    photo_fallback = await adapter_text_only.send_photo(chat_id="chat-b", image_path="/tmp/not-used.png")
    assert photo_fallback.delivered is False
    assert photo_fallback.error == "send_photo_not_supported"


@pytest.mark.asyncio
async def test_phase_d_nl_list_jobs_and_daily_digest_intents(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-d-intent", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30001, "message": {"chat": {"id": "chat-dn1"}, "text": "/monitor TSLA 1h"}})
    assert await gateway.process_update({"update_id": 30002, "message": {"chat": {"id": "chat-dn1"}, "text": "看看我的监控列表"}})
    assert "活跃监控任务" in sender.messages[-1][1]

    assert await gateway.process_update({"update_id": 30003, "message": {"chat": {"id": "chat-dn1"}, "text": "给我每日报告"}})
    assert "每日报告" in sender.messages[-1][1]

    with store._connect() as conn:  # noqa: SLF001
        list_row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30002").fetchone()
        digest_row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30003").fetchone()
    assert list_row is not None and digest_row is not None
    list_req = store.get_nl_request(request_id=str(list_row["request_id"]))
    digest_req = store.get_nl_request(request_id=str(digest_row["request_id"]))
    assert list_req is not None and list_req.intent == "list_jobs" and list_req.status == "completed"
    assert digest_req is not None and digest_req.intent == "daily_digest" and digest_req.status == "completed"


@pytest.mark.asyncio
async def test_phase_d_nl_stop_job_requires_confirm_then_executes(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-d-stop", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30011, "message": {"chat": {"id": "chat-dn2"}, "text": "/monitor TSLA 1h"}})
    assert store.count_active_watch_jobs(chat_id="chat-dn2") == 1

    assert await gateway.process_update({"update_id": 30012, "message": {"chat": {"id": "chat-dn2"}, "text": "停止 TSLA 监控"}})
    pending = store.get_pending_confirm_request(chat_id="chat-dn2")
    assert pending is not None
    assert pending.intent == "stop_job"
    assert store.count_active_watch_jobs(chat_id="chat-dn2") == 1

    assert await gateway.process_update(
        {
            "update_id": 30013,
            "callback_query": {"id": "cb-dn2", "data": f"yes|{pending.request_id}", "message": {"chat": {"id": "chat-dn2"}}},
        }
    )
    assert store.count_active_watch_jobs(chat_id="chat-dn2") == 0
    req = store.get_nl_request(request_id=pending.request_id)
    assert req is not None and req.status == "completed"


@pytest.mark.asyncio
async def test_phase_d_plan_steps_and_evidence_trace_versions(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-d-plan",
            "fused_insights": {"summary": "Plan/evidence trace."},
            "metrics": {"data_close": 10.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30021, "message": {"chat": {"id": "chat-dn3"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30021").fetchone()
    assert row is not None
    request_id = str(row["request_id"])
    req = store.get_nl_request(request_id=request_id)
    assert req is not None
    assert req.action_version == "v2"
    assert req.slots.get("_schema_version") == "telegram_nlu_plan_v2"

    plan_steps = store.list_nl_plan_step_events(request_id=request_id)
    assert plan_steps
    assert any(str(item.get("status", "")).lower() == "completed" for item in plan_steps)
    evidence = store.list_nl_execution_evidence(request_id=request_id)
    assert evidence
    assert evidence[0]["request_id"] == request_id
    assert evidence[0]["schema_version"] == "telegram_nlu_plan_v2"
    assert evidence[0]["action_version"] == "v2"
    assert evidence[0]["result"] == "completed"

    report = store.build_phase_d_run_report()
    assert report["nl_execution_evidence_total"] >= 1
    assert report["nl_plan_step_total"] >= 1
    assert report["nl_evidence_mapped_total"] >= 1
    assert "clarify_followup_success_rate" in report
    assert "analysis_explainability_rate" in report
    assert "chart_fail_reason_topk" in report


@pytest.mark.asyncio
async def test_phase_d_v2_compatible_with_v1_replay_request(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-d-v1",
            "fused_insights": {"summary": "v1 replay"},
            "metrics": {"data_close": 10.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    store.insert_bot_update_if_new(update_id=30031, chat_id="chat-dn4", payload={"message": {"chat": {"id": "chat-dn4"}, "text": "legacy"}})
    created = store.create_nl_request(
        request_id="nlr-legacyv1",
        update_id=30031,
        chat_id="chat-dn4",
        intent="analyze_snapshot",
        slots={"symbol": "TSLA", "period": "1mo", "interval": "1d", "need_chart": False, "need_news": False},
        confidence=0.95,
        needs_confirm=False,
        status="queued",
        text_dedupe_key="t-v1",
        intent_dedupe_key="i-v1",
        normalized_text="legacy",
        normalized_request="analyze_snapshot legacy",
        action_version="v1",
        risk_level="low",
        raw_text_hash="legacyhash",
        intent_candidate="analyze_snapshot",
    )
    assert created
    assert await gateway._execute_nl_request(update_id=30031, request_id="nlr-legacyv1")  # noqa: SLF001
    req = store.get_nl_request(request_id="nlr-legacyv1")
    assert req is not None and req.status == "completed"
    evidence = store.list_nl_execution_evidence(request_id="nlr-legacyv1")
    assert evidence
    assert evidence[0]["action_version"] == "v1"


@pytest.mark.asyncio
async def test_phase_d_clarify_once_returns_command_template(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-d-clarify", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30041, "message": {"chat": {"id": "chat-dn5"}, "text": "帮我盯 每小时"}})
    assert "one follow-up within 5 minutes" in sender.messages[-1][1]
    assert "/monitor <symbol> <interval>" in sender.messages[-1][1]
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30041").fetchone()
    assert row is not None
    req = store.get_nl_request(request_id=str(row["request_id"]))
    assert req is not None
    assert req.status == "clarify_pending"
    assert req.reject_reason == "clarify_needed"
    assert store.count_metric_events(metric_name="nl_clarify_asked_total") == 1


@pytest.mark.asyncio
async def test_phase_d_blacklist_mode_allows_non_blocked_chat(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-blacklist-1", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, access_mode="blacklist", blocked_chat_ids={"chat-blocked"})

    assert await gateway.process_update({"update_id": 30051, "message": {"chat": {"id": "chat-open"}, "text": "/help"}})
    assert "可用命令" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_d_blacklist_mode_denies_blocked_chat(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-blacklist-2", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, access_mode="blacklist", blocked_chat_ids={"chat-blocked"})

    assert await gateway.process_update({"update_id": 30052, "message": {"chat": {"id": "chat-blocked"}, "text": "/help"}})
    assert "Permission denied" in sender.messages[-1][1]
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT status,command,error FROM bot_updates WHERE update_id = 30052").fetchone()
    assert row is not None
    assert row["status"] == "failed"
    assert row["command"] == "source_denied"
    assert "blocklisted" in str(row["error"])


@pytest.mark.asyncio
async def test_phase_d_chart_like_text_without_symbol_returns_clarify_template(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-chart-clarify", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30053, "message": {"chat": {"id": "chat-chart"}, "text": "看看k线图"}})
    assert "one follow-up within 5 minutes" in sender.messages[-1][1]
    assert "/analyze <symbol>" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_s_clarify_followup_success_executes_analysis(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-s-followup",
            "fused_insights": {"summary": "Follow-up resolved."},
            "metrics": {"data_close": 100.0, "technical_rsi_14": 55.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30101, "message": {"chat": {"id": "chat-s1"}, "text": "看看k线图"}})
    assert await gateway.process_update({"update_id": 30102, "message": {"chat": {"id": "chat-s1"}, "text": "TSLA"}})
    assert any("Snapshot Analysis" in item[1] for item in sender.messages)
    assert store.count_metric_events(metric_name="nl_clarify_resolved_total") == 1
    assert store.get_clarify_pending(chat_id="chat-s1") is None


@pytest.mark.asyncio
async def test_phase_s_clarify_timeout_returns_template(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-s-timeout", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30111, "message": {"chat": {"id": "chat-s2"}, "text": "看看k线图"}})
    with store._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE clarify_pending SET expires_at = ? WHERE chat_id = ?",
            ("2000-01-01T00:00:00+00:00", "chat-s2"),
        )
    assert await gateway.process_update({"update_id": 30112, "message": {"chat": {"id": "chat-s2"}, "text": "TSLA"}})
    assert "Clarify timeout" in sender.messages[-1][1]
    assert "/analyze <symbol>" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_s_chart_failure_reason_is_explainable(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    huge_chart = Path(tmp_path / "huge_chart_phase_s.png")
    huge_chart.write_bytes(b"\x89PNG\r\n" + b"C" * (6 * 1024 * 1024))

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-s-chart",
            "fused_insights": {"summary": "Chart fallback explainable."},
            "metrics": {"data_close": 320.0},
            "sandbox_artifacts": {"stdout": f"ARTIFACT_PNG={huge_chart}\n"},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30121, "message": {"chat": {"id": "chat-s3"}, "text": "分析 TSLA 一个月走势并发图"}})
    assert any("没能生成价格图表" in item[1] for item in sender.messages)
    assert sender.photos == []


@pytest.mark.asyncio
async def test_phase_s_report_full_contains_evidence_block(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-s-report",
            "fused_insights": {"summary": "Report full evidence block."},
            "metrics": {"data_close": 210.0, "technical_rsi_14": 61.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30131, "message": {"chat": {"id": "chat-s4"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30132, "message": {"chat": {"id": "chat-s4"}, "text": "/report run-s-report full"}})
    assert "证据块：" in sender.messages[-1][1]
    assert "执行事件数：" in sender.messages[-1][1]
    lowered = sender.messages[-1][1].lower()
    assert "schema_version" not in lowered
    assert "action_version" not in lowered


@pytest.mark.asyncio
async def test_phase_t_candidate_selection_timeout_returns_examples(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-t-timeout", "metrics": {"data_close": 1.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30201, "message": {"chat": {"id": "chat-t1"}, "text": "分析腾讯走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30201").fetchone()
        assert row is not None
        conn.execute(
            "UPDATE pending_candidate_selection SET expires_at = ? WHERE request_id = ?",
            ("2000-01-01T00:00:00+00:00", str(row["request_id"])),
        )
    assert await gateway.process_update(
        {"update_id": 30202, "callback_query": {"id": "cb-t1", "data": f"pick|{row['request_id']}|0700.HK", "message": {"chat": {"id": "chat-t1"}}}}
    )
    assert "候选点选已过期" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_t_reset_clears_context_and_pending(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-t-reset", "metrics": {"data_close": 10.0, "technical_rsi_14": 50.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30211, "message": {"chat": {"id": "chat-t2"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30212, "message": {"chat": {"id": "chat-t2"}, "text": "/reset"}})
    assert "已清空上下文" in sender.messages[-1][1]
    assert store.get_conversation_context(scope_key="chat:chat-t2") is None


@pytest.mark.asyncio
async def test_phase_t_new_command_clears_context_and_returns_capability(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-t-new", "metrics": {"data_close": 10.0, "technical_rsi_14": 50.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30213, "message": {"chat": {"id": "chat-t-new"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30214, "message": {"chat": {"id": "chat-t-new"}, "text": "/new"}})
    texts = [item[1] for item in sender.messages]
    assert any("已开启新对话" in text for text in texts)
    assert "能力卡" in sender.messages[-1][1]
    assert store.get_conversation_context(scope_key="chat:chat-t-new") is None


@pytest.mark.asyncio
async def test_phase_t_new_phrase_clears_context_and_returns_capability(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-t-new-phrase", "metrics": {"data_close": 10.0, "technical_rsi_14": 50.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30215, "message": {"chat": {"id": "chat-t-new2"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30216, "message": {"chat": {"id": "chat-t-new2"}, "text": "新对话"}})
    texts = [item[1] for item in sender.messages]
    assert any("已开启新对话" in text for text in texts)
    assert "能力卡" in sender.messages[-1][1]
    assert store.get_conversation_context(scope_key="chat:chat-t-new2") is None

@pytest.mark.asyncio
async def test_phase_t_group_context_isolation_by_user(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-t-group", "metrics": {"data_close": 20.0, "technical_rsi_14": 52.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update(
        {
            "update_id": 30221,
            "message": {"chat": {"id": "chat-group", "type": "group"}, "from": {"id": 1001}, "text": "分析 TSLA 一个月走势"},
        }
    )
    assert await gateway.process_update(
        {
            "update_id": 30222,
            "message": {"chat": {"id": "chat-group", "type": "group"}, "from": {"id": 2002}, "text": "看看新闻怎么说"},
        }
    )
    assert "one follow-up within 5 minutes" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_p1_candidate_selection_uses_inline_keyboard_buttons(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p1-candidate", "metrics": {"data_close": 10.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30231, "message": {"chat": {"id": "chat-p1a"}, "text": "分析腾讯走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30231").fetchone()
    assert row is not None
    req_id = str(row["request_id"])
    selector_text = sender.messages[-1][1]
    assert "排序依据" in selector_text
    assert "默认候选" in selector_text
    assert sender.keyboards
    inline = sender.keyboards[-1][1].get("inline_keyboard")
    assert isinstance(inline, list) and inline
    labels = [str(button["text"]) for row in inline for button in row]
    assert 1 <= len(labels) <= 5
    assert all("｜" in label for label in labels)
    assert any("港股" in label and "腾讯" in label for label in labels)
    first_button = inline[0][0]
    assert str(first_button["callback_data"]).startswith(f"pick|{req_id[-6:]}|")


@pytest.mark.asyncio
async def test_phase_p1_candidate_selection_supports_natural_language_market_choice(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    calls: list[str] = []

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls.append(str(kwargs.get("symbol", "")))
        return {"run_id": "run-p1-candidate-natural", "metrics": {"data_close": 10.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30235, "message": {"chat": {"id": "chat-p1-natural"}, "text": "分析腾讯走势"}})
    assert await gateway.process_update({"update_id": 30236, "message": {"chat": {"id": "chat-p1-natural"}, "text": "选港股那个"}})
    assert calls
    assert calls[-1] == "0700.HK"
    assert any("已选择标的 0700.HK" in text for _, text in sender.messages)


@pytest.mark.asyncio
async def test_phase_p1_news_window_toggle_30_days_via_callback(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-p1-news",
            "fused_insights": {"summary": "news window toggle"},
            "metrics": {"data_close": 99.0, "window_low": 90.0, "window_high": 110.0},
            "news": [{"title": "headline", "source": "wire"}],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30241, "message": {"chat": {"id": "chat-p1b"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30241").fetchone()
    assert row is not None
    req_id = str(row["request_id"])
    assert await gateway.process_update(
        {"update_id": 30242, "callback_query": {"id": "cb-p1-news", "data": f"act|{req_id[-6:]}|news30", "message": {"chat": {"id": "chat-p1b"}}}}
    )
    assert any("窗口=近30天" in text for _, text in sender.messages)


@pytest.mark.asyncio
async def test_phase_p1_dedupe_echoes_ready_state_without_internal_ids(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {
            "run_id": f"run-p1-dedupe-{calls['n']}",
            "fused_insights": {"summary": "dedupe"},
            "metrics": {"data_close": 101.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)
    gateway._build_bucket = staticmethod(lambda now, seconds=30: 123456)  # type: ignore[method-assign]  # noqa: SLF001

    assert await gateway.process_update({"update_id": 30251, "message": {"chat": {"id": "chat-p1c"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30252, "message": {"chat": {"id": "chat-p1c"}, "text": "分析 TSLA 一个月走势"}})
    assert calls["n"] == 1
    assert "复用最近分析结果" in sender.messages[-1][1]
    assert "run_id=" not in sender.messages[-1][1].lower()
    assert "request_id=" not in sender.messages[-1][1].lower()


@pytest.mark.asyncio
async def test_phase_p2_user_copy_forbidden_words_guard(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-p2-guard",
            "fused_insights": {"summary": "metrics unavailable traceback chart_missing run_id=abc request_id=req-1 action=analyze internal_score=0.8"},
            "metrics": {"data_close": 123.0, "technical_rsi_14": 51.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30301, "message": {"chat": {"id": "chat-p2a"}, "text": "分析 TSLA 一个月走势"}})
    merged = "\n".join(text for _, text in sender.messages).lower()
    assert "metrics unavailable" not in merged
    assert "chart_missing" not in merged
    assert "traceback" not in merged
    assert "run_id" not in merged
    assert "request_id" not in merged
    assert "action=analyze" not in merged
    assert "internal_score" not in merged


@pytest.mark.asyncio
async def test_upgrade7_user_response_contract_baseline_and_density(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-u7-contract",
            "fused_insights": {
                "summary": "schema_version action_version raw_error traceback "
                "整体趋势偏强，但接近区间上沿，短线不宜追高。"
            },
            "metrics": {
                "data_close": 205.0,
                "technical_rsi_14": 62.5,
                "window_low": 180.0,
                "window_high": 210.0,
            },
            "news": [{"title": "headline", "source": "wire"}],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30306, "message": {"chat": {"id": "chat-u7a"}, "text": "分析 TSLA 一个月走势"}})
    contract_msgs = [text for _, text in sender.messages if "Card A｜区间表现" in text]
    assert len(contract_msgs) == 1
    main = contract_msgs[0]
    assert "Card A｜区间表现" in main
    assert "Card B｜原因摘要" in main
    assert "Card C｜证据三件套" in main
    assert "Card D｜动作入口" in main
    assert len(main) <= 1200
    lowered = main.lower()
    assert "schema_version" not in lowered
    assert "action_version" not in lowered
    assert "raw_error" not in lowered
    assert "traceback" not in lowered


@pytest.mark.asyncio
async def test_upgrade7_analyze_command_and_snapshot_have_same_contract_shape(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-u7-shape",
            "fused_insights": {"summary": "Trend check."},
            "metrics": {"data_close": 101.2, "technical_rsi_14": 55.0, "window_low": 90.0, "window_high": 110.0},
            "news": [{"title": "headline", "source": "wire"}],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30307, "message": {"chat": {"id": "chat-u7b"}, "text": "/analyze TSLA"}})
    cmd_msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)
    assert await gateway.process_update({"update_id": 30308, "message": {"chat": {"id": "chat-u7c"}, "text": "分析 TSLA 一个月走势"}})
    snapshot_msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)

    for label in ("Card A｜区间表现", "Card B｜原因摘要", "Card C｜证据三件套", "Card D｜动作入口", "类型: Snapshot Analysis"):
        assert label in cmd_msg
        assert label in snapshot_msg


@pytest.mark.asyncio
async def test_upgrade7_context_carry_prompt_is_explicit_and_switchable(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    limits = RuntimeLimits(session_singleflight_ttl_seconds=0)

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": f"run-u7-carry-{kwargs.get('symbol', 'na')}",
            "fused_insights": {"summary": "carry context"},
            "metrics": {"data_close": 88.0, "technical_rsi_14": 52.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, limits=limits, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    assert await gateway.process_update({"update_id": 30309, "message": {"chat": {"id": "chat-u7d"}, "text": "分析 TSLA 一个月走势"}})
    assert await gateway.process_update({"update_id": 30310, "message": {"chat": {"id": "chat-u7d"}, "text": "分析最近走势"}})
    assert any("已沿用上次上下文" in text and "TSLA" in text and "换标的" in text for _, text in sender.messages)


@pytest.mark.asyncio
async def test_upgrade8_p0_snapshot_card_contains_first_screen_required_fields(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "Date": (start + timedelta(days=i)).isoformat(),
                "Open": 170.0 + i,
                "High": 171.0 + i,
                "Low": 169.0 + i,
                "Close": 170.0 + i,
            }
            for i in range(30)
        ]
        return {
            "run_id": "run-p2-order",
            "fused_insights": {"summary": "fixed order"},
            "metrics": {
                "data_close": 200.0,
                "technical_rsi_14": 56.0,
                "window_low": 180.0,
                "window_high": 220.0,
                "return_30d": 0.1,
                "max_drawdown_30d": -0.08,
                "market_data_source": "AlphaFeed",
                "market_data_updated_at": "2026-02-28T09:30:00+00:00",
                "data_window": "2026-01-28 ~ 2026-02-27",
            },
            "records": records,
            "news": [
                {"title": "earnings beat", "source": "WireA", "published_at": "2026-02-27T08:00:00+00:00"},
                {"title": "regulator probe", "source": "WireB", "published_at": "2026-02-27T07:00:00+00:00"},
            ],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30311, "message": {"chat": {"id": "chat-p2b"}, "text": "分析 TSLA 一个月走势"}})
    msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)
    assert "近30天：" in msg
    assert "涨跌额" in msg
    assert "区间：低" in msg
    assert "振幅" in msg
    assert "最大回撤" in msg
    assert "现价区间位置：" in msg
    assert "Card B｜原因摘要" in msg
    assert "技术：" in msg
    assert "新闻：" in msg
    assert "Card C｜证据三件套" in msg
    assert "行情源=" in msg
    assert "新闻源覆盖=" in msg
    assert "指标口径=" in msg
    assert "MA10(1d)" in msg
    assert "MA20(1d)" in msg


@pytest.mark.asyncio
async def test_upgrade8_p1_snapshot_card_news_section_contains_thematic_top3_with_representative_news(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "Date": (start + timedelta(days=i)).isoformat(),
                "Open": 170.0 + i,
                "High": 171.0 + i,
                "Low": 169.0 + i,
                "Close": 170.0 + i,
            }
            for i in range(30)
        ]
        return {
            "run_id": "run-p1-theme",
            "fused_insights": {"summary": "theme output"},
            "metrics": {
                "data_close": 200.0,
                "technical_rsi_14": 56.0,
                "window_low": 180.0,
                "window_high": 220.0,
                "return_30d": 0.1,
                "max_drawdown_30d": -0.08,
                "market_data_source": "AlphaFeed",
                "market_data_updated_at": "2026-02-28T09:30:00+00:00",
            },
            "records": records,
            "news": [
                {
                    "title": "Q4 earnings beat estimates",
                    "publisher": "WireA",
                    "url": "https://wirea.example/earnings",
                    "published_at": "2026-02-27T08:00:00+00:00",
                },
                {
                    "title": "Regulator opens probe into filings",
                    "publisher": "WireB",
                    "url": "https://wireb.example/regulator",
                    "published_at": "2026-02-27T07:00:00+00:00",
                },
                {
                    "title": "New product launch gains traction",
                    "publisher": "WireC",
                    "url": "https://wirec.example/product",
                    "published_at": "2026-02-26T09:00:00+00:00",
                },
                {
                    "title": "Fed signals lower rate path",
                    "publisher": "WireD",
                    "url": "https://wired.example/macro",
                    "published_at": "2026-02-26T06:00:00+00:00",
                },
                {
                    "title": "Earnings guidance maintained",
                    "publisher": "WireE",
                    "url": "https://wiree.example/guidance",
                    "published_at": "2026-02-25T06:00:00+00:00",
                },
            ],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30312, "message": {"chat": {"id": "chat-p1-theme"}, "text": "分析 TSLA 一个月走势"}})
    msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)
    assert "主题Top3：" in msg
    assert "1) 财报：" in msg
    assert "2) 监管：" in msg
    assert "3) 产品：" in msg
    assert "代表新闻：" in msg
    assert "媒体：" in msg
    assert "链接：" in msg
    assert "method=lexicon" in msg


def test_upgrade8_p1_technical_sentence_contains_direction_trigger_and_risk() -> None:
    sentence = TelegramActions._technical_sentence(latest_close=120.0, ma=100.0, rsi=70.0)  # noqa: SLF001
    assert "偏强" in sentence
    assert "MA20=100.00" in sentence
    assert "若" in sentence
    assert "风险" in sentence


def test_upgrade9_technical_sentence_contains_key_levels_and_trigger() -> None:
    sentence = TelegramActions._technical_sentence_with_levels(  # noqa: SLF001
        latest_close=95.0,
        ma10=100.0,
        ma20=110.0,
        support=90.0,
        resistance=100.0,
        rsi=40.0,
        sample_size=30,
    )
    assert "MA10=100.00" in sentence
    assert "MA20=110.00" in sentence
    assert "支撑 L=90.00" in sentence
    assert "压力 R=100.00" in sentence
    assert "风险" in sentence


@pytest.mark.asyncio
async def test_upgrade9_card_a_degrades_when_missing_recent_ohlc_series(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-u9-carda-degrade",
            "fused_insights": {"summary": "degrade test"},
            "metrics": {
                "data_close": 200.0,
                "window_low": 180.0,
                "window_high": 220.0,
                "return_30d": 0.1,
                "technical_rsi_14": 56.0,
            },
            "news": [{"title": "news item", "source": "wire"}],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 3031201, "message": {"chat": {"id": "chat-u9a"}, "text": "分析 TSLA 一个月走势"}})
    msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)
    assert "行情数据不足（缺少近30日序列" in msg
    assert "现价区间位置：数据不足" in msg
    assert "涨跌额 N/A" not in msg


@pytest.mark.asyncio
async def test_upgrade9_news_sentiment_is_skipped_when_sample_below_threshold(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "Date": (start + timedelta(days=i)).isoformat(),
                "Open": 170.0 + i,
                "High": 171.0 + i,
                "Low": 169.0 + i,
                "Close": 170.0 + i,
            }
            for i in range(30)
        ]
        return {
            "run_id": "run-u9-news-threshold",
            "fused_insights": {"summary": "news threshold"},
            "metrics": {"data_close": 200.0, "technical_rsi_14": 56.0},
            "records": records,
            "news": [
                {"title": "Q4 earnings beat estimates", "source": "WireA", "url": "https://wirea.example/1", "published_at": "2026-02-27T08:00:00+00:00"},
                {"title": "Regulator opens probe", "source": "WireB", "url": "https://wireb.example/2", "published_at": "2026-02-27T07:00:00+00:00"},
                {"title": "New product launch", "source": "WireC", "url": "https://wirec.example/3", "published_at": "2026-02-26T09:00:00+00:00"},
                {"title": "Fed signals lower rate path", "source": "WireD", "url": "https://wired.example/4", "published_at": "2026-02-26T06:00:00+00:00"},
            ],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 3031202, "message": {"chat": {"id": "chat-u9b"}, "text": "分析 TSLA 一个月走势"}})
    msg = next(text for _, text in reversed(sender.messages) if "Card A｜区间表现" in text)
    assert "情绪：样本不足（N=4<5），不计算情绪分。" in msg


@pytest.mark.asyncio
async def test_upgrade8_p0_snapshot_buttons_collapsed_to_four_primary_actions(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p2-btn", "metrics": {"data_close": 88.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30321, "message": {"chat": {"id": "chat-p2c"}, "text": "分析 TSLA 一个月走势"}})
    inline = sender.keyboards[-1][1].get("inline_keyboard")
    assert isinstance(inline, list)
    labels = [str(button["text"]) for row in inline for button in row]
    callbacks = [str(button["callback_data"]) for row in inline for button in row]
    assert labels == ["📈K线", "📰新闻", "🔔提醒", "更多"]
    assert any(item.endswith("|chart") for item in callbacks)
    assert any(item.endswith("|news") for item in callbacks)
    assert any(item.endswith("|alert") for item in callbacks)
    assert any(item.endswith("|more") for item in callbacks)


@pytest.mark.asyncio
async def test_upgrade8_p0_more_button_keeps_legacy_followup_actions(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p0-more", "metrics": {"data_close": 88.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30325, "message": {"chat": {"id": "chat-p0-more"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30325").fetchone()
    assert row is not None and row["request_id"]
    req_id = str(row["request_id"])
    assert await gateway.process_update(
        {
            "update_id": 30326,
            "callback_query": {"id": "cb-p0-more", "data": f"act|{req_id[-6:]}|more", "message": {"chat": {"id": "chat-p0-more"}}},
        }
    )
    inline = sender.keyboards[-1][1].get("inline_keyboard")
    assert isinstance(inline, list)
    callbacks = [str(button["callback_data"]) for row in inline for button in row]
    assert any(item.endswith("|period3mo") for item in callbacks)
    assert any(item.endswith("|news_detail") for item in callbacks)
    assert any(item.endswith("|news_cluster") for item in callbacks)
    assert any(item.endswith("|retry") for item in callbacks)
    assert any(item.endswith("|report") for item in callbacks)


@pytest.mark.asyncio
async def test_phase_p2_explain_buttons_callback_reply(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p2-explain", "metrics": {"data_close": 70.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30331, "message": {"chat": {"id": "chat-p2d"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 30331").fetchone()
    assert row is not None
    req_id = str(row["request_id"])

    assert await gateway.process_update(
        {"update_id": 30332, "callback_query": {"id": "cb-p2-explain1", "data": f"act|{req_id[-6:]}|why_no_chart", "message": {"chat": {"id": "chat-p2d"}}}}
    )
    assert "为什么不给K线" in sender.messages[-1][1]
    assert "证据:" in sender.messages[-1][1]

    assert await gateway.process_update(
        {"update_id": 30333, "callback_query": {"id": "cb-p2-explain2", "data": f"act|{req_id[-6:]}|why_no_rsi", "message": {"chat": {"id": "chat-p2d"}}}}
    )
    assert "为什么不给RSI" in sender.messages[-1][1]
    assert "证据:" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_p2_news_detail_and_cluster_callbacks_render_content(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-p2-news-actions",
            "fused_insights": {"summary": "news detail and cluster"},
            "metrics": {"data_close": 120.0, "technical_rsi_14": 52.0},
            "news": [
                {"title": "Q4 earnings beat estimates", "source": "WireA", "published_at": "2026-02-27T08:00:00+00:00"},
                {"title": "Regulator opens probe into filings", "source": "WireB", "published_at": "2026-02-27T06:00:00+00:00"},
                {"title": "New product launch gains traction", "source": "WireC", "published_at": "2026-02-26T09:00:00+00:00"},
            ],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 303341, "message": {"chat": {"id": "chat-p2-news"}, "text": "分析 TSLA 一个月走势"}})
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = 303341").fetchone()
    assert row is not None
    req_id = str(row["request_id"])

    assert await gateway.process_update(
        {
            "update_id": 303342,
            "callback_query": {"id": "cb-p2-news-detail", "data": f"act|{req_id[-6:]}|news_detail", "message": {"chat": {"id": "chat-p2-news"}}},
        }
    )
    assert "新闻详单" in sender.messages[-1][1]
    assert "影响：" in sender.messages[-1][1]

    assert await gateway.process_update(
        {
            "update_id": 303343,
            "callback_query": {"id": "cb-p2-news-cluster", "data": f"act|{req_id[-6:]}|news_cluster", "message": {"chat": {"id": "chat-p2-news"}}},
        }
    )
    assert "事件聚类" in sender.messages[-1][1]
    assert "分布：" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_p2_report_full_filters_forbidden_metric_keys(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p2-report-filter", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    store.upsert_analysis_report(
        run_id="run-p2-report-filter",
        request_id="req-p2-report-filter",
        chat_id="chat-p2-report",
        symbol="TSLA",
        summary="report filter check",
        key_metrics={
            "data_close": 101.0,
            "technical_rsi_14": 54.0,
            "schema_version": "telegram_nlu_plan_v2",
            "action_version": "v2",
            "_schema_version": "telegram_nlu_plan_v2",
            "traceback": "xxx",
            "raw_error": "yyy",
            "news_digest": {
                "window_days": 7,
                "window_label": "近7天",
                "total_count": 1,
                "source_coverage": ["WireA"],
                "event_distribution": {"财报": 1, "监管": 0, "产品": 0, "宏观": 0, "其他": 0},
                "sentiment_score": 60,
                "sentiment_direction": "偏多",
                "sentiment_range": "55-65",
                "top_news": [
                    {
                        "title": "earnings beat",
                        "published_at": "2026-02-27 08:00",
                        "source": "WireA",
                        "impact": "偏利多，关注业绩兑现与预期差。",
                        "category": "财报",
                        "sentiment": "偏多",
                    }
                ],
            },
        },
    )

    assert await gateway.process_update(
        {"update_id": 303344, "message": {"chat": {"id": "chat-p2-report"}, "text": "/report run-p2-report-filter full"}}
    )
    lowered = sender.messages[-1][1].lower()
    assert "schema_version" not in lowered
    assert "action_version" not in lowered
    assert "_schema_version" not in lowered
    assert "traceback" not in lowered
    assert "raw_error" not in lowered


@pytest.mark.asyncio
async def test_phase_p2_stop_without_target_cancels_executing_snapshot(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    started = asyncio.Event()

    async def fake_runner(**kwargs):  # noqa: ANN003
        started.set()
        await asyncio.sleep(30)
        return {"run_id": "run-p2-cancel", "metrics": {"data_close": 1.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=10)
    gateway = TelegramGateway(store=store, actions=actions)

    analyze_task = asyncio.create_task(
        gateway.process_update({"update_id": 30341, "message": {"chat": {"id": "chat-p2e"}, "text": "分析 TSLA 一个月走势"}})
    )
    await asyncio.wait_for(started.wait(), timeout=1.5)
    await asyncio.sleep(0.05)

    assert await gateway.process_update({"update_id": 30342, "message": {"chat": {"id": "chat-p2e"}, "text": "/stop"}})
    assert await analyze_task

    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT request_id FROM bot_updates WHERE update_id = 30341",
        ).fetchone()
    assert row is not None and row["request_id"]
    request_id = str(row["request_id"])
    req = store.get_nl_request(request_id=request_id)
    assert req is not None
    assert req.status == "rejected"
    assert req.reject_reason == "cancelled"
    merged = "\n".join(text for _, text in sender.messages)
    assert "已发送取消信号" in merged
    assert "任务已取消" in merged


@pytest.mark.asyncio
async def test_phase_p2_conversation_history_compaction_archives_old_requests(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()
    limits = RuntimeLimits(
        session_singleflight_ttl_seconds=0,
        conversation_archive_keep_recent=2,
        conversation_archive_min_batch=2,
    )

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": f"run-{kwargs['symbol']}", "metrics": {"data_close": 1.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, limits=limits, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions, limits=limits)

    symbols = ["TSLA", "AAPL", "MSFT", "NVDA", "AMZN"]
    for index, symbol in enumerate(symbols, start=1):
        assert await gateway.process_update(
            {"update_id": 30400 + index, "message": {"chat": {"id": "chat-p2f"}, "text": f"分析 {symbol} 一个月走势"}}
        )

    archives = store.list_conversation_archives(scope_key="chat:chat-p2f", limit=5)
    assert archives
    assert archives[0].request_count >= 2
    assert int(archives[0].summary.get("request_count", 0)) >= 2
    with store._connect() as conn:  # noqa: SLF001
        archived = conn.execute(
            "SELECT COUNT(*) AS c FROM nl_requests WHERE chat_id = ? AND archived_at IS NOT NULL",
            ("chat-p2f",),
        ).fetchone()
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM nl_requests WHERE chat_id = ? AND archived_at IS NULL",
            ("chat-p2f",),
        ).fetchone()
    assert archived is not None and int(archived["c"]) >= 2
    assert active is not None and int(active["c"]) >= 2


def test_phase_p2_run_report_contains_operational_metrics(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    store.record_metric(metric_name="symbol_carry_over_hit_rate", metric_value=1.0)
    store.record_metric(metric_name="nl_clarify_asked_total", metric_value=1.0)
    store.record_metric(metric_name="chart_retry_attempted", metric_value=1.0)
    store.record_metric(metric_name="chart_retry_success", metric_value=1.0)
    store.record_metric(metric_name="analysis_response_total", metric_value=1.0)
    store.record_metric(metric_name="evidence_visible_total", metric_value=1.0)

    report = store.build_phase_d_run_report()
    assert "clarify_avoid_rate" in report
    assert "chart_success_rate_after_retry" in report
    assert "evidence_visible_rate" in report
    assert report["chart_success_rate_after_retry"] == 1.0
