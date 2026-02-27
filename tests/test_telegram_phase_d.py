from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

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
        self.photos: list[tuple[str, str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}

    async def send_photo(self, chat_id: str, image_path: str, caption: str = "") -> dict[str, object]:
        self.photos.append((chat_id, image_path, caption))
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

    latest = sender.messages[-1][1]
    assert "Snapshot Analysis" in latest
    assert "run_id=run-b1" in latest
    assert sender.photos
    assert sender.photos[-1][0] == "chat-b1"
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = ?", (21001,)).fetchone()
    assert row is not None and row["request_id"]
    req = store.get_nl_request(request_id=str(row["request_id"]))
    assert req is not None
    assert req.intent == "analyze_snapshot"
    assert req.slots["symbol"] == "0700.HK"


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
    assert sender.messages
    assert sender.photos == []
    fail_metrics = store.metric_values(metric_name="chart_render_fail_rate")
    assert fail_metrics


@pytest.mark.asyncio
async def test_phase_b_analyze_snapshot_dedupe_requires_double_key_hit(tmp_path) -> None:  # noqa: ANN001
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

    first = {"update_id": 21003, "message": {"chat": {"id": "chat-b3"}, "text": "腾讯一个月涨跌"}}
    second = {"update_id": 21004, "message": {"chat": {"id": "chat-b3"}, "text": "分析腾讯一个月走势"}}
    third = {"update_id": 21005, "message": {"chat": {"id": "chat-b3"}, "text": "腾讯一个月涨跌"}}

    assert await gateway.process_update(first)
    assert await gateway.process_update(second)
    assert calls["n"] == 2
    assert await gateway.process_update(third)
    assert calls["n"] == 2
