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
        self.keyboards: list[tuple[str, dict[str, object]]] = []

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        self.messages.append((chat_id, text))
        if reply_markup is not None:
            self.keyboards.append((chat_id, reply_markup))
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
    with store._connect() as conn:  # noqa: SLF001
        row = conn.execute("SELECT request_id FROM bot_updates WHERE update_id = ?", (21001,)).fetchone()
    assert row is not None and row["request_id"]
    request_id = str(row["request_id"])
    assert await gateway.process_update(
        {"update_id": 21006, "callback_query": {"id": "cb-b1", "data": f"pick|{request_id}|0700.HK", "message": {"chat": {"id": "chat-b1"}}}}
    )

    latest = next(item[1] for item in reversed(sender.messages) if "Snapshot Analysis" in item[1])
    assert "Snapshot Analysis" in latest
    assert "run_id=run-b1" in latest
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

    first = {"update_id": 21003, "message": {"chat": {"id": "chat-b3"}, "text": "TSLA 一个月涨跌"}}
    second = {"update_id": 21004, "message": {"chat": {"id": "chat-b3"}, "text": "分析 TSLA 一个月走势"}}
    third = {"update_id": 21005, "message": {"chat": {"id": "chat-b3"}, "text": "TSLA 一个月涨跌"}}

    assert await gateway.process_update(first)
    assert await gateway.process_update(second)
    assert calls["n"] == 2
    assert await gateway.process_update(third)
    assert calls["n"] == 2


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
    assert "Active monitor jobs" in sender.messages[-1][1]

    assert await gateway.process_update({"update_id": 30003, "message": {"chat": {"id": "chat-dn1"}, "text": "给我每日报告"}})
    assert "Daily digest" in sender.messages[-1][1]

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
    assert "Available commands" in sender.messages[-1][1]


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
    assert "证据块 (Evidence)" in sender.messages[-1][1]
    assert "schema_version=" in sender.messages[-1][1]


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
    assert sender.keyboards
    inline = sender.keyboards[-1][1].get("inline_keyboard")
    assert isinstance(inline, list) and inline
    first_button = inline[0][0]
    assert str(first_button["callback_data"]).startswith(f"pick|{req_id[-6:]}|")


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
    assert any("时间窗=近30天" in text for _, text in sender.messages)


@pytest.mark.asyncio
async def test_phase_p1_dedupe_echoes_ready_state_and_run_id(tmp_path) -> None:  # noqa: ANN001
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
    assert "30秒内重复请求已合并：已生成。" in sender.messages[-1][1]
    assert "run_id=run-p1-dedupe-1" in sender.messages[-1][1]


@pytest.mark.asyncio
async def test_phase_p2_user_copy_forbidden_words_guard(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-p2-guard",
            "fused_insights": {"summary": "metrics unavailable traceback chart_missing"},
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


@pytest.mark.asyncio
async def test_phase_p2_snapshot_output_order_and_evidence_visible(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {
            "run_id": "run-p2-order",
            "fused_insights": {"summary": "fixed order"},
            "metrics": {
                "data_close": 200.0,
                "technical_rsi_14": 56.0,
                "window_low": 180.0,
                "window_high": 220.0,
                "data_window": "2026-01-28 ~ 2026-02-27",
            },
            "news": [{"title": "news", "source": "wire"}],
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30311, "message": {"chat": {"id": "chat-p2b"}, "text": "分析 TSLA 一个月走势"}})
    msg = sender.messages[-1][1]
    idx_symbol = msg.find("标的区间:")
    idx_price = msg.find("价格摘要:")
    idx_tech = msg.find("技术一句话:")
    idx_news = msg.find("新闻一句话:")
    idx_risk = msg.find("风险:")
    idx_menu = msg.find("菜单:")
    assert -1 not in {idx_symbol, idx_price, idx_tech, idx_news, idx_risk, idx_menu}
    assert idx_symbol < idx_price < idx_tech < idx_news < idx_risk < idx_menu
    assert "证据:" in msg


@pytest.mark.asyncio
async def test_phase_p2_buttons_include_followup_and_explanations(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeChatSender()

    async def fake_runner(**kwargs):  # noqa: ANN003
        return {"run_id": "run-p2-btn", "metrics": {"data_close": 88.0}, **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    assert await gateway.process_update({"update_id": 30321, "message": {"chat": {"id": "chat-p2c"}, "text": "分析 TSLA 一个月走势"}})
    inline = sender.keyboards[-1][1].get("inline_keyboard")
    assert isinstance(inline, list)
    callbacks = [str(button["callback_data"]) for row in inline for button in row]
    assert any(item.endswith("|period3mo") for item in callbacks)
    assert any(item.endswith("|news_only") for item in callbacks)
    assert any(item.endswith("|set_monitor") for item in callbacks)
    assert any(item.endswith("|why_no_chart") for item in callbacks)
    assert any(item.endswith("|why_no_rsi") for item in callbacks)


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
