from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from agents.scanner_engine import (
    AlertSnapshotStore,
    ScanConfig,
    WatchSignal,
    build_scan_trigger,
    dispatch_alert_notifications,
    format_signal_message,
    run_watchlist_cycle,
    scan_watchlist,
    select_alerts_for_mode,
)
from core.guardrails import GuardrailError, validate_sandbox_code
from core.models import AlertSnapshot
from core.observability import QuantTelemetry
from tools.market_data import MarketDataResult
from ui.streamlit_dashboard import build_watchlist_figure


@pytest.mark.asyncio
async def test_scan_watchlist_priority_sorting() -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        base = 100.0
        if symbol == "AAPL":
            closes = [100, 106]  # +6%
        elif symbol == "MSFT":
            closes = [100, 103]  # +3%
        else:
            closes = [100, 100.5]  # +0.5%
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Close": closes,
                "Open": [base, base],
                "High": [base, base],
                "Low": [base, base],
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=df.to_dict(orient="records"))

    config = ScanConfig(watchlist=["TSLA", "MSFT", "AAPL"], pct_alert_threshold=0.03)
    out = await scan_watchlist(config, fetcher=fake_fetch)
    assert [item.symbol for item in out] == ["AAPL", "MSFT", "TSLA"]
    assert out[0].priority == "critical"


def test_select_alerts_for_mode() -> None:
    now = datetime.now(timezone.utc)
    signals = [
        WatchSignal("AAPL", now, 100.0, 0.06, 50.0, "critical", "price_move"),
        WatchSignal("MSFT", now, 100.0, 0.03, 50.0, "high", "price_or_rsi"),
        WatchSignal("TSLA", now, 100.0, 0.005, 50.0, "normal", "digest"),
    ]
    anomaly = select_alerts_for_mode(signals, "anomaly")
    digest = select_alerts_for_mode(signals, "digest")
    assert len(anomaly) == 2
    assert len(digest) == 3


def test_guardrails_block_forbidden_calls() -> None:
    with pytest.raises(GuardrailError):
        validate_sandbox_code("import subprocess\nsubprocess.run(['ls'])")


def test_guardrails_allow_safe_code() -> None:
    validate_sandbox_code("import pandas as pd\nprint('ok')")


@pytest.mark.parametrize(
    "code",
    [
        "import requests\nrequests.get('https://example.com')",
        "import yfinance as yf\nyf.Ticker('AAPL').history(period='1mo')",
        "import os\nos.system('pip install numpy')",
        "python -m pip install pandas",
    ],
)
def test_guardrails_block_network_or_dynamic_install(code: str) -> None:
    with pytest.raises(GuardrailError):
        validate_sandbox_code(code)


def test_telemetry_span_and_token_usage() -> None:
    telemetry = QuantTelemetry()
    with telemetry.span("week4.scan"):
        _ = 1 + 1
    telemetry.add_token_usage(prompt_tokens=12, completion_tokens=8)
    events = telemetry.flush()
    assert len(events) == 2
    assert events[0].name == "week4.scan"
    assert events[0].status == "ok"


def test_build_watchlist_figure() -> None:
    rows = [
        {"symbol": "AAPL", "company_name": "苹果", "pct_change": 0.03, "rsi": 55.0, "priority": "high"},
        {"symbol": "MSFT", "company_name": "微软", "pct_change": -0.01, "rsi": 45.0, "priority": "normal"},
    ]
    fig = build_watchlist_figure(rows)
    assert len(fig.data) == 1
    assert fig.data[0]["x"][0] == "AAPL(苹果)"


def test_format_signal_message_contains_priority() -> None:
    signal = WatchSignal(
        symbol="AAPL",
        timestamp=datetime.now(timezone.utc),
        price=188.5,
        pct_change=0.031,
        rsi=61.2,
        priority="high",
        reason="price_or_rsi",
        company_name="Apple Inc.",
    )
    text = format_signal_message(signal)
    assert "[HIGH]" in text
    assert "AAPL" in text
    assert "Apple Inc." in text
    assert "price/价格=" in text
    assert "change/涨跌幅=" in text
    assert "reason/原因=" in text
    assert "time/时间=" in text


def test_format_signal_message_without_name_duplication() -> None:
    signal = WatchSignal(
        symbol="002916.SZ",
        timestamp=datetime.now(timezone.utc),
        price=100.0,
        pct_change=0.01,
        rsi=50.0,
        priority="high",
        reason="price_or_rsi",
        company_name="002916.SZ",
    )
    text = format_signal_message(signal)
    assert "002916.SZ |" not in text


def test_format_signal_message_without_name_duplication_for_hk() -> None:
    signal = WatchSignal(
        symbol="0881",
        timestamp=datetime.now(timezone.utc),
        price=100.0,
        pct_change=0.01,
        rsi=50.0,
        priority="high",
        reason="price_or_rsi",
        company_name="0881.HK",
    )
    text = format_signal_message(signal)
    assert "0881 |" not in text


def test_build_scan_trigger_supports_scheduled_and_event() -> None:
    scheduled = build_scan_trigger(trigger_type="scheduled")
    event = build_scan_trigger(trigger_type="event", trigger_id="event-news-01", metadata={"source": "breaking_news"})
    assert scheduled.trigger_type == "scheduled"
    assert scheduled.trigger_id.startswith("sched-")
    assert event.trigger_type == "event"
    assert event.trigger_id == "event-news-01"
    assert event.metadata["source"] == "breaking_news"


def test_alert_snapshot_store_persist_and_load(tmp_path) -> None:  # noqa: ANN001
    store = AlertSnapshotStore(tmp_path / "alerts.jsonl")
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id="sched-test")
    now = datetime.now(timezone.utc)
    snapshots = [
        {
            "snapshot_id": "alert-1",
            "trigger_type": trigger.trigger_type,
            "trigger_id": trigger.trigger_id,
            "trigger_time": trigger.trigger_time,
            "mode": "anomaly",
            "signal": {
                "symbol": "AAPL",
                "company_name": "Apple Inc.",
                "timestamp": now,
                "price": 100.0,
                "pct_change": 0.06,
                "rsi": 58.0,
                "priority": "critical",
                "reason": "price_move",
            },
        },
        {
            "snapshot_id": "alert-2",
            "trigger_type": trigger.trigger_type,
            "trigger_id": trigger.trigger_id,
            "trigger_time": trigger.trigger_time,
            "mode": "anomaly",
            "signal": {
                "symbol": "MSFT",
                "company_name": "Microsoft",
                "timestamp": now,
                "price": 50.0,
                "pct_change": 0.03,
                "rsi": 56.0,
                "priority": "high",
                "reason": "price_or_rsi",
            },
        },
    ]
    parsed = [AlertSnapshot.model_validate(item) for item in snapshots]
    assert store.persist(parsed) == 2
    restored = store.load_recent(limit=10)
    assert len(restored) == 2
    assert restored[0].signal.symbol == "AAPL"
    assert restored[1].signal.priority == "high"


@pytest.mark.asyncio
async def test_run_watchlist_cycle_triggers_research_and_persists_snapshot(tmp_path) -> None:  # noqa: ANN001
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        closes = [100, 107] if symbol == "AAPL" else [100, 103]
        df = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Close": closes,
                "Open": [100, 100],
                "High": [101, 108],
                "Low": [99, 99],
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=df.to_dict(orient="records"))

    class FakeNotifier:
        channel_name = "fake"

        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send_text(self, text: str) -> dict[str, object]:
            self.messages.append(text)
            return {"ok": True, "text": text}

    def _fake_research_result(run_id: str, symbol: str) -> dict:
        return {
            "run_id": run_id,
            "request": f"auto {symbol}",
            "symbol": symbol,
            "period": "5d",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "plan": {
                "provider": "test",
                "data_source": "api",
                "steps": ["fetch", "analyze"],
                "reason": "critical alert",
            },
            "data_bundle_ref": {
                "data_source": "api",
                "asof": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "market": "auto",
                "interval": "1d",
                "record_count": 2,
            },
            "sandbox_artifacts": {
                "code": "print('ok')",
                "stdout": "ok",
                "stderr": "",
                "backend": "docker",
                "retry_count": 0,
                "success": True,
                "traceback": None,
            },
            "fused_insights": {
                "summary": "ok",
                "analysis_steps": ["step1"],
                "raw": {"latest_close": 107.0},
            },
            "metrics": {"latest_close": 107.0},
            "provenance": [
                {
                    "metric": "latest_close",
                    "value": 107.0,
                    "source": "fused_metrics",
                    "pointer": "fused_insights.raw.latest_close",
                    "note": "",
                }
            ],
        }

    call_args: dict[str, object] = {}

    async def fake_research_runner(**kwargs):  # noqa: ANN003
        call_args.update(kwargs)
        symbol = str(kwargs["symbol"])
        return _fake_research_result("run-auto-critical-1", symbol)

    cfg = ScanConfig(watchlist=["AAPL", "MSFT"], pct_alert_threshold=0.03)
    trigger = build_scan_trigger(trigger_type="event", trigger_id="event-001", metadata={"source": "earnings"})
    store = AlertSnapshotStore(tmp_path / "snapshots.jsonl")
    notifier = FakeNotifier()
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=fake_fetch,
        notifier=notifier,
        snapshot_store=store,
        research_runner=fake_research_runner,
    )

    assert len(result.signals) == 2
    assert len(result.selected_alerts) == 2
    critical = next(item for item in result.snapshots if item.signal.symbol == "AAPL")
    assert critical.research_status == "triggered"
    assert critical.research_run_id == "run-auto-critical-1"
    assert critical.research_result is not None
    assert critical.research_result.run_id == "run-auto-critical-1"
    assert call_args["symbol"] == "AAPL"
    persisted = store.load_recent(limit=10)
    assert len(persisted) == 2
    assert any(item.notification_dispatched for item in persisted)
    assert len(result.notifications) == 2
    assert len(notifier.messages) == 2
    assert "research_run_id=run-auto-critical-1" in notifier.messages[0]


@pytest.mark.asyncio
async def test_dispatch_alert_notifications_uses_channel_abstraction() -> None:
    now = datetime.now(timezone.utc)
    signals = [WatchSignal("AAPL", now, 100.0, 0.06, 55.0, "critical", "price_move", "Apple Inc.")]

    class FakeNotifier:
        channel_name = "fake"

        def __init__(self) -> None:
            self.messages: list[str] = []

        async def send_text(self, text: str) -> dict[str, object]:
            self.messages.append(text)
            return {"ok": True}

    notifier = FakeNotifier()
    responses = await dispatch_alert_notifications(signals, notifier=notifier, mode="anomaly")
    assert len(responses) == 1
    assert responses[0]["channel"] == "fake"
    assert len(notifier.messages) == 1
