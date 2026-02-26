from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from agents.scanner_engine import (
    ScanConfig,
    WatchSignal,
    format_signal_message,
    scan_watchlist,
    select_alerts_for_mode,
)
from core.guardrails import GuardrailError, validate_sandbox_code
from core.observability import QuantTelemetry
from tools.market_data import MarketDataResult
from ui.streamlit_dashboard import build_watchlist_figure


@pytest.mark.asyncio
async def test_scan_watchlist_priority_sorting() -> None:
    async def fake_fetch(symbol: str, period: str) -> MarketDataResult:
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
        {"symbol": "AAPL", "pct_change": 0.03, "rsi": 55.0, "priority": "high"},
        {"symbol": "MSFT", "pct_change": -0.01, "rsi": 45.0, "priority": "normal"},
    ]
    fig = build_watchlist_figure(rows)
    assert len(fig.data) == 1


def test_format_signal_message_contains_priority() -> None:
    signal = WatchSignal(
        symbol="AAPL",
        timestamp=datetime.now(timezone.utc),
        price=188.5,
        pct_change=0.031,
        rsi=61.2,
        priority="high",
        reason="price_or_rsi",
    )
    text = format_signal_message(signal)
    assert "[HIGH]" in text
    assert "AAPL" in text
