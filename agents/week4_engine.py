"""Week4 realtime watchlist scan + priority alert dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

import pandas as pd

from tools.market_data import MarketDataResult, fetch_market_data
from tools.telegram import send_text


@dataclass
class ScanConfig:
    watchlist: list[str]
    period: str = "5d"
    pct_alert_threshold: float = 0.03
    rsi_overbought: float = 70.0
    rsi_oversold: float = 30.0


@dataclass
class WatchSignal:
    symbol: str
    timestamp: datetime
    price: float
    pct_change: float
    rsi: float
    priority: str  # critical | high | normal
    reason: str


def _compute_rsi(close: pd.Series, period: int = 14) -> float:
    if close.empty:
        return 50.0
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.rolling(period).mean()
    roll_down = down.rolling(period).mean().replace(0, pd.NA)
    rs = roll_up / roll_down
    rsi = (100 - (100 / (1 + rs))).fillna(50)
    return float(rsi.iloc[-1])


def _classify_priority(pct_change: float, threshold: float, rsi: float, overbought: float, oversold: float) -> tuple[str, str]:
    abs_change = abs(pct_change)
    rsi_extreme = rsi >= overbought or rsi <= oversold

    if abs_change >= threshold * 2:
        return "critical", "price_move"
    if abs_change >= threshold or rsi_extreme:
        return "high", "price_or_rsi"
    return "normal", "digest"


def format_signal_message(signal: WatchSignal) -> str:
    arrow = "📈" if signal.pct_change >= 0 else "📉"
    pct = round(signal.pct_change * 100, 2)
    return (
        f"[{signal.priority.upper()}] {signal.symbol} {arrow}\n"
        f"price={signal.price:.2f}, change={pct}%\n"
        f"rsi={signal.rsi:.2f}, reason={signal.reason}\n"
        f"time={signal.timestamp.isoformat()}"
    )


async def analyze_symbol(
    symbol: str,
    config: ScanConfig,
    *,
    fetcher: Callable[[str, str], Awaitable[MarketDataResult]] = fetch_market_data,
) -> WatchSignal | None:
    result = await fetcher(symbol, config.period)
    if not result.ok or not result.records:
        return None

    df = pd.DataFrame(result.records)
    if "Close" not in df.columns or len(df) < 2:
        return None

    close = pd.to_numeric(df["Close"], errors="coerce").ffill().fillna(0)
    last = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) >= 2 else last
    pct_change = 0.0 if prev == 0 else (last - prev) / prev
    rsi = _compute_rsi(close)

    priority, reason = _classify_priority(
        pct_change,
        config.pct_alert_threshold,
        rsi,
        config.rsi_overbought,
        config.rsi_oversold,
    )

    return WatchSignal(
        symbol=symbol,
        timestamp=datetime.now(timezone.utc),
        price=last,
        pct_change=pct_change,
        rsi=rsi,
        priority=priority,
        reason=reason,
    )


async def scan_watchlist(
    config: ScanConfig,
    *,
    fetcher: Callable[[str, str], Awaitable[MarketDataResult]] = fetch_market_data,
) -> list[WatchSignal]:
    tasks = [analyze_symbol(symbol, config, fetcher=fetcher) for symbol in config.watchlist]
    results = await asyncio.gather(*tasks)
    signals = [item for item in results if item is not None]
    order = {"critical": 0, "high": 1, "normal": 2}
    return sorted(signals, key=lambda x: order.get(x.priority, 9))


def select_alerts_for_mode(signals: list[WatchSignal], mode: str) -> list[WatchSignal]:
    mode = mode.lower().strip()
    if mode == "anomaly":
        return [sig for sig in signals if sig.priority in {"critical", "high"}]
    return signals


async def dispatch_telegram_alerts(
    signals: list[WatchSignal],
    *,
    bot_token: str,
    chat_id: str,
    mode: str = "anomaly",
) -> list[dict]:
    selected = select_alerts_for_mode(signals, mode)
    responses: list[dict] = []
    for signal in selected:
        text = format_signal_message(signal)
        response = await send_text(bot_token, chat_id, text)
        responses.append(response)
    return responses
