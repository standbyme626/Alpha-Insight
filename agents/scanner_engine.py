"""Week4 realtime watchlist scan + priority alert dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

import pandas as pd

from tools.market_data import MarketDataResult, fetch_market_data, get_company_name, normalize_market_symbol
from tools.telegram import send_text


@dataclass
class ScanConfig:
    watchlist: list[str]
    market: str = "auto"  # auto | us | hk | cn
    period: str = "5d"
    interval: str = "1d"  # 1d | 60m | 5m
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
    company_name: str = ""


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
    company = (signal.company_name or "").strip()
    normalized_symbol = normalize_market_symbol(signal.symbol, market="auto")
    normalized_hk_symbol = normalize_market_symbol(signal.symbol, market="hk")
    if company and company not in {signal.symbol, normalized_symbol, normalized_hk_symbol}:
        display = f"{signal.symbol} | {company}"
    else:
        display = signal.symbol
    return (
        f"[{signal.priority.upper()}] {display} {arrow}\n"
        f"price/价格={signal.price:.2f}, change/涨跌幅={pct}%\n"
        f"rsi/相对强弱={signal.rsi:.2f}, reason/原因={signal.reason}\n"
        f"time/时间={signal.timestamp.isoformat()}"
    )


async def analyze_symbol(
    symbol: str,
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
) -> WatchSignal | None:
    query_symbol = normalize_market_symbol(symbol, market=config.market)
    try:
        result = await fetcher(query_symbol, config.period, config.interval)
    except TypeError:
        # Backward compatibility for older test doubles accepting only (symbol, period).
        result = await fetcher(query_symbol, config.period)
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
        symbol=symbol.strip().upper() if symbol.strip() else query_symbol,
        timestamp=datetime.now(timezone.utc),
        price=last,
        pct_change=pct_change,
        rsi=rsi,
        priority=priority,
        reason=reason,
        company_name=get_company_name(query_symbol, resolve_remote=False),
    )


async def scan_watchlist(
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
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
        if not signal.company_name or signal.company_name == signal.symbol:
            signal.company_name = await asyncio.to_thread(get_company_name, signal.symbol, True)
        text = format_signal_message(signal)
        response = await send_text(bot_token, chat_id, text)
        responses.append(response)
    return responses
