"""Week4 realtime watchlist scan + priority alert dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from uuid import uuid4

import pandas as pd

from core.models import AlertSignalSnapshot, AlertSnapshot, ResearchResult
from tools.market_data import (
    MarketDataResult,
    fetch_market_data,
    get_company_name,
    get_company_names_batch,
    normalize_market_symbol,
)
from tools.telegram import NotificationChannel, NotificationMessage, TelegramNotifier, dispatch_notifications


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


@dataclass
class ScanTrigger:
    trigger_type: Literal["scheduled", "event"] = "scheduled"
    trigger_id: str = ""
    trigger_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WatchlistRunResult:
    trigger: ScanTrigger
    signals: list[WatchSignal]
    selected_alerts: list[WatchSignal]
    snapshots: list[AlertSnapshot]
    notifications: list[dict[str, Any]]


ResearchRunner = Callable[..., Awaitable[dict[str, Any] | ResearchResult]]


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


def build_scan_trigger(
    *,
    trigger_type: Literal["scheduled", "event"] = "scheduled",
    trigger_id: str = "",
    trigger_time: datetime | None = None,
    metadata: dict[str, Any] | None = None,
) -> ScanTrigger:
    timestamp = trigger_time or datetime.now(timezone.utc)
    normalized_id = trigger_id.strip()
    if not normalized_id:
        prefix = "sched" if trigger_type == "scheduled" else "event"
        normalized_id = f"{prefix}-{timestamp.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:6]}"
    return ScanTrigger(
        trigger_type=trigger_type,
        trigger_id=normalized_id,
        trigger_time=timestamp,
        metadata=dict(metadata or {}),
    )


def _default_snapshot_path() -> Path:
    return Path("artifacts/alerts/watchlist_alert_snapshots.jsonl")


class AlertSnapshotStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path is not None else _default_snapshot_path()

    def persist(self, snapshots: list[AlertSnapshot]) -> int:
        if not snapshots:
            return 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            for snapshot in snapshots:
                fh.write(snapshot.model_dump_json())
                fh.write("\n")
        return len(snapshots)

    def load_recent(self, limit: int = 100) -> list[AlertSnapshot]:
        if not self.path.exists():
            return []
        if limit <= 0:
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        selected = lines[-max(int(limit), 0) :]
        snapshots: list[AlertSnapshot] = []
        for line in selected:
            line = line.strip()
            if not line:
                continue
            snapshots.append(AlertSnapshot.model_validate_json(line))
        return snapshots


def _signal_to_snapshot(signal: WatchSignal) -> AlertSignalSnapshot:
    return AlertSignalSnapshot(
        symbol=signal.symbol,
        company_name=signal.company_name,
        timestamp=signal.timestamp,
        price=signal.price,
        pct_change=signal.pct_change,
        rsi=signal.rsi,
        priority=signal.priority,
        reason=signal.reason,
    )


def _build_triggered_request(signal: WatchSignal, trigger: ScanTrigger) -> str:
    move_pct = round(signal.pct_change * 100, 2)
    return (
        f"[auto-trigger:{trigger.trigger_id}] Critical alert for {signal.symbol}. "
        f"price={signal.price:.2f}, change={move_pct}%, rsi={signal.rsi:.2f}, reason={signal.reason}. "
        "Generate a unified research report."
    )


async def _run_unified_research_default(**kwargs) -> dict[str, Any]:
    from agents.workflow_engine import run_unified_research

    return await run_unified_research(**kwargs)


def _snapshot_lookup_by_symbol(snapshots: list[AlertSnapshot]) -> dict[str, AlertSnapshot]:
    lookup: dict[str, AlertSnapshot] = {}
    for snapshot in snapshots:
        lookup[snapshot.signal.symbol] = snapshot
    return lookup


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
    if signals:
        try:
            name_map = get_company_names_batch([sig.symbol for sig in signals], market=config.market, resolve_remote=False)
            for sig in signals:
                normalized = normalize_market_symbol(sig.symbol, market=config.market)
                resolved = name_map.get(normalized) or name_map.get(sig.symbol, "")
                if resolved:
                    sig.company_name = resolved
        except Exception:
            pass
    order = {"critical": 0, "high": 1, "normal": 2}
    return sorted(signals, key=lambda x: order.get(x.priority, 9))


def select_alerts_for_mode(signals: list[WatchSignal], mode: str) -> list[WatchSignal]:
    mode = mode.lower().strip()
    if mode == "anomaly":
        return [sig for sig in signals if sig.priority in {"critical", "high"}]
    return signals


async def dispatch_alert_notifications(
    signals: list[WatchSignal],
    *,
    notifier: NotificationChannel,
    mode: str = "anomaly",
    snapshots: list[AlertSnapshot] | None = None,
) -> list[dict[str, Any]]:
    selected = select_alerts_for_mode(signals, mode)
    snapshot_lookup = _snapshot_lookup_by_symbol(snapshots or [])
    messages: list[NotificationMessage] = []
    for signal in selected:
        if not signal.company_name or signal.company_name == signal.symbol:
            signal.company_name = await asyncio.to_thread(get_company_name, signal.symbol, True)
        text = format_signal_message(signal)
        related_snapshot = snapshot_lookup.get(signal.symbol)
        if related_snapshot and related_snapshot.research_run_id:
            text += f"\nresearch_run_id={related_snapshot.research_run_id}"
        messages.append(
            NotificationMessage(
                text=text,
                metadata={
                    "symbol": signal.symbol,
                    "priority": signal.priority,
                    "triggered_research": bool(related_snapshot and related_snapshot.research_run_id),
                },
            )
        )
    return await dispatch_notifications(messages, notifier=notifier)


async def run_watchlist_cycle(
    config: ScanConfig,
    *,
    trigger: ScanTrigger,
    mode: str = "anomaly",
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    notifier: NotificationChannel | None = None,
    snapshot_store: AlertSnapshotStore | None = None,
    enable_triggered_research: bool = True,
    research_runner: ResearchRunner | None = None,
) -> WatchlistRunResult:
    signals = await scan_watchlist(config, fetcher=fetcher)
    selected = select_alerts_for_mode(signals, mode)
    snapshots: list[AlertSnapshot] = []
    runner = research_runner or _run_unified_research_default

    for signal in selected:
        snapshot = AlertSnapshot(
            snapshot_id=f"alert-{uuid4().hex[:12]}",
            trigger_type=trigger.trigger_type,
            trigger_id=trigger.trigger_id,
            trigger_time=trigger.trigger_time,
            trigger_metadata=dict(trigger.metadata),
            mode=mode,
            signal=_signal_to_snapshot(signal),
            notification_channels=[notifier.channel_name] if notifier is not None else [],
        )
        if enable_triggered_research and signal.priority == "critical":
            try:
                payload = await runner(
                    request=_build_triggered_request(signal, trigger),
                    symbol=signal.symbol,
                    period=config.period,
                    interval=config.interval,
                )
                research = payload if isinstance(payload, ResearchResult) else ResearchResult.model_validate(payload)
                snapshot.research_status = "triggered"
                snapshot.research_run_id = research.run_id
                snapshot.research_result = research
            except Exception as exc:  # pragma: no cover - defensive branch for runtime.
                snapshot.research_status = "failed"
                snapshot.research_error = str(exc)
        snapshots.append(snapshot)

    notifications: list[dict[str, Any]] = []
    if notifier is not None:
        notifications = await dispatch_alert_notifications(
            signals,
            notifier=notifier,
            mode=mode,
            snapshots=snapshots,
        )
        notified_symbols = {str(item.get("metadata", {}).get("symbol", "")) for item in notifications}
        for snapshot in snapshots:
            if snapshot.signal.symbol in notified_symbols:
                snapshot.notification_dispatched = True

    if snapshot_store is not None:
        snapshot_store.persist(snapshots)

    return WatchlistRunResult(
        trigger=trigger,
        signals=signals,
        selected_alerts=selected,
        snapshots=snapshots,
        notifications=notifications,
    )


async def dispatch_telegram_alerts(
    signals: list[WatchSignal],
    *,
    bot_token: str,
    chat_id: str,
    mode: str = "anomaly",
) -> list[dict[str, Any]]:
    notifier = TelegramNotifier(bot_token, chat_id)
    return await dispatch_alert_notifications(signals, notifier=notifier, mode=mode)
