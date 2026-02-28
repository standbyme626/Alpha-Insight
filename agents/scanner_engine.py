"""Week4 realtime watchlist scan + priority alert dispatch."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal
from uuid import uuid4

import pandas as pd

from core.models import AlertSignalSnapshot, AlertSnapshot, ResearchResult
from core.observability import (
    FailureEvent,
    aggregate_failure_clusters,
    aggregate_failure_tags,
    classify_failure,
    evaluate_threshold_alarms,
)
from core.fault_injection import FaultInjector, fault_semantic, resolve_fault_injection
from core.reliability_budget import evaluate_latency_error_budget
from core.runtime_config import ResolvedRuntimeConfig, resolve_runtime_config
from core.strategy_tier import resolve_strategy_tier
from core.strategy_plugins import PluginContext, PluginKind, StrategyPluginManager
from core.tool_result import build_tool_result
from tools.market_data import (
    MarketDataResult,
    fetch_market_data,
    get_company_name,
    get_company_names_batch,
    market_data_result_to_tool_result,
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
    fallback_spike_rate: float = 0.25
    failure_spike_count: int = 3
    latency_anomaly_ms: float = 2500.0


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
    runtime_metrics: dict[str, Any] = field(default_factory=dict)
    failure_events: list[dict[str, Any]] = field(default_factory=list)
    failure_clusters: dict[str, int] = field(default_factory=dict)
    alarms: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class SymbolAnalysisResult:
    signal: WatchSignal | None
    failure: FailureEvent | None = None
    used_fallback: bool = False
    market_tool_result: dict[str, Any] | None = None
    latency_ms: float = 0.0
    retry_count: int = 0
    fault_events: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ScanDiagnostics:
    signals: list[WatchSignal]
    failures: list[FailureEvent]
    fallback_count: int
    scan_latency_ms: float
    symbol_latency_ms: list[float] = field(default_factory=list)
    retry_count: int = 0
    fault_events: list[dict[str, Any]] = field(default_factory=list)


ResearchRunner = Callable[..., Awaitable[dict[str, Any] | ResearchResult]]


@lru_cache(maxsize=1)
def _resolve_runtime_config_cached() -> ResolvedRuntimeConfig:
    return resolve_runtime_config()


def _resolve_runtime(runtime_flags: dict[str, Any] | None) -> ResolvedRuntimeConfig:
    if runtime_flags:
        return resolve_runtime_config(runtime_flags_layer=runtime_flags)
    return _resolve_runtime_config_cached()


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


async def _analyze_symbol_with_diagnostics(
    symbol: str,
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    fault_injector: FaultInjector | None = None,
) -> SymbolAnalysisResult:
    started_at = time.perf_counter()
    query_symbol = normalize_market_symbol(symbol, market=config.market)
    used_fallback = False
    retry_count = 0
    injector = fault_injector if fault_injector is not None else FaultInjector.disabled()
    fault_events: list[dict[str, Any]] = []

    fetch_fault = injector.maybe_inject(
        node="scanner.fetch",
        allowed_faults=("timeout", "upstream_5xx", "rate_limit"),
    )
    if fetch_fault is not None:
        fault_events.append(fetch_fault.to_dict())
        semantic = fault_semantic(fetch_fault.fault, node="scanner.fetch")
        message = semantic.message
        retry_count = max(retry_count, semantic.default_retry_delta)
        latency_ms = (time.perf_counter() - started_at) * 1000
        return SymbolAnalysisResult(
            signal=None,
            failure=classify_failure(
                source="scanner.fetch",
                error_type=semantic.error_type,
                message=message,
                backend="fault_injection",
            ),
            used_fallback=False,
            market_tool_result=build_tool_result(
                source="market_data:fault_injection",
                confidence=0.0,
                raw={"symbol": query_symbol},
                error=message,
                meta={"fault_injection": fetch_fault.to_dict(), "period": config.period, "interval": config.interval},
            ).to_dict(),
            latency_ms=latency_ms,
            retry_count=retry_count,
            fault_events=fault_events,
        )

    try:
        result = await fetcher(query_symbol, config.period, config.interval)
    except TypeError:
        # Backward compatibility for older test doubles accepting only (symbol, period).
        used_fallback = True
        retry_count += 1
        try:
            result = await fetcher(query_symbol, config.period)
        except Exception as exc:
            tool_result = build_tool_result(
                source="market_data:legacy_fetcher",
                confidence=0.0,
                raw={"symbol": query_symbol},
                error=str(exc),
                meta={"period": config.period, "interval": config.interval},
            )
            return SymbolAnalysisResult(
                signal=None,
                failure=classify_failure(
                    source="scanner.fetch",
                    error_type=exc.__class__.__name__,
                    message=str(exc),
                    backend="legacy_fetcher_fallback",
                ),
                used_fallback=True,
                market_tool_result=tool_result.to_dict(),
                latency_ms=(time.perf_counter() - started_at) * 1000,
                retry_count=retry_count,
                fault_events=fault_events,
            )
    except Exception as exc:
        tool_result = build_tool_result(
            source="market_data:fetcher",
            confidence=0.0,
            raw={"symbol": query_symbol},
            error=str(exc),
            meta={"period": config.period, "interval": config.interval},
        )
        return SymbolAnalysisResult(
            signal=None,
            failure=classify_failure(
                source="scanner.fetch",
                error_type=exc.__class__.__name__,
                message=str(exc),
            ),
            used_fallback=False,
            market_tool_result=tool_result.to_dict(),
            latency_ms=(time.perf_counter() - started_at) * 1000,
            retry_count=retry_count,
            fault_events=fault_events,
        )

    tool_result = market_data_result_to_tool_result(
        result,
        period=config.period,
        interval=config.interval,
    )
    tool_error_message = tool_result.error or result.message
    if not result.ok or not result.records:
        return SymbolAnalysisResult(
            signal=None,
            failure=classify_failure(
                source="scanner.fetch",
                error_type="DataFetchError",
                message=tool_error_message,
            ),
            used_fallback=used_fallback,
            market_tool_result=tool_result.to_dict(),
            latency_ms=(time.perf_counter() - started_at) * 1000,
            retry_count=retry_count,
            fault_events=fault_events,
        )

    parse_fault = injector.maybe_inject(
        node="scanner.parse",
        allowed_faults=("parse",),
    )
    if parse_fault is not None:
        fault_events.append(parse_fault.to_dict())
        semantic = fault_semantic(parse_fault.fault, node="scanner.parse")
        message = semantic.message
        return SymbolAnalysisResult(
            signal=None,
            failure=classify_failure(
                source="scanner.parse",
                error_type=semantic.error_type,
                message=message,
                backend="fault_injection",
            ),
            used_fallback=used_fallback,
            market_tool_result=tool_result.to_dict(),
            latency_ms=(time.perf_counter() - started_at) * 1000,
            retry_count=retry_count,
            fault_events=fault_events,
        )

    df = pd.DataFrame(result.records)
    if "Close" not in df.columns or len(df) < 2:
        return SymbolAnalysisResult(
            signal=None,
            failure=classify_failure(
                source="scanner.parse",
                error_type="DataShapeError",
                message="invalid Close column or insufficient rows",
            ),
            used_fallback=used_fallback,
            market_tool_result=tool_result.to_dict(),
            latency_ms=(time.perf_counter() - started_at) * 1000,
            retry_count=retry_count,
            fault_events=fault_events,
        )

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

    return SymbolAnalysisResult(
        signal=WatchSignal(
            symbol=symbol.strip().upper() if symbol.strip() else query_symbol,
            timestamp=datetime.now(timezone.utc),
            price=last,
            pct_change=pct_change,
            rsi=rsi,
            priority=priority,
            reason=reason,
            company_name=get_company_name(query_symbol, resolve_remote=False),
        ),
        failure=None,
        used_fallback=used_fallback,
        market_tool_result=tool_result.to_dict(),
        latency_ms=(time.perf_counter() - started_at) * 1000,
        retry_count=retry_count,
        fault_events=fault_events,
    )


async def analyze_symbol(
    symbol: str,
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    fault_injector: FaultInjector | None = None,
) -> WatchSignal | None:
    result = await _analyze_symbol_with_diagnostics(
        symbol,
        config,
        fetcher=fetcher,
        fault_injector=fault_injector,
    )
    return result.signal


async def _scan_watchlist_internal(
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    fault_injector: FaultInjector | None = None,
) -> ScanDiagnostics:
    started_at = time.perf_counter()
    injector = fault_injector if fault_injector is not None else FaultInjector.disabled()
    tasks = [
        _analyze_symbol_with_diagnostics(symbol, config, fetcher=fetcher, fault_injector=injector)
        for symbol in config.watchlist
    ]
    results = await asyncio.gather(*tasks)

    signals = [item.signal for item in results if item.signal is not None]
    failures = [item.failure for item in results if item.failure is not None]
    fallback_count = sum(1 for item in results if item.used_fallback)
    latency_samples = [float(item.latency_ms) for item in results if item.latency_ms >= 0.0]
    total_retries = sum(int(max(0, item.retry_count)) for item in results)
    fault_events = [event for item in results for event in item.fault_events if isinstance(event, dict)]

    if signals:
        try:
            name_map = get_company_names_batch([sig.symbol for sig in signals], market=config.market, resolve_remote=False)
            for sig in signals:
                normalized = normalize_market_symbol(sig.symbol, market=config.market)
                resolved = name_map.get(normalized) or name_map.get(sig.symbol, "")
                if resolved:
                    sig.company_name = resolved
        except Exception as exc:
            failures.append(
                classify_failure(
                    source="scanner.company_name",
                    error_type=exc.__class__.__name__,
                    message=str(exc),
                )
            )
    order = {"critical": 0, "high": 1, "normal": 2}
    sorted_signals = sorted(signals, key=lambda x: order.get(x.priority, 9))
    latency_ms = (time.perf_counter() - started_at) * 1000
    return ScanDiagnostics(
        signals=sorted_signals,
        failures=failures,
        fallback_count=fallback_count,
        scan_latency_ms=latency_ms,
        symbol_latency_ms=latency_samples,
        retry_count=total_retries,
        fault_events=fault_events,
    )


async def scan_watchlist_with_diagnostics(
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    fault_injector: FaultInjector | None = None,
) -> tuple[list[WatchSignal], list[FailureEvent], int, float]:
    diagnostics = await _scan_watchlist_internal(config, fetcher=fetcher, fault_injector=fault_injector)
    return diagnostics.signals, diagnostics.failures, diagnostics.fallback_count, diagnostics.scan_latency_ms


async def scan_watchlist(
    config: ScanConfig,
    *,
    fetcher: Callable[..., Awaitable[MarketDataResult]] = fetch_market_data,
    fault_injector: FaultInjector | None = None,
) -> list[WatchSignal]:
    signals, _, _, _ = await scan_watchlist_with_diagnostics(
        config,
        fetcher=fetcher,
        fault_injector=fault_injector,
    )
    return signals


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
    plugin_manager: StrategyPluginManager | None = None,
    runtime_flags: dict[str, Any] | None = None,
    strategy_tier: str = "execution-ready",
) -> WatchlistRunResult:
    cycle_started_at = time.perf_counter()
    config_flags, fault_injector = resolve_fault_injection(runtime_flags)
    resolved_runtime = _resolve_runtime(config_flags)
    manager = plugin_manager or StrategyPluginManager.from_runtime_config(resolved_runtime.config)
    tier_decision = resolve_strategy_tier(
        strategy_tier,
        requested_enable_triggered_research=enable_triggered_research,
    )
    scan_diag = await _scan_watchlist_internal(config, fetcher=fetcher, fault_injector=fault_injector)
    signals = scan_diag.signals
    scan_failures = scan_diag.failures
    fallback_count = scan_diag.fallback_count
    scan_latency_ms = scan_diag.scan_latency_ms
    plugin_context = PluginContext(
        request_id=trigger.trigger_id,
        trigger_id=trigger.trigger_id,
        observability_tags={
            "trigger_type": trigger.trigger_type,
            "mode": mode,
        },
    )
    signal_payload, signal_audit = await manager.apply(
        kind=PluginKind.SIGNALS,
        payload={"signals": signals, "mode": mode},
        context=plugin_context,
    )
    signals = list(signal_payload.get("signals", signals) or [])
    selected = select_alerts_for_mode(signals, mode)
    alert_payload, alert_audit = await manager.apply(
        kind=PluginKind.ALERTS,
        payload={"selected_alerts": selected, "mode": mode},
        context=plugin_context,
    )
    selected = list(alert_payload.get("selected_alerts", selected) or [])
    policy_payload, policy_audit = await manager.apply(
        kind=PluginKind.POLICIES,
        payload={
            "allow_triggered_research": tier_decision.allow_triggered_research,
            "selected_alerts": selected,
            "mode": mode,
            "strategy_tier": tier_decision.tier,
        },
        context=plugin_context,
    )
    effective_enable_triggered_research = bool(
        policy_payload.get("allow_triggered_research", tier_decision.allow_triggered_research)
    ) and tier_decision.allow_triggered_research
    plugin_audit = [item.to_dict() for item in [*signal_audit, *alert_audit, *policy_audit]]

    snapshots: list[AlertSnapshot] = []
    failure_events = list(scan_failures)
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
        if effective_enable_triggered_research and signal.priority == "critical":
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
                backend = (research.sandbox_artifacts.backend or "").lower()
                if "fallback" in backend or "local-process" in backend:
                    fallback_count += 1
            except Exception as exc:  # pragma: no cover - defensive branch for runtime.
                snapshot.research_status = "failed"
                snapshot.research_error = str(exc)
                failure_events.append(
                    classify_failure(
                        source="scanner.research",
                        error_type=exc.__class__.__name__,
                        message=str(exc),
                    )
                )
        snapshots.append(snapshot)

    notifications: list[dict[str, Any]] = []
    if notifier is not None and tier_decision.allow_notification_dispatch:
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

    cycle_latency_ms = (time.perf_counter() - cycle_started_at) * 1000
    denominator = max(1, len(selected))
    fallback_rate = fallback_count / denominator
    failure_clusters = aggregate_failure_clusters(failure_events)
    failure_tags = aggregate_failure_tags(failure_events)
    alarms = evaluate_threshold_alarms(
        fallback_rate=fallback_rate,
        failure_count=len(failure_events),
        latency_ms=cycle_latency_ms,
        fallback_spike_rate=config.fallback_spike_rate,
        failure_spike_count=config.failure_spike_count,
        latency_anomaly_ms=config.latency_anomaly_ms,
    )
    budget = evaluate_latency_error_budget(
        latency_samples_ms=[*scan_diag.symbol_latency_ms, cycle_latency_ms],
        error_count=len(failure_events),
        total_count=max(1, len(config.watchlist)),
        fallback_count=fallback_count,
        retry_count=scan_diag.retry_count,
    )
    runtime_metrics: dict[str, Any] = {
        "watchlist_size": len(config.watchlist),
        "signal_count": len(signals),
        "selected_alert_count": len(selected),
        "scan_latency_ms": round(scan_latency_ms, 3),
        "cycle_latency_ms": round(cycle_latency_ms, 3),
        "failure_count": len(failure_events),
        "fallback_count": fallback_count,
        "fallback_rate": round(fallback_rate, 6),
        "failure_clusters": failure_clusters,
        "failure_tags": failure_tags,
        "plugin_audit_count": len(plugin_audit),
        "plugin_audit": plugin_audit,
        "config_layer_diff_count": len(resolved_runtime.diff_summary),
        "config_merge_priority": list(resolved_runtime.merge_priority),
        "config_changed_fields": resolved_runtime.diff_summary,
        "allow_triggered_research": effective_enable_triggered_research,
        "strategy_tier": tier_decision.tier,
        "strategy_tier_decision": tier_decision.to_dict(),
        "strategy_tier_notifications_guarded": not tier_decision.allow_notification_dispatch,
        "strategy_tier_notification_guarded_count": len(selected) if not tier_decision.allow_notification_dispatch else 0,
        "runtime_budget_verdict": budget.status,
        "runtime_budget_reasons": [item.to_dict() for item in budget.reasons],
        "runtime_latency_p50_ms": budget.metrics["p50_latency_ms"],
        "runtime_latency_p95_ms": budget.metrics["p95_latency_ms"],
        "runtime_error_rate": budget.metrics["error_rate"],
        "runtime_fallback_rate": budget.metrics["fallback_rate"],
        "runtime_retry_pressure": budget.metrics["retry_pressure"],
        "fault_injection_enabled": bool(fault_injector.enabled),
        "fault_injection_event_count": len(scan_diag.fault_events),
        "fault_injection_events": list(scan_diag.fault_events),
    }

    return WatchlistRunResult(
        trigger=trigger,
        signals=signals,
        selected_alerts=selected,
        snapshots=snapshots,
        notifications=notifications,
        runtime_metrics=runtime_metrics,
        failure_events=[event.to_dict() for event in failure_events],
        failure_clusters=failure_clusters,
        alarms=[item.to_dict() for item in alarms],
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
