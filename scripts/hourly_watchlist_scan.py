"""Cron-friendly hourly watchlist scanner for Alpha-Insight Week4."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

from agents.scanner_engine import AlertSnapshotStore, ScanConfig, build_scan_trigger, format_signal_message, run_watchlist_cycle
from tools.market_data import get_market_top100_watchlist
from tools.telegram import TelegramNotifier


def _parse_watchlist(raw: str) -> list[str]:
    items = [part.strip().upper() for part in raw.split(",")]
    return [item for item in items if item]


def _granularity_to_interval(value: str) -> str:
    mapping = {"day": "1d", "hour": "60m", "minute": "5m"}
    return mapping.get(value, "1d")


def _parse_trigger_metadata(raw: str) -> dict[str, Any]:
    payload = (raw or "").strip()
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        print("[WARN] trigger metadata is not valid JSON; ignore metadata payload.")
        return {}
    if not isinstance(parsed, dict):
        print("[WARN] trigger metadata must be a JSON object; ignore metadata payload.")
        return {}
    return parsed


async def _run_once(
    config: ScanConfig,
    *,
    mode: str,
    trigger_type: str,
    trigger_id: str,
    trigger_metadata: dict[str, Any],
    snapshot_path: str,
    enable_triggered_research: bool,
) -> int:
    print(f"[DEBUG] QuantNode week4.hourly_scan Start @ {datetime.now(timezone.utc).isoformat()}")
    trigger = build_scan_trigger(
        trigger_type=trigger_type,
        trigger_id=trigger_id,
        metadata=trigger_metadata,
    )
    snapshot_store = AlertSnapshotStore(snapshot_path if snapshot_path else None)
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    notifier = TelegramNotifier(bot_token, chat_id) if bot_token and chat_id else None
    result = await run_watchlist_cycle(
        config,
        trigger=trigger,
        mode=mode,
        notifier=notifier,
        snapshot_store=snapshot_store,
        enable_triggered_research=enable_triggered_research,
    )
    signals = result.signals
    if not signals:
        print("[INFO] No signals generated.")
        return 0

    for signal in signals:
        print(format_signal_message(signal))

    if notifier is not None:
        print(f"[INFO] Notifications dispatched: {len(result.notifications)}")
    else:
        print("[WARN] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; skip sending.")
    print(f"[INFO] Alert snapshots persisted: {len(result.snapshots)}")
    triggered_runs = [item.research_run_id for item in result.snapshots if item.research_run_id]
    if triggered_runs:
        print(f"[INFO] Triggered research run_ids: {', '.join(triggered_runs)}")
    print(f"[INFO] Runtime metrics: {json.dumps(result.runtime_metrics, ensure_ascii=False)}")
    if result.alarms:
        print("[WARN] Threshold alarms triggered:")
        for alarm in result.alarms:
            print(f"  - [{alarm['severity']}] {alarm['rule']}: {alarm['message']}")
    return 0


async def _loop(
    config: ScanConfig,
    *,
    mode: str,
    interval_seconds: int,
    trigger_type: str,
    trigger_id: str,
    trigger_metadata: dict[str, Any],
    snapshot_path: str,
    enable_triggered_research: bool,
) -> None:
    while True:
        await _run_once(
            config,
            mode=mode,
            trigger_type=trigger_type,
            trigger_id=trigger_id,
            trigger_metadata=trigger_metadata,
            snapshot_path=snapshot_path,
            enable_triggered_research=enable_triggered_research,
        )
        await asyncio.sleep(interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly watchlist scan")
    parser.add_argument("--watchlist", default=os.getenv("WATCHLIST", "AAPL,MSFT,TSLA"))
    parser.add_argument("--market", default=os.getenv("WATCH_MARKET", "auto"), choices=["auto", "us", "hk", "cn"])
    parser.add_argument("--top100", action="store_true", help="Use top100 watchlist for selected market")
    parser.add_argument("--period", default=os.getenv("WATCH_PERIOD", "5d"))
    parser.add_argument("--granularity", default=os.getenv("WATCH_GRANULARITY", "day"), choices=["day", "hour", "minute"])
    parser.add_argument("--mode", default=os.getenv("ALERT_MODE", "anomaly"), choices=["anomaly", "digest"])
    parser.add_argument("--threshold", type=float, default=float(os.getenv("ALERT_THRESHOLD", "0.03")))
    parser.add_argument("--fallback-spike-rate", type=float, default=float(os.getenv("FALLBACK_SPIKE_RATE", "0.25")))
    parser.add_argument("--failure-spike-count", type=int, default=int(os.getenv("FAILURE_SPIKE_COUNT", "3")))
    parser.add_argument("--latency-anomaly-ms", type=float, default=float(os.getenv("LATENCY_ANOMALY_MS", "2500")))
    parser.add_argument("--trigger-type", default=os.getenv("SCAN_TRIGGER_TYPE", "scheduled"), choices=["scheduled", "event"])
    parser.add_argument("--trigger-id", default=os.getenv("SCAN_TRIGGER_ID", ""))
    parser.add_argument(
        "--trigger-metadata",
        default=os.getenv("SCAN_TRIGGER_METADATA", ""),
        help='JSON object string, e.g. \'{"source":"news_breaking"}\'',
    )
    parser.add_argument("--snapshot-path", default=os.getenv("ALERT_SNAPSHOT_PATH", ""))
    parser.add_argument(
        "--disable-triggered-research",
        action="store_true",
        help="Disable auto run_unified_research on critical alerts",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit (for cron)")
    parser.add_argument("--interval-seconds", type=int, default=3600)
    args = parser.parse_args()

    watchlist = _parse_watchlist(args.watchlist)
    if args.market in {"cn", "hk", "us"} and args.top100:
        watchlist = get_market_top100_watchlist(args.market)[:100]

    config = ScanConfig(
        watchlist=watchlist,
        market=args.market,
        period=args.period,
        interval=_granularity_to_interval(args.granularity),
        pct_alert_threshold=args.threshold,
        fallback_spike_rate=args.fallback_spike_rate,
        failure_spike_count=args.failure_spike_count,
        latency_anomaly_ms=args.latency_anomaly_ms,
    )
    trigger_metadata = _parse_trigger_metadata(args.trigger_metadata)
    if args.once:
        return asyncio.run(
            _run_once(
                config,
                mode=args.mode,
                trigger_type=args.trigger_type,
                trigger_id=args.trigger_id,
                trigger_metadata=trigger_metadata,
                snapshot_path=args.snapshot_path,
                enable_triggered_research=not args.disable_triggered_research,
            )
        )

    asyncio.run(
        _loop(
            config,
            mode=args.mode,
            interval_seconds=args.interval_seconds,
            trigger_type=args.trigger_type,
            trigger_id=args.trigger_id,
            trigger_metadata=trigger_metadata,
            snapshot_path=args.snapshot_path,
            enable_triggered_research=not args.disable_triggered_research,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
