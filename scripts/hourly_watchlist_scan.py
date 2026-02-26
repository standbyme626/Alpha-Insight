"""Cron-friendly hourly watchlist scanner for Alpha-Insight Week4."""

from __future__ import annotations

import argparse
import asyncio
import os
from datetime import datetime, timezone

from agents.scanner_engine import ScanConfig, dispatch_telegram_alerts, format_signal_message, scan_watchlist


def _parse_watchlist(raw: str) -> list[str]:
    items = [part.strip().upper() for part in raw.split(",")]
    return [item for item in items if item]


async def _run_once(config: ScanConfig, *, mode: str) -> int:
    print(f"[DEBUG] QuantNode week4.hourly_scan Start @ {datetime.now(timezone.utc).isoformat()}")
    signals = await scan_watchlist(config)
    if not signals:
        print("[INFO] No signals generated.")
        return 0

    for signal in signals:
        print(format_signal_message(signal))

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if bot_token and chat_id:
        await dispatch_telegram_alerts(signals, bot_token=bot_token, chat_id=chat_id, mode=mode)
        print("[INFO] Telegram alerts dispatched.")
    else:
        print("[WARN] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; skip sending.")
    return 0


async def _loop(config: ScanConfig, *, mode: str, interval_seconds: int) -> None:
    while True:
        await _run_once(config, mode=mode)
        await asyncio.sleep(interval_seconds)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hourly watchlist scan")
    parser.add_argument("--watchlist", default=os.getenv("WATCHLIST", "AAPL,MSFT,TSLA"))
    parser.add_argument("--period", default=os.getenv("WATCH_PERIOD", "5d"))
    parser.add_argument("--mode", default=os.getenv("ALERT_MODE", "anomaly"), choices=["anomaly", "digest"])
    parser.add_argument("--threshold", type=float, default=float(os.getenv("ALERT_THRESHOLD", "0.03")))
    parser.add_argument("--once", action="store_true", help="Run once and exit (for cron)")
    parser.add_argument("--interval-seconds", type=int, default=3600)
    args = parser.parse_args()

    config = ScanConfig(
        watchlist=_parse_watchlist(args.watchlist),
        period=args.period,
        pct_alert_threshold=args.threshold,
    )
    if args.once:
        return asyncio.run(_run_once(config, mode=args.mode))

    asyncio.run(_loop(config, mode=args.mode, interval_seconds=args.interval_seconds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
