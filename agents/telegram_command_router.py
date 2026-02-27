from __future__ import annotations

import re
from dataclasses import dataclass

from tools.market_data import normalize_market_symbol

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_INTERVAL_PATTERN = re.compile(r"^(\d+)([smhd])$", re.IGNORECASE)
_JOB_ID_PATTERN = re.compile(r"^job-[a-f0-9]{8}$")


@dataclass
class CommandRoute:
    name: str
    args: dict[str, str]


@dataclass
class CommandError:
    message: str


def _parse_interval_to_seconds(raw: str) -> int | None:
    match = _INTERVAL_PATTERN.fullmatch((raw or "").strip())
    if not match:
        return None
    value = int(match.group(1))
    unit = match.group(2).lower()
    if value <= 0:
        return None
    factor = {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
    seconds = value * factor
    if seconds < 60 or seconds > 86400:
        return None
    return seconds


def parse_telegram_command(text: str) -> CommandRoute | CommandError:
    raw = (text or "").strip()
    if not raw:
        return CommandError("Empty command. Use /help to see available commands.")
    parts = raw.split()
    command = parts[0].lower()

    if command == "/help":
        if len(parts) > 1:
            return CommandError("`/help` does not take parameters.")
        return CommandRoute(name="help", args={})

    if command == "/analyze":
        if len(parts) != 2:
            return CommandError("Usage: /analyze <symbol>. Example: /analyze AAPL")
        symbol = normalize_market_symbol(parts[1], market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return CommandError(f"Invalid symbol: {parts[1]}. Example: /analyze AAPL")
        return CommandRoute(name="analyze", args={"symbol": symbol})

    if command == "/monitor":
        if len(parts) != 3:
            return CommandError("Usage: /monitor <symbol> <interval>. Example: /monitor TSLA 1h")
        symbol = normalize_market_symbol(parts[1], market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return CommandError(f"Invalid symbol: {parts[1]}. Example: /monitor TSLA 1h")
        interval_seconds = _parse_interval_to_seconds(parts[2])
        if interval_seconds is None:
            return CommandError("Invalid interval. Use 1m-24h, e.g. 5m, 1h, 4h.")
        return CommandRoute(
            name="monitor",
            args={"symbol": symbol, "interval": parts[2].lower(), "interval_sec": str(interval_seconds)},
        )

    if command == "/list":
        if len(parts) > 1:
            return CommandError("`/list` does not take parameters.")
        return CommandRoute(name="list", args={})

    if command == "/stop":
        if len(parts) != 2:
            return CommandError("Usage: /stop <job_id|symbol>. Example: /stop TSLA")
        target = parts[1].strip()
        if not target:
            return CommandError("Usage: /stop <job_id|symbol>. Example: /stop TSLA")
        if _JOB_ID_PATTERN.fullmatch(target.lower()):
            return CommandRoute(name="stop", args={"target": target.lower(), "target_type": "job_id"})
        symbol = normalize_market_symbol(target, market="auto")
        if not symbol or not _SYMBOL_PATTERN.fullmatch(symbol):
            return CommandError("Stop target must be a valid symbol or job_id, e.g. /stop TSLA or /stop job-deadbeef")
        return CommandRoute(name="stop", args={"target": symbol, "target_type": "symbol"})

    return CommandError(f"Unsupported command: {parts[0]}. Use /help to see available commands.")
