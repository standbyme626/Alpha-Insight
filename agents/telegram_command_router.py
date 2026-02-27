from __future__ import annotations

import re
from dataclasses import dataclass

from tools.market_data import normalize_market_symbol

_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.\-]{1,12}$")


@dataclass
class CommandRoute:
    name: str
    args: dict[str, str]


@dataclass
class CommandError:
    message: str


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

    return CommandError(f"Unsupported command: {parts[0]}. Use /help to see available commands.")

