"""Stock data tools backed by yfinance."""

from __future__ import annotations

from typing import Any

import pandas as pd
import yfinance as yf


def _error_payload(tool_name: str, symbol: str, message: str, error_type: str) -> dict[str, Any]:
    print(f"[DEBUG] Tool {tool_name} Start")
    return {
        "ok": False,
        "error": {
            "type": error_type,
            "message": message,
            "symbol": symbol,
        },
    }


def get_stock_info(symbol: str) -> dict[str, Any]:
    print("[DEBUG] Tool get_stock_info Start")
    symbol = symbol.strip().upper()
    if not symbol:
        return _error_payload(
            "get_stock_info",
            symbol,
            "Stock symbol is empty. Please provide a valid ticker (e.g., AAPL).",
            "ValidationError",
        )

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}
    except Exception as exc:  # pragma: no cover - network/remote failures
        return _error_payload(
            "get_stock_info",
            symbol,
            f"Failed to fetch stock info from yfinance: {exc}",
            "DataSourceError",
        )

    market_cap = info.get("marketCap")
    industry = info.get("industry")
    pe_ratio = info.get("trailingPE")

    if market_cap is None and industry is None and pe_ratio is None:
        return _error_payload(
            "get_stock_info",
            symbol,
            "No fundamental data found. The ticker may be invalid, delisted, or unsupported.",
            "InvalidSymbolError",
        )

    return {
        "ok": True,
        "symbol": symbol,
        "market_cap": market_cap,
        "industry": industry,
        "pe_ratio": pe_ratio,
    }


def get_historical_prices(symbol: str, period: str = "1mo") -> pd.DataFrame | dict[str, Any]:
    print("[DEBUG] Tool get_historical_prices Start")
    symbol = symbol.strip().upper()
    if not symbol:
        return _error_payload(
            "get_historical_prices",
            symbol,
            "Stock symbol is empty. Please provide a valid ticker (e.g., TSLA).",
            "ValidationError",
        )

    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period)
    except Exception as exc:  # pragma: no cover - network/remote failures
        return _error_payload(
            "get_historical_prices",
            symbol,
            f"Failed to fetch historical prices from yfinance: {exc}",
            "DataSourceError",
        )

    if data.empty:
        return _error_payload(
            "get_historical_prices",
            symbol,
            f"No historical price data returned for period='{period}'. The ticker may be invalid.",
            "InvalidSymbolError",
        )

    # Ensure a standard, easy-to-use DataFrame format.
    data = data.reset_index()
    if "Date" in data.columns:
        data["Date"] = pd.to_datetime(data["Date"], errors="coerce")
    return data
