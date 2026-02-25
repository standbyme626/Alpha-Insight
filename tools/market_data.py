"""Async market data adapter based on yfinance."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pandas as pd
import yfinance as yf


@dataclass
class MarketDataResult:
    ok: bool
    symbol: str
    message: str
    records: list[dict[str, Any]]


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.reset_index().copy()
    if "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce", utc=True)

    numeric_cols = out.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        out[numeric_cols] = out[numeric_cols].ffill().bfill().fillna(0)
    return out


async def fetch_market_data(symbol: str, period: str = "1mo") -> MarketDataResult:
    print("[DEBUG] QuantNode fetch_market_data Start")
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        return MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message="数据未找到: symbol 为空。",
            records=[],
        )

    try:
        ticker = yf.Ticker(normalized_symbol)
        df = await asyncio.to_thread(ticker.history, period=period)
    except Exception as exc:  # pragma: no cover - network/runtime variability
        return MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message=f"数据未找到: yfinance 请求失败 ({exc})",
            records=[],
        )

    if df.empty:
        return MarketDataResult(
            ok=False,
            symbol=normalized_symbol,
            message="数据未找到",
            records=[],
        )

    normalized = _normalize_dataframe(df)
    records = normalized.to_dict(orient="records")
    return MarketDataResult(
        ok=True,
        symbol=normalized_symbol,
        message="ok",
        records=records,
    )
