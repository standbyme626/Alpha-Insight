"""Week2 coder: generate sandbox-first quant code."""

from __future__ import annotations

from typing import Any


CODER_STYLE_GUIDE = """
1. Always import pandas as pd.
2. Normalize Date to UTC when available.
3. All numeric outputs must be computed in sandbox Python, never in LLM text.
4. Keep variable names deterministic: df, summary, metrics.
""".strip()


def _base_script(symbol: str, period: str, *, bad_column: bool = False) -> str:
    ma_source_col = "Clsoe" if bad_column else "Close"
    return f"""
import pandas as pd
import yfinance as yf

symbol = {symbol!r}
period = {period!r}

df = yf.Ticker(symbol).history(period=period).reset_index()
if df.empty:
    raise ValueError("数据未找到")

if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")

for col in ["Open", "High", "Low", "Close", "Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.fillna(0)
df["MA5"] = df[{ma_source_col!r}].rolling(5).mean().fillna(0)
summary = df.tail(1)[["Date", "Close", "MA5"]]
print(summary.to_string(index=False))
""".strip()


def generate_code(state: dict[str, Any]) -> str:
    symbol = str(state.get("symbol", "AAPL"))
    period = str(state.get("period", "1mo"))

    # Test hook: force a bad first attempt to validate self-correction loop.
    inject_failure = bool(state.get("inject_failure", False))
    retry_count = int(state.get("retry_count", 0))

    if inject_failure and retry_count == 0:
        return _base_script(symbol, period, bad_column=True)

    debug_advice = str(state.get("debug_advice") or "")
    if "use_close_column" in debug_advice:
        return _base_script(symbol, period, bad_column=False)

    return _base_script(symbol, period, bad_column=False)
