"""Week3 coder: TA indicators, vectorized backtest, and multimodal report code."""

from __future__ import annotations

import json
from typing import Any


def _sanitize(value: str, fallback: str) -> str:
    value = (value or "").strip()
    return value if value else fallback


def build_week3_code(state: dict[str, Any]) -> str:
    symbol = _sanitize(str(state.get("symbol", "AAPL")), "AAPL")
    period = _sanitize(str(state.get("period", "6mo")), "6mo")
    sentiment_score = float(state.get("sentiment_score", 0.0))

    payload = json.dumps(
        {
            "symbol": symbol,
            "period": period,
            "sentiment_score": sentiment_score,
        }
    )

    return f"""
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

cfg = json.loads({payload!r})
symbol = cfg["symbol"]
period = cfg["period"]
sentiment_score = float(cfg["sentiment_score"])

df = yf.Ticker(symbol).history(period=period).reset_index()
if df.empty:
    raise ValueError("数据未找到")

if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
for col in ["Open", "High", "Low", "Close", "Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.fillna(0)

close = df["Close"]

# TA-Lib first, pandas fallback
try:
    import talib  # type: ignore
    macd, macd_signal, macd_hist = talib.MACD(close.values, fastperiod=12, slowperiod=26, signalperiod=9)
    rsi = talib.RSI(close.values, timeperiod=14)
    df["MACD"] = pd.Series(macd)
    df["MACD_SIGNAL"] = pd.Series(macd_signal)
    df["RSI"] = pd.Series(rsi)
except Exception:
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean().replace(0, np.nan)
    rs = roll_up / roll_down
    df["RSI"] = (100 - (100 / (1 + rs))).fillna(50)

df = df.fillna(method="ffill").fillna(0)

# Simple vectorized backtest
short = close.rolling(5).mean()
long = close.rolling(20).mean()
signal = (short > long).astype(int)
strategy_ret = signal.shift(1).fillna(0) * close.pct_change().fillna(0)
benchmark_ret = close.pct_change().fillna(0)

strategy_curve = (1 + strategy_ret).cumprod()
benchmark_curve = (1 + benchmark_ret).cumprod()

rolling_max = strategy_curve.cummax().replace(0, np.nan)
drawdown = (strategy_curve / rolling_max - 1).fillna(0)
max_drawdown = float(drawdown.min())
win_rate = float((strategy_ret > 0).sum() / max((strategy_ret != 0).sum(), 1))

latest = df.iloc[-1]
latest_rsi = float(latest.get("RSI", 50))
latest_macd = float(latest.get("MACD", 0))
latest_signal = float(latest.get("MACD_SIGNAL", 0))

tech_score = 50.0
if latest_rsi < 30:
    tech_score += 15
elif latest_rsi > 70:
    tech_score -= 15

if latest_macd > latest_signal:
    tech_score += 10
else:
    tech_score -= 10

tech_score += max(min(win_rate * 30, 20), -20)
tech_score += max(min(strategy_curve.iloc[-1] - 1, 0.2), -0.2) * 100
tech_score = float(max(0, min(100, tech_score)))

# Multimodal fused score: sentiment + technical
fused_score = float(max(0, min(100, 0.4 * sentiment_score + 0.6 * tech_score)))
recommendation = "BUY" if fused_score >= 70 else "HOLD"

report = {{
    "symbol": symbol,
    "period": period,
    "sentiment_score": sentiment_score,
    "technical_score": tech_score,
    "fused_score": fused_score,
    "recommendation": recommendation,
    "win_rate": win_rate,
    "max_drawdown": max_drawdown,
    "strategy_return": float(strategy_curve.iloc[-1] - 1),
    "benchmark_return": float(benchmark_curve.iloc[-1] - 1),
}}

# HTML output (Plotly first, fallback to plain HTML table)
html_path = Path("report_week3.html")
png_path = Path("chart_week3.png")
pdf_path = Path("report_week3.pdf")

try:
    import plotly.graph_objects as go  # type: ignore
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["Date"], y=df["Close"], mode="lines", name="Close"))
    fig.add_trace(go.Scatter(x=df["Date"], y=short, mode="lines", name="MA5"))
    fig.add_trace(go.Scatter(x=df["Date"], y=long, mode="lines", name="MA20"))
    fig.update_layout(title=f"{{symbol}} Price & MA", xaxis_title="Date", yaxis_title="Price")
    fig.write_html(str(html_path), include_plotlyjs="cdn")
except Exception:
    html = "<html><body><h2>Week3 Report</h2>" + df.tail(20).to_html(index=False) + "</body></html>"
    html_path.write_text(html, encoding="utf-8")

try:
    import matplotlib.pyplot as plt  # type: ignore
    plt.figure(figsize=(8, 4))
    plt.plot(df["Date"], df["Close"], label="Close")
    plt.plot(df["Date"], short, label="MA5")
    plt.plot(df["Date"], long, label="MA20")
    plt.legend()
    plt.tight_layout()
    plt.savefig(png_path)
    plt.savefig(pdf_path)
    plt.close()
except Exception:
    pass

print("METRICS_JSON=" + json.dumps(report, ensure_ascii=False))
print("ARTIFACT_HTML=" + str(html_path))
print("ARTIFACT_PNG=" + str(png_path))
print("ARTIFACT_PDF=" + str(pdf_path))
""".strip()
