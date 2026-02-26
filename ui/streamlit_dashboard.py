"""Week4 Streamlit dashboard: log streaming + Plotly watchlist board."""

from __future__ import annotations

import asyncio
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

from agents.scanner_engine import ScanConfig, scan_watchlist


def build_watchlist_figure(signals: list[dict]) -> go.Figure:
    df = pd.DataFrame(signals)
    fig = go.Figure()
    if df.empty:
        fig.update_layout(title="No signals yet")
        return fig

    color_map = {"critical": "#d62728", "high": "#ff7f0e", "normal": "#1f77b4"}
    colors = [color_map.get(item, "#1f77b4") for item in df["priority"]]
    fig.add_trace(
        go.Bar(
            x=df["symbol"],
            y=df["pct_change"] * 100,
            marker_color=colors,
            text=[f"RSI={r:.1f}" for r in df["rsi"]],
            name="Change %",
        )
    )
    fig.update_layout(title="Watchlist Intraday Signals", xaxis_title="Symbol", yaxis_title="% Change")
    return fig


def run_dashboard() -> None:
    import streamlit as st

    st.set_page_config(page_title="Alpha-Insight Cockpit", layout="wide")
    st.title("Alpha-Insight Week4 Cockpit")

    with st.sidebar:
        st.header("Controls")
        watchlist_raw = st.text_input("Watchlist", value="AAPL,MSFT,TSLA,NVDA")
        period = st.selectbox("Period", ["5d", "1mo", "3mo"], index=0)
        threshold = st.slider("Alert Threshold (%)", min_value=1.0, max_value=10.0, value=3.0)
        run_scan = st.button("Run Scan")

    st.subheader("Log Streaming")
    if "logs" not in st.session_state:
        st.session_state.logs = []

    log_container = st.empty()

    def push_log(msg: str) -> None:
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        st.session_state.logs.append(f"[{timestamp}] {msg}")
        log_container.code("\n".join(st.session_state.logs[-20:]), language="text")

    if run_scan:
        push_log("Planner: reading watchlist request")
        push_log("Scheduler: dispatching concurrent market fetch tasks")
        watchlist = [item.strip().upper() for item in watchlist_raw.split(",") if item.strip()]
        cfg = ScanConfig(watchlist=watchlist, period=period, pct_alert_threshold=threshold / 100.0)
        signals = asyncio.run(scan_watchlist(cfg))
        push_log(f"Engine: generated {len(signals)} signals")
        push_log("Reviewer: classifying priorities and preparing dashboard payload")

        rows = [
            {
                "symbol": item.symbol,
                "price": item.price,
                "pct_change": item.pct_change,
                "rsi": item.rsi,
                "priority": item.priority,
                "reason": item.reason,
            }
            for item in signals
        ]

        st.subheader("Plotly Signal Board")
        st.plotly_chart(build_watchlist_figure(rows), use_container_width=True)
        st.subheader("Signal Table")
        st.dataframe(pd.DataFrame(rows), use_container_width=True)


if __name__ == "__main__":
    run_dashboard()
