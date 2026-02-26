"""Realtime analysis cockpit: run status, signal board, and alert operations."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

# Ensure imports work even when streamlit is launched outside repo root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.scanner_engine import (
    ScanConfig,
    dispatch_telegram_alerts,
    format_signal_message,
    scan_watchlist,
    select_alerts_for_mode,
)
from core.observability import QuantTelemetry


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


def build_execution_code(config: ScanConfig, mode: str, rows: list[dict]) -> str:
    symbols = ", ".join(repr(symbol) for symbol in config.watchlist)
    signal_lines = "\n".join(
        f"signals.append({{'symbol': '{row['symbol']}', 'priority': '{row['priority']}', 'pct_change': {row['pct_change']:.6f}}})"
        for row in rows
    )
    selected = [row for row in rows if mode == "digest" or row["priority"] in {"critical", "high"}]
    return (
        "from agents.scanner_engine import ScanConfig, scan_watchlist, select_alerts_for_mode\n\n"
        f"cfg = ScanConfig(watchlist=[{symbols}], market='{config.market}', period='{config.period}', pct_alert_threshold={config.pct_alert_threshold})\n"
        "signals = []\n"
        f"{signal_lines if signal_lines else '# no signals'}\n"
        f"alert_mode = '{mode}'\n"
        f"selected_alerts = {len(selected)}  # anomaly=high/critical, digest=all\n"
    )


def _build_node_status(events: list[dict], selected_count: int, sent_count: int) -> pd.DataFrame:
    rows = []
    for event in events:
        node = event["node"]
        status = "success" if event["status"] == "ok" else "failed"
        rows.append(
            {
                "node": node,
                "status": status,
                "duration_ms": event["duration_ms"],
                "detail": "",
            }
        )
    rows.append(
        {
            "node": "reviewer.select_alert_mode",
            "status": "success",
            "duration_ms": 0.0,
            "detail": f"selected={selected_count}",
        }
    )
    rows.append(
        {
            "node": "dispatcher.telegram_send",
            "status": "success" if sent_count >= 0 else "skipped",
            "duration_ms": 0.0,
            "detail": f"sent={sent_count}",
        }
    )
    return pd.DataFrame(rows)


def _render_quickstart(st) -> None:  # noqa: ANN001
    with st.expander("How To Use Alpha-Insight", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "1. Select `Market` (`US` or `CN`) and pick a watchlist preset or custom symbols.",
                    "2. Choose `Period`, `Alert Mode`, and threshold.",
                    "3. Click `Run Realtime Scan`.",
                    "4. Review `Pipeline Status`, `Node Runtime`, and `Signal Board`.",
                    "5. Optionally enable Telegram sending for selected alerts.",
                ]
            )
        )
    with st.expander("Plan Delivery Matrix", expanded=False):
        matrix = pd.DataFrame(
            [
                {"module": "Data & Sandbox", "status": "completed", "note": "Market API, scraper fallback, sandbox manager"},
                {"module": "Self-Correction Loop", "status": "completed", "note": "Planner/Coder/Executor/Debugger loop"},
                {"module": "Quant Report", "status": "completed", "note": "Indicators, backtest, report payload"},
                {"module": "Realtime Scan & Alerts", "status": "completed", "note": "Priority scan + Telegram dispatch"},
                {"module": "Frontend Productization", "status": "in_progress", "note": "Pipeline observability and onboarding added"},
                {"module": "CN Market Adaptation", "status": "in_progress", "note": "CN symbol normalization and UI presets"},
                {"module": "Phoenix/LangSmith Full Link", "status": "partial", "note": "Telemetry abstraction exists; full remote wiring pending"},
            ]
        )
        st.dataframe(matrix, use_container_width=True)


def run_dashboard() -> None:
    import streamlit as st

    st.set_page_config(page_title="Alpha-Insight Realtime Cockpit", layout="wide")
    st.title("Alpha-Insight Realtime Cockpit")
    st.caption("Visible execution pipeline for market scan, risk scoring, and alert dispatch.")

    _render_quickstart(st)

    with st.sidebar:
        st.header("Run Config")
        market = st.selectbox("Market", ["US", "CN", "AUTO"], index=2)
        if market == "CN":
            default_watchlist = "贵州茅台,宁德时代,招商银行,600519,000001"
        else:
            default_watchlist = "AAPL,MSFT,TSLA,NVDA"
        watchlist_raw = st.text_input("Watchlist", value=default_watchlist)
        period = st.selectbox("Period", ["5d", "1mo", "3mo"], index=0)
        mode = st.selectbox("Alert Mode", ["anomaly", "digest"], index=0)
        threshold = st.slider("Alert Threshold (%)", min_value=1.0, max_value=10.0, value=3.0)
        send_to_telegram = st.checkbox("Send Telegram", value=False)
        run_scan = st.button("Run Realtime Scan")

    st.subheader("Runtime Log")
    if "logs" not in st.session_state:
        st.session_state.logs = []

    log_container = st.empty()

    def push_log(msg: str) -> None:
        timestamp = datetime.utcnow().strftime("%H:%M:%S")
        st.session_state.logs.append(f"[{timestamp}] {msg}")
        log_container.code("\n".join(st.session_state.logs[-20:]), language="text")

    if run_scan:
        telemetry = QuantTelemetry()
        push_log("Planner: reading watchlist request")
        with telemetry.span("planner.parse_watchlist"):
            watchlist = [item.strip().upper() for item in watchlist_raw.split(",") if item.strip()]
            cfg = ScanConfig(
                watchlist=watchlist,
                market=market.lower(),
                period=period,
                pct_alert_threshold=threshold / 100.0,
            )
        push_log("Scheduler: dispatching concurrent market fetch tasks")
        with telemetry.span("scanner.scan_watchlist"):
            signals = asyncio.run(scan_watchlist(cfg))
        push_log(f"Engine: generated {len(signals)} signals")
        push_log("Reviewer: classifying priorities and preparing dashboard payload")
        with telemetry.span("reviewer.select_alert_mode"):
            selected = select_alerts_for_mode(signals, mode)

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
        trace_rows = [{"node": event.name, "duration_ms": round(event.duration_ms, 2), "status": event.status} for event in telemetry.flush()]
        sent_count = 0

        tab_overview, tab_pipeline, tab_signals, tab_artifacts = st.tabs(
            ["Overview", "Pipeline Status", "Signals", "Artifacts & Alerts"]
        )
        with tab_overview:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Signals", len(rows))
            c2.metric("Critical/High", len([x for x in rows if x["priority"] in {"critical", "high"}]))
            c3.metric("Selected", len(selected))
            c4.metric("Market", cfg.market.upper())
            st.plotly_chart(build_watchlist_figure(rows), use_container_width=True)

        with tab_pipeline:
            st.markdown("**Pipeline Graph**")
            st.code(
                "Planner -> Scanner -> Reviewer -> Dispatcher",
                language="text",
            )
            st.markdown("**Node Runtime**")
            node_df = _build_node_status(trace_rows, len(selected), sent_count)
            st.dataframe(node_df, use_container_width=True)
            st.markdown("**Thought Trace**")
            st.dataframe(pd.DataFrame(trace_rows), use_container_width=True)

        with tab_signals:
            st.dataframe(pd.DataFrame(rows), use_container_width=True)

        with tab_artifacts:
            st.markdown("**Code Flow**")
            st.code(build_execution_code(cfg, mode, rows), language="python")
            st.markdown("**Telegram Preview**")
            preview = [format_signal_message(item) for item in selected]
            st.code("\n\n".join(preview) if preview else "No alerts selected for current mode.", language="text")

        if send_to_telegram:
            token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
            if not token or not chat_id:
                st.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing; skip sending.")
            else:
                with telemetry.span("dispatcher.telegram_send"):
                    responses = asyncio.run(dispatch_telegram_alerts(signals, bot_token=token, chat_id=chat_id, mode=mode))
                sent_count = len(responses)
                st.success(f"Telegram sent: {sent_count} messages")


if __name__ == "__main__":
    run_dashboard()
