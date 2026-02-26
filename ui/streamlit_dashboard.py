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
from tools.market_data import get_market_top100_constituents, get_market_top100_watchlist


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
    fig.update_layout(title="Watchlist Signals / 监控池信号", xaxis_title="Symbol / 标的", yaxis_title="% Change / 涨跌幅")
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
        f"cfg = ScanConfig(watchlist=[{symbols}], market='{config.market}', period='{config.period}', interval='{config.interval}', pct_alert_threshold={config.pct_alert_threshold})\n"
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
    with st.expander("How To Use / 使用指南", expanded=True):
        st.markdown(
            "\n".join(
                [
                    "1. Select `Market / 市场` (`US` or `CN`) and choose watchlist preset.",
                    "2. Choose `Period / 区间` + `Granularity / 粒度` (`day/hour/minute`).",
                    "3. Click `点击分析 / Analyze Now`.",
                    "4. Review `Pipeline Status / 流水线状态`, `Node Runtime / 节点耗时`, and `Signal Board / 信号看板`.",
                    "5. Optionally enable Telegram dispatch for selected alerts.",
                ]
            )
        )
    with st.expander("Plan Delivery Matrix / 计划完成矩阵", expanded=False):
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


def _granularity_to_interval(value: str) -> str:
    mapping = {"day": "1d", "hour": "60m", "minute": "5m"}
    return mapping.get(value, "1d")


def run_dashboard() -> None:
    import streamlit as st

    st.set_page_config(page_title="Alpha-Insight Realtime Cockpit / 实时驾驶舱", layout="wide")
    st.title("Alpha-Insight Realtime Cockpit / 实时驾驶舱")
    st.caption("Visible execution pipeline for market scan, risk scoring, and alert dispatch / 可视化展示扫描、评分、告警全链路。")

    _render_quickstart(st)

    with st.sidebar:
        st.header("Run Config / 运行配置")
        market = st.selectbox("Market", ["US", "HK", "CN", "AUTO"], index=2)
        use_top100 = st.checkbox(
            "Top100 Pool / 市值前100监控池",
            value=(market in {"US", "HK", "CN"}),
            disabled=(market == "AUTO"),
        )
        granularity = st.selectbox("Granularity / 粒度", ["day", "hour", "minute"], index=0)
        if market in {"CN", "HK", "US"}:
            default_watchlist = ",".join(get_market_top100_watchlist(market.lower())[:100]) if use_top100 else (
                "贵州茅台,宁德时代,招商银行,600519,000001" if market == "CN" else "AAPL,MSFT,TSLA,NVDA"
            )
        else:
            default_watchlist = "AAPL,MSFT,TSLA,NVDA"
        watchlist_raw = st.text_input("Watchlist / 监控列表", value=default_watchlist)
        period = st.selectbox("Period / 时间区间", ["5d", "1mo", "3mo"], index=0)
        mode = st.selectbox("Alert Mode / 告警模式", ["anomaly", "digest"], index=0)
        threshold = st.slider("Alert Threshold (%) / 阈值", min_value=1.0, max_value=10.0, value=3.0)
        send_to_telegram = st.checkbox("Send Telegram / 发送电报", value=False)
        run_scan = st.button("点击分析 / Analyze Now")
        if market in {"CN", "HK", "US"} and use_top100:
            st.markdown("**Top100 Constituents / 市值前100成分（代码+公司名）**")
            cache_key = f"top100_df_{market.lower()}"
            if cache_key not in st.session_state:
                with st.spinner("Loading company names / 正在加载公司名..."):
                    st.session_state[cache_key] = pd.DataFrame(get_market_top100_constituents(market.lower()))
            st.dataframe(
                st.session_state[cache_key],
                use_container_width=True,
                height=420,
                column_config={"symbol": "Symbol / 代码", "name": "Company / 公司名"},
            )

    st.subheader("Runtime Log / 运行日志")
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
            if market in {"CN", "HK", "US"} and use_top100:
                watchlist = get_market_top100_watchlist(market.lower())[:100]
            cfg = ScanConfig(
                watchlist=watchlist,
                market=market.lower(),
                period=period,
                interval=_granularity_to_interval(granularity),
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
                "company_name": item.company_name,
                "timestamp": item.timestamp.isoformat(),
                "date": item.timestamp.strftime("%Y-%m-%d"),
                "hour": item.timestamp.strftime("%H"),
                "minute": item.timestamp.strftime("%M"),
            }
            for item in signals
        ]
        trace_rows = [{"node": event.name, "duration_ms": round(event.duration_ms, 2), "status": event.status} for event in telemetry.flush()]
        sent_count = 0

        tab_overview, tab_pipeline, tab_signals, tab_artifacts = st.tabs(
            ["Overview / 总览", "Pipeline Status / 流水线", "Signals / 信号", "Artifacts & Alerts / 产物与告警"]
        )
        with tab_overview:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Signals / 信号数", len(rows))
            c2.metric("Critical/High / 高优先级", len([x for x in rows if x["priority"] in {"critical", "high"}]))
            c3.metric("Selected / 入选告警", len(selected))
            c4.metric("Market / 市场", cfg.market.upper())
            st.caption(f"Granularity / 粒度: `{granularity}` | Interval: `{cfg.interval}` | Watchlist Size / 监控数量: `{len(cfg.watchlist)}`")
            st.plotly_chart(build_watchlist_figure(rows), use_container_width=True)

        with tab_pipeline:
            st.markdown("**Pipeline Graph / 流水线图**")
            st.code(
                "Planner -> Scanner -> Reviewer -> Dispatcher",
                language="text",
            )
            st.markdown("**Node Runtime / 节点耗时**")
            node_df = _build_node_status(trace_rows, len(selected), sent_count)
            st.dataframe(node_df, use_container_width=True)
            st.markdown("**Thought Trace / 执行轨迹**")
            st.dataframe(pd.DataFrame(trace_rows), use_container_width=True)

        with tab_signals:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, column_config={
                "symbol": "Symbol / 标的",
                "price": "Price / 价格",
                "pct_change": "PctChange / 涨跌幅",
                "rsi": "RSI",
                "priority": "Priority / 优先级",
                "reason": "Reason / 原因",
                "company_name": "Company / 公司名",
                "timestamp": "Timestamp / 时间戳",
                "date": "Date / 日期",
                "hour": "Hour / 小时",
                "minute": "Minute / 分钟",
            })

        with tab_artifacts:
            st.markdown("**Code Flow / 代码流**")
            st.code(build_execution_code(cfg, mode, rows), language="python")
            st.markdown("**Telegram Preview / 电报预览**")
            preview = [format_signal_message(item) for item in selected]
            st.code("\n\n".join(preview) if preview else "No alerts selected for current mode. / 当前模式未选出告警。", language="text")

        if send_to_telegram:
            token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
            chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
            if not token or not chat_id:
                st.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID missing; skip sending. / 缺少配置，跳过发送。")
            else:
                with telemetry.span("dispatcher.telegram_send"):
                    responses = asyncio.run(dispatch_telegram_alerts(signals, bot_token=token, chat_id=chat_id, mode=mode))
                sent_count = len(responses)
                st.success(f"Telegram sent / 已发送: {sent_count} messages")


if __name__ == "__main__":
    run_dashboard()
