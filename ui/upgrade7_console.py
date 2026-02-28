"""Upgrade7 frontend console: runs / alerts / evidence with typed resources."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ui.typed_resource_client import FrontendResourceClient


def _render_runs_tab(client: FrontendResourceClient) -> None:
    st.subheader("Runs / 运行记录")
    runs = client.list_runs(limit=100)
    if not runs:
        st.info("No runs found in store. / 当前数据库没有运行记录。")
        return
    rows: list[dict[str, object]] = []
    for item in runs:
        metric = item.key_metrics or {}
        rows.append(
            {
                "run_id": item.run_id,
                "request_id": item.request_id,
                "chat_id": item.chat_id,
                "symbol": item.symbol,
                "runtime_success": metric.get("runtime_success"),
                "runtime_fallback_used": metric.get("runtime_fallback_used"),
                "runtime_retry_count": metric.get("runtime_retry_count"),
                "runtime_budget_verdict": metric.get("runtime_budget_verdict"),
                "runtime_latency_p95_ms": metric.get("runtime_latency_p95_ms"),
                "runtime_error_rate": metric.get("runtime_error_rate"),
                "latest_close": metric.get("latest_close"),
                "updated_at": item.updated_at,
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True)
    with st.expander("Latest Run Summary / 最新运行摘要", expanded=True):
        latest = runs[0]
        st.markdown(f"**run_id**: `{latest.run_id}`")
        st.markdown(f"**summary**: {latest.summary}")
        st.json(latest.key_metrics)


def _render_alerts_tab(client: FrontendResourceClient) -> None:
    st.subheader("Alerts / 告警")
    alerts = client.list_alerts(limit=120)
    if not alerts:
        st.info("No alert records found. / 当前数据库没有告警记录。")
        return
    df = pd.DataFrame(
        [
            {
                "event_id": item.event_id,
                "symbol": item.symbol,
                "priority": item.priority,
                "rule": item.rule,
                "strategy_tier": item.strategy_tier,
                "tier_guarded": item.tier_guarded,
                "channel": item.channel,
                "status": item.status,
                "run_id": item.run_id,
                "trigger_ts": item.trigger_ts,
                "updated_at": item.updated_at,
                "suppressed_reason": item.suppressed_reason,
                "last_error": item.last_error,
            }
            for item in alerts
        ]
    )
    st.dataframe(df, use_container_width=True)
    counts = df["status"].value_counts().to_dict()
    c1, c2, c3 = st.columns(3)
    c1.metric("Delivered", int(counts.get("delivered", 0)))
    c2.metric("Retry/DLQ", int(counts.get("retry_pending", 0)) + int(counts.get("retrying", 0)) + int(counts.get("dlq", 0)))
    c3.metric("Suppressed", int(counts.get("suppressed", 0)))
    tier_counts = df["strategy_tier"].value_counts().to_dict() if "strategy_tier" in df else {}
    guarded_counts = (
        df[df["tier_guarded"] == True]["strategy_tier"].value_counts().to_dict()  # noqa: E712
        if {"tier_guarded", "strategy_tier"}.issubset(df.columns)
        else {}
    )
    st.caption(f"Tier distribution: {tier_counts} | Guarded tiers: {guarded_counts}")


def _render_evidence_tab(client: FrontendResourceClient) -> None:
    st.subheader("Evidence / 验证证据")
    evidence = client.list_evidence(limit=200)
    if not evidence:
        st.info("No evidence files found in docs/evidence. / 未找到证据文件。")
        return
    df = pd.DataFrame(
        [
            {
                "name": item.name,
                "generated_at": item.generated_at,
                "updated_at": item.updated_at,
                "size_bytes": item.size_bytes,
                "path": item.path,
            }
            for item in evidence
        ]
    )
    st.dataframe(df, use_container_width=True)
    with st.expander("Evidence Quick View / 证据快速预览", expanded=True):
        st.json(evidence[0].summary)


def _render_governance_tab(client: FrontendResourceClient) -> None:
    st.subheader("Governance / 治理状态")
    states = client.list_degradation_states()
    alerts = client.list_alerts(limit=500)
    if not states:
        st.info("No degradation state records found. / 当前没有降级状态记录。")
    else:
        st.dataframe(
            pd.DataFrame([item.model_dump(mode="python") for item in states]),
            use_container_width=True,
        )
    if alerts:
        tier_counts: dict[str, int] = {}
        guarded_counts: dict[str, int] = {}
        for item in alerts:
            tier = item.strategy_tier or "execution-ready"
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
            if item.tier_guarded:
                guarded_counts[tier] = guarded_counts.get(tier, 0) + 1
        st.markdown("**Strategy Tier Governance**")
        c1, c2 = st.columns(2)
        c1.json(tier_counts)
        c2.json(guarded_counts)


def run_console() -> None:
    st.set_page_config(page_title="Alpha-Insight Upgrade7 Console", layout="wide")
    st.title("Alpha-Insight Upgrade7 Console / 升级7控制台")
    client = FrontendResourceClient()
    snapshot = client.build_snapshot(run_limit=10, alert_limit=10, evidence_limit=10)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Runs", len(snapshot.runs))
    c2.metric("Alerts", len(snapshot.alerts))
    c3.metric("Evidence Files", len(snapshot.evidence))
    c4.metric("Degrade States", len(snapshot.degradation_states))
    st.caption(f"store_db={snapshot.db_path}")

    tab_runs, tab_alerts, tab_evidence, tab_governance = st.tabs(
        ["Runs / 运行", "Alerts / 告警", "Evidence / 证据", "Governance / 治理"]
    )
    with tab_runs:
        _render_runs_tab(client)
    with tab_alerts:
        _render_alerts_tab(client)
    with tab_evidence:
        _render_evidence_tab(client)
    with tab_governance:
        _render_governance_tab(client)


if __name__ == "__main__":
    run_console()
