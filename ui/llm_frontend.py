"""Frontend UI: planner console with market-aware examples."""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

# Ensure imports work even when streamlit is launched outside repo root.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from agents.planner_engine import plan_tasks


def _mask_key(key: str) -> str:
    if len(key) < 8:
        return "***"
    return key[:4] + "***" + key[-4:]


def _run_planner(request: str) -> dict:
    result = asyncio.run(plan_tasks(request))
    return {
        "provider": result.provider,
        "data_source": result.data_source,
        "steps": result.steps,
        "reason": result.reason,
    }


def main() -> None:
    st.set_page_config(page_title="Alpha-Insight Planner Console / 规划控制台", layout="wide")
    st.title("Alpha-Insight Planner Console / 规划控制台")
    st.caption("Use this page to inspect planner decomposition and data-source routing / 查看任务拆解和数据源路由。")

    with st.sidebar:
        st.header("Runtime Config / 运行配置")
        api_base = os.getenv("OPENAI_API_BASE", "")
        model = os.getenv("OPENAI_MODEL_NAME", "")
        key = os.getenv("OPENAI_API_KEY", "")
        fallback = os.getenv("ENABLE_LOCAL_FALLBACK", "true")
        temp = os.getenv("TEMPERATURE", "0.0")
        st.text(f"API Base: {api_base}")
        st.text(f"Model: {model}")
        st.text(f"Key: {_mask_key(key) if key else '(missing)'}")
        st.text(f"Fallback: {fallback}")
        st.text(f"Temperature: {temp}")
        st.markdown("---")
        st.markdown("**Quick Start / 快速开始**")
        st.markdown("1. Select a request template / 选择示例请求")
        st.markdown("2. Click `Run Planner / 运行规划`")
        st.markdown("3. Review data source + steps / 查看数据源和步骤")

    st.subheader("Planner Input / 规划输入")
    templates = {
        "US Tech (AAPL)": "分析 AAPL 最近一个月走势，给出规划步骤",
        "CN A-Share (贵州茅台)": "分析 贵州茅台 最近三个月走势并给出执行计划",
        "CN Numeric (600519)": "分析 600519 最近三个月走势并说明应使用 API 还是网页抓取",
    }
    selected_template = st.selectbox("Request Template / 请求模板", list(templates.keys()), index=0)
    request = st.text_area("Request / 请求", value=templates[selected_template], height=120)
    run_btn = st.button("Run Planner / 运行规划")

    if "history" not in st.session_state:
        st.session_state.history = []

    if run_btn:
        if not request.strip():
            st.error("Request is empty / 请求不能为空。")
        else:
            with st.spinner("Calling remote LLM / 正在调用远程模型..."):
                try:
                    output = _run_planner(request)
                    record = {
                        "time": datetime.utcnow().isoformat() + "Z",
                        "request": request,
                        **output,
                    }
                    st.session_state.history.insert(0, record)
                    st.success("Planner completed / 规划完成。")
                except Exception as exc:
                    st.error(f"Planner failed / 规划失败: {exc}")

    if st.session_state.history:
        st.subheader("Latest Result / 最新结果")
        latest = st.session_state.history[0]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Provider / 模型", latest["provider"])
        with c2:
            st.metric("Data Source / 数据源", latest["data_source"])
        with c3:
            st.metric("Steps Count / 步骤数", len(latest["steps"]))
        st.write("Steps / 执行步骤")
        st.code("\n".join(latest["steps"]), language="text")
        st.write("Reason / 理由")
        st.info(latest["reason"])

        st.subheader("Run History / 运行历史")
        st.dataframe(st.session_state.history, use_container_width=True)


if __name__ == "__main__":
    main()
