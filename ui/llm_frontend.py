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
from agents.workflow_engine import build_week2_graph


def _load_local_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


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


def _run_full_analysis(request: str, symbol: str, period: str) -> dict:
    app = build_week2_graph()
    output = asyncio.run(
        app.ainvoke(
            {
                "request": request,
                "symbol": symbol.strip().upper(),
                "period": period.strip(),
                "max_retries": 2,
            },
            config={"configurable": {"thread_id": f"ui-full-{datetime.utcnow().timestamp()}"}},
        )
    )
    return {
        "success": bool(output.get("success", False)),
        "planner_provider": str(output.get("planner_provider", "")),
        "planner_reason": str(output.get("planner_reason", "")),
        "data_source": str(output.get("data_source", "")),
        "plan_steps": [str(step) for step in output.get("plan_steps", [])],
        "sandbox_code": str(output.get("sandbox_code", "")),
        "sandbox_stdout": str(output.get("sandbox_stdout", "")),
        "sandbox_stderr": str(output.get("sandbox_stderr", "")),
        "traceback": output.get("traceback"),
        "retry_count": int(output.get("retry_count", 0)),
    }


def _to_bilingual_step(step: str) -> str:
    mapping = {
        "Data Fetch": "Data Fetch / 数据获取",
        "Logic Calc": "Logic Calc / 逻辑计算",
        "Plotting": "Plotting / 绘图展示",
    }
    return mapping.get(step, step)


def main() -> None:
    _load_local_env()
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
        st.text(f"API Base / 接口地址: {api_base}")
        st.text(f"Model / 模型: {model}")
        st.text(f"Key / 密钥: {_mask_key(key) if key else '(missing/缺失)'}")
        st.text(f"Fallback / 本地回退: {fallback}")
        st.text(f"Temperature / 温度: {temp}")
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
    template_symbol = {
        "US Tech (AAPL)": "AAPL",
        "CN A-Share (贵州茅台)": "600519.SS",
        "CN Numeric (600519)": "600519.SS",
    }
    selected_template = st.selectbox("Request Template / 请求模板", list(templates.keys()), index=0)
    request = st.text_area("Request / 请求", value=templates[selected_template], height=120)
    symbol = st.text_input("Symbol / 标的代码", value=template_symbol[selected_template])
    period = st.selectbox("Period / 时间区间", ["1mo", "3mo", "6mo"], index=0)
    run_btn = st.button("Run Planner / 运行规划")
    run_full_btn = st.button("Run Full Analysis / 运行完整分析")

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

    if run_full_btn:
        if not request.strip() or not symbol.strip():
            st.error("Request/Symbol is empty / 请求或代码不能为空。")
        else:
            with st.spinner("Running full workflow in sandbox / 正在执行完整分析工作流..."):
                try:
                    full_output = _run_full_analysis(request, symbol, period)
                    record = {
                        "time": datetime.utcnow().isoformat() + "Z",
                        "request": request,
                        "provider": full_output["planner_provider"] or "unknown",
                        "data_source": full_output["data_source"] or "unknown",
                        "steps": full_output["plan_steps"] or [],
                        "reason": full_output["planner_reason"] or "",
                        "full_analysis": full_output,
                    }
                    st.session_state.history.insert(0, record)
                    st.success("Full analysis completed / 完整分析完成。")
                except Exception as exc:
                    st.error(f"Full analysis failed / 完整分析失败: {exc}")

    if st.session_state.history:
        st.subheader("Latest Result / 最新结果")
        latest = st.session_state.history[0]
        if latest["provider"] == "fallback":
            st.warning(
                "Planner is using local fallback / 当前为本地回退规划。"
                " Please check OPENAI_API_KEY, OPENAI_API_BASE, OPENAI_MODEL_NAME / "
                "请检查以上环境变量是否在当前进程生效。"
            )
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Provider / 模型", latest["provider"])
        with c2:
            st.metric("Data Source / 数据源", latest["data_source"])
        with c3:
            st.metric("Steps Count / 步骤数", len(latest["steps"]))
        st.write("Steps / 执行步骤")
        st.code("\n".join(_to_bilingual_step(step) for step in latest["steps"]), language="text")
        st.write("Reason / 理由")
        st.info(latest["reason"])

        st.subheader("Run History / 运行历史")
        st.dataframe(st.session_state.history, use_container_width=True)

        full = latest.get("full_analysis")
        if isinstance(full, dict):
            st.subheader("Full Analysis Artifacts / 完整分析产物")
            c1, c2, c3 = st.columns(3)
            c1.metric("Success / 成功", "Yes / 是" if full.get("success") else "No / 否")
            c2.metric("Retry Count / 重试次数", int(full.get("retry_count", 0)))
            c3.metric("Data Source / 数据源", full.get("data_source", ""))
            st.markdown("**Sandbox Code / 沙箱代码**")
            st.code(full.get("sandbox_code", ""), language="python")
            st.markdown("**Sandbox Stdout / 沙箱标准输出**")
            st.code(full.get("sandbox_stdout", "") or "(empty / 空)", language="text")
            st.markdown("**Sandbox Stderr / 沙箱错误输出**")
            st.code(full.get("sandbox_stderr", "") or "(empty / 空)", language="text")
            st.markdown("**Traceback / 异常回溯**")
            st.code(str(full.get("traceback") or "(none / 无)"), language="text")


if __name__ == "__main__":
    main()
