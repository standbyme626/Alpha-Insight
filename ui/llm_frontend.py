"""Frontend UI: real online planner test console."""

from __future__ import annotations

import asyncio
import os
from datetime import datetime

import streamlit as st

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
    st.set_page_config(page_title="Alpha-Insight LLM Console", layout="wide")
    st.title("Alpha-Insight Real LLM Planner Console")

    with st.sidebar:
        st.header("Runtime Config")
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

    st.subheader("Planner Input")
    default_request = "分析 AAPL 最近一个月走势，给出规划步骤"
    request = st.text_area("Request", value=default_request, height=120)
    run_btn = st.button("Run Real LLM Planner")

    if "history" not in st.session_state:
        st.session_state.history = []

    if run_btn:
        if not request.strip():
            st.error("Request is empty.")
        else:
            with st.spinner("Calling remote LLM..."):
                try:
                    output = _run_planner(request)
                    record = {
                        "time": datetime.utcnow().isoformat() + "Z",
                        "request": request,
                        **output,
                    }
                    st.session_state.history.insert(0, record)
                    st.success("Planner completed.")
                except Exception as exc:
                    st.error(f"Planner failed: {exc}")

    if st.session_state.history:
        st.subheader("Latest Result")
        latest = st.session_state.history[0]
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Provider", latest["provider"])
            st.metric("Data Source", latest["data_source"])
        with c2:
            st.write("Steps")
            st.code("\n".join(latest["steps"]), language="text")
        st.write("Reason")
        st.info(latest["reason"])

        st.subheader("Run History")
        st.dataframe(st.session_state.history, use_container_width=True)


if __name__ == "__main__":
    main()
