"""Week1 LangGraph workflow: Planner -> (API or Scraper) -> Sandbox."""

from __future__ import annotations

import json
from typing import TypedDict

import pandas as pd
from langgraph.graph import END, StateGraph

from agents.planner import plan_route
from agents.scraper import scrape_web
from core.models import AgentState, TraceFrame, TracebackInfo
from core.sandbox_manager import SandboxManager
from tools.market_data import fetch_market_data


class GraphState(TypedDict, total=False):
    request: str
    symbol: str
    period: str
    fallback_url: str | None
    route: str
    market_data: list[dict]
    scraped_data: str | None
    sandbox_code: str | None
    sandbox_stdout: str | None
    sandbox_stderr: str | None
    traceback: dict | None


def _build_ma_code(records: list[dict]) -> str:
    payload = json.dumps(records, default=str)
    return f"""
import json
import pandas as pd

records = json.loads({payload!r})
df = pd.DataFrame(records)
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
for col in ["Close", "Open", "High", "Low"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
df = df.fillna(0)
if "Close" in df.columns and not df.empty:
    df["MA5"] = df["Close"].rolling(5).mean().fillna(0)
print(df.tail(1).to_string(index=False))
"""


async def planner_node(state: GraphState) -> GraphState:
    print("[DEBUG] QuantNode planner_node Start")
    planned = plan_route(AgentState(**state))
    return {"route": planned.route}


async def api_node(state: GraphState) -> GraphState:
    print("[DEBUG] QuantNode api_node Start")
    result = await fetch_market_data(state.get("symbol", "AAPL"), state.get("period", "1mo"))
    if not result.ok:
        fallback_url = state.get("fallback_url")
        if fallback_url:
            return {"route": "scraper", "market_data": []}
        return {"route": "done", "market_data": []}
    return {"route": "done", "market_data": result.records}


async def scraper_node(state: GraphState) -> GraphState:
    print("[DEBUG] QuantNode scraper_node Start")
    url = state.get("fallback_url")
    if not url:
        return {"route": "done", "scraped_data": "数据未找到"}
    output = await scrape_web(url)
    content = output.markdown if output.ok else "数据未找到"
    return {"route": "done", "scraped_data": content}


async def sandbox_node(state: GraphState) -> GraphState:
    print("[DEBUG] QuantNode sandbox_node Start")
    rows = state.get("market_data") or []
    if not rows:
        return {}

    code = _build_ma_code(rows)
    manager = SandboxManager()
    await manager.create_session()
    result = await manager.execute(code)
    await manager.destroy_session()

    trace_payload = None
    if result.traceback:
        trace_payload = {
            "error_type": result.traceback.error_type,
            "message": result.traceback.message,
            "frames": result.traceback.frames,
            "raw": result.traceback.raw,
        }

    return {
        "sandbox_code": code,
        "sandbox_stdout": result.stdout,
        "sandbox_stderr": result.stderr,
        "traceback": trace_payload,
    }


def _route_from_planner(state: GraphState) -> str:
    return state.get("route", "api")


def build_week1_graph():
    print("[DEBUG] QuantNode build_week1_graph Start")
    graph = StateGraph(GraphState)
    graph.add_node("planner", planner_node)
    graph.add_node("api", api_node)
    graph.add_node("scraper", scraper_node)
    graph.add_node("sandbox", sandbox_node)

    graph.set_entry_point("planner")
    graph.add_conditional_edges(
        "planner",
        _route_from_planner,
        {
            "api": "api",
            "scraper": "scraper",
        },
    )
    graph.add_conditional_edges(
        "api",
        _route_from_planner,
        {
            "scraper": "scraper",
            "done": "sandbox",
        },
    )
    graph.add_edge("scraper", END)
    graph.add_edge("sandbox", END)
    return graph.compile()


def to_agent_state(result: GraphState, request: str) -> AgentState:
    trace_info = None
    if result.get("traceback"):
        payload = result["traceback"]
        frames = [TraceFrame(**frame) for frame in payload.get("frames", [])]
        trace_info = TracebackInfo(
            error_type=payload.get("error_type", "ExecutionError"),
            message=payload.get("message", ""),
            frames=frames,
            raw=payload.get("raw", ""),
        )

    state = AgentState(
        request=request,
        symbol=result.get("symbol", "AAPL"),
        period=result.get("period", "1mo"),
        fallback_url=result.get("fallback_url"),
        route=result.get("route", "done"),
        market_data=result.get("market_data", []),
        scraped_data=result.get("scraped_data"),
        sandbox_code=result.get("sandbox_code"),
        sandbox_stdout=result.get("sandbox_stdout"),
        sandbox_stderr=result.get("sandbox_stderr"),
        traceback=trace_info,
    )

    if state.market_data:
        # Keep an explicit UTC normalization checkpoint for auditability.
        df = pd.DataFrame(state.market_data)
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
        state.market_data = df.fillna(0).to_dict(orient="records")

    return state
