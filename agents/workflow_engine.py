"""Week2 LangGraph workflow: Planner -> Coder -> Executor -> Debugger(loop)."""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from agents.coder_engine import generate_code
from agents.debugger_engine import build_debug_advice
from agents.planner_engine import plan_tasks
from core.sandbox_manager import SandboxManager
from tools.market_data import fetch_market_data


class Week2GraphState(TypedDict, total=False):
    request: str
    symbol: str
    period: str
    interval: str

    plan_steps: list[str]
    data_source: str
    planner_reason: str
    planner_provider: str
    market_data_bundle: dict[str, Any]
    data_fetch_message: str

    sandbox_code: str
    sandbox_stdout: str
    sandbox_stderr: str
    sandbox_backend: str
    traceback: dict[str, Any] | None

    debug_advice: str
    retry_count: int
    max_retries: int
    success: bool
    inject_failure: bool


async def planner_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.planner_node Start")
    plan = await plan_tasks(state.get("request", ""))
    return {
        "plan_steps": plan.steps,
        "data_source": plan.data_source,
        "planner_reason": plan.reason,
        "planner_provider": plan.provider,
        "interval": str(state.get("interval", "1d")),
        "retry_count": int(state.get("retry_count", 0)),
        "max_retries": int(state.get("max_retries", 2)),
    }


async def market_data_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.market_data_node Start")
    symbol = str(state.get("symbol", "AAPL"))
    period = str(state.get("period", "1mo"))
    interval = str(state.get("interval", "1d"))
    result = await fetch_market_data(symbol, period=period, interval=interval)

    if not result.ok or not result.records:
        return {
            "data_fetch_message": result.message,
            "traceback": {
                "error_type": "DataFetchError",
                "message": result.message,
                "frames": [],
                "raw": result.message,
            },
            "success": False,
            "sandbox_stdout": "",
            "sandbox_stderr": result.message,
        }

    bundle_payload = result.bundle.to_serializable_dict() if result.bundle else {
        "records": result.records,
        "metadata": {"period": period, "record_count": len(result.records)},
        "data_source": "api",
        "symbol": result.symbol or symbol,
        "market": "auto",
        "interval": interval,
    }
    return {
        "market_data_bundle": bundle_payload,
        "data_fetch_message": result.message,
        "traceback": None,
    }


async def coder_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.coder_node Start")
    if state.get("traceback"):
        return {}
    code = generate_code(state)
    return {"sandbox_code": code}


async def executor_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.executor_node Start")
    code = state.get("sandbox_code", "")
    manager = SandboxManager()

    await manager.create_session()
    try:
        result = await manager.execute(code)
    finally:
        await manager.destroy_session()

    tb = None
    if result.traceback:
        tb = {
            "error_type": result.traceback.error_type,
            "message": result.traceback.message,
            "frames": result.traceback.frames,
            "raw": result.traceback.raw,
        }

    retry_count = int(state.get("retry_count", 0))
    if tb:
        retry_count += 1

    return {
        "sandbox_stdout": result.stdout,
        "sandbox_stderr": result.stderr,
        "sandbox_backend": str(getattr(result, "execution_backend", "unknown")),
        "traceback": tb,
        "retry_count": retry_count,
        "success": tb is None,
    }


async def debugger_node(state: Week2GraphState) -> Week2GraphState:
    print("[DEBUG] QuantNode week2.debugger_node Start")
    advice = build_debug_advice(state.get("traceback"))
    return {"debug_advice": advice}


def _after_market_data(state: Week2GraphState) -> str:
    if state.get("traceback") is not None:
        return "done"
    return "coder"


def _after_executor(state: Week2GraphState) -> str:
    if state.get("traceback") is None:
        return "done"

    retry_count = int(state.get("retry_count", 0))
    max_retries = int(state.get("max_retries", 2))
    if retry_count < max_retries:
        return "debugger"
    return "done"


def build_repair_graph(*, checkpointer: InMemorySaver | None = None):
    print("[DEBUG] QuantNode week2.build_week2_graph Start")
    graph = StateGraph(Week2GraphState)
    graph.add_node("planner", planner_node)
    graph.add_node("market_data", market_data_node)
    graph.add_node("coder", coder_node)
    graph.add_node("executor", executor_node)
    graph.add_node("debugger", debugger_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "market_data")
    graph.add_conditional_edges(
        "market_data",
        _after_market_data,
        {
            "coder": "coder",
            "done": END,
        },
    )
    graph.add_edge("coder", "executor")
    graph.add_conditional_edges(
        "executor",
        _after_executor,
        {
            "debugger": "debugger",
            "done": END,
        },
    )
    graph.add_edge("debugger", "coder")

    cp = checkpointer if checkpointer is not None else InMemorySaver()
    return graph.compile(checkpointer=cp)


# Backward-compatible alias.
build_week2_graph = build_repair_graph
