"""Week3 LangGraph workflow: quant intelligence + multimodal report + HITL."""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph
from langgraph.types import interrupt

from agents.report_coder import build_report_code
from agents.report_reviewer import build_markdown_report, extract_metrics_from_stdout
from core.sandbox_manager import SandboxManager
from tools.artifact_extractor import build_transfer_payload


class Week3GraphState(TypedDict, total=False):
    request: str
    symbol: str
    period: str

    sentiment_score: float
    sentiment_text: str

    sandbox_code: str
    sandbox_stdout: str
    sandbox_stderr: str
    traceback: dict[str, Any] | None
    output_files: list[str]

    metrics: dict[str, Any]
    metrics_history: list[dict[str, Any]]
    report_markdown: str
    transfer_payload: list[dict[str, str]]

    recommendation: str
    hitl_status: str
    human_approved: bool


async def planner_node(state: Week3GraphState) -> Week3GraphState:
    print("[DEBUG] QuantNode week3.planner_node Start")
    history = list(state.get("metrics_history", []))
    return {
        "symbol": str(state.get("symbol", "AAPL")),
        "period": str(state.get("period", "6mo")),
        "sentiment_score": float(state.get("sentiment_score", 50.0)),
        "sentiment_text": str(state.get("sentiment_text", "")),
        "metrics_history": history,
    }


async def coder_node(state: Week3GraphState) -> Week3GraphState:
    print("[DEBUG] QuantNode week3.coder_node Start")
    return {"sandbox_code": build_report_code(state)}


async def executor_node(state: Week3GraphState) -> Week3GraphState:
    print("[DEBUG] QuantNode week3.executor_node Start")
    manager = SandboxManager()
    await manager.create_session()
    try:
        result = await manager.execute(state.get("sandbox_code", ""))
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

    return {
        "sandbox_stdout": result.stdout,
        "sandbox_stderr": result.stderr,
        "traceback": tb,
        "output_files": result.output_files,
    }


async def reviewer_node(state: Week3GraphState) -> Week3GraphState:
    print("[DEBUG] QuantNode week3.reviewer_node Start")
    metrics = extract_metrics_from_stdout(state.get("sandbox_stdout", ""))
    if metrics is None:
        metrics = {
            "symbol": state.get("symbol", "AAPL"),
            "recommendation": "HOLD",
            "fused_score": 0,
            "technical_score": 0,
            "sentiment_score": state.get("sentiment_score", 50),
            "strategy_return": 0,
            "benchmark_return": 0,
            "win_rate": 0,
            "max_drawdown": 0,
        }

    report_markdown = build_markdown_report(metrics, sentiment_text=state.get("sentiment_text"))

    output_files = state.get("output_files", [])
    transferable = [path for path in output_files if path.lower().endswith((".html", ".png", ".pdf"))]
    transfer_payload = build_transfer_payload(transferable)

    history = list(state.get("metrics_history", []))
    history.append(metrics)

    return {
        "metrics": metrics,
        "metrics_history": history,
        "report_markdown": report_markdown,
        "transfer_payload": transfer_payload,
        "recommendation": str(metrics.get("recommendation", "HOLD")),
    }


async def hitl_node(state: Week3GraphState) -> Week3GraphState:
    print("[DEBUG] QuantNode week3.hitl_node Start")
    recommendation = str(state.get("recommendation", "HOLD")).upper()
    if recommendation != "BUY":
        return {"hitl_status": "not_required", "human_approved": True}

    approved = interrupt(
        {
            "type": "approval_request",
            "message": "策略建议 BUY，是否确认发送买入信号？",
            "symbol": state.get("symbol", "AAPL"),
            "fused_score": state.get("metrics", {}).get("fused_score"),
        }
    )
    approved_bool = bool(approved)
    return {
        "hitl_status": "approved" if approved_bool else "rejected",
        "human_approved": approved_bool,
    }


def _route_after_executor(state: Week3GraphState) -> str:
    return "reviewer" if state.get("traceback") is None else "done"


def build_report_graph(*, checkpointer: InMemorySaver | None = None):
    print("[DEBUG] QuantNode week3.build_week3_graph Start")
    graph = StateGraph(Week3GraphState)
    graph.add_node("planner", planner_node)
    graph.add_node("coder", coder_node)
    graph.add_node("executor", executor_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("hitl", hitl_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "coder")
    graph.add_edge("coder", "executor")
    graph.add_conditional_edges(
        "executor",
        _route_after_executor,
        {
            "reviewer": "reviewer",
            "done": END,
        },
    )
    graph.add_edge("reviewer", "hitl")
    graph.add_edge("hitl", END)

    cp = checkpointer if checkpointer is not None else InMemorySaver()
    return graph.compile(checkpointer=cp)


# Backward-compatible alias.
build_week3_graph = build_report_graph
