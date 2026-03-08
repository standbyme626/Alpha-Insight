"""Graph assembly for Week2 repair workflow."""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, StateGraph

from agents.workflow_nodes import WorkflowDecisions, WorkflowNodes


def build_repair_graph(
    *,
    state_type: type[Any],
    nodes: WorkflowNodes,
    decisions: WorkflowDecisions,
    checkpointer: InMemorySaver | None = None,
):
    graph = StateGraph(state_type)
    graph.add_node("planner", nodes.planner)
    graph.add_node("market_data", nodes.market_data)
    graph.add_node("coder", nodes.coder)
    graph.add_node("executor", nodes.executor)
    graph.add_node("debugger", nodes.debugger)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "market_data")
    graph.add_conditional_edges(
        "market_data",
        decisions.after_market_data,
        {
            "coder": "coder",
            "done": END,
        },
    )
    graph.add_edge("coder", "executor")
    graph.add_conditional_edges(
        "executor",
        decisions.after_executor,
        {
            "debugger": "debugger",
            "done": END,
        },
    )
    graph.add_edge("debugger", "coder")

    cp = checkpointer if checkpointer is not None else InMemorySaver()
    return graph.compile(checkpointer=cp)
