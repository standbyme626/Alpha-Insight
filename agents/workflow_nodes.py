"""Composable node registry for Week2 repair workflow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

WorkflowState = dict[str, Any]
NodeCallable = Callable[[WorkflowState], Awaitable[WorkflowState]]
DecisionCallable = Callable[[WorkflowState], str]


@dataclass(frozen=True)
class WorkflowNodes:
    planner: NodeCallable
    market_data: NodeCallable
    coder: NodeCallable
    executor: NodeCallable
    debugger: NodeCallable


@dataclass(frozen=True)
class WorkflowDecisions:
    after_market_data: DecisionCallable
    after_executor: DecisionCallable
