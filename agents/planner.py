"""Planner node for Week1 routing."""

from __future__ import annotations

from core.models import AgentState


SCRAPER_KEYWORDS = (
    "新闻",
    "news",
    "公告",
    "财报",
    "twitter",
    "reuters",
    "网页",
)


def plan_route(state: AgentState) -> AgentState:
    print("[DEBUG] QuantNode planner.plan_route Start")
    request_lower = state.request.lower()

    if any(keyword in request_lower for keyword in SCRAPER_KEYWORDS):
        state.route = "scraper"
        return state

    state.route = "api"
    return state
