"""Week1 integration runner: request -> planner -> api/scraper -> sandbox."""

from __future__ import annotations

import asyncio

from agents.workflow import build_week1_graph, to_agent_state


async def main() -> None:
    graph = build_week1_graph()
    initial_state = {
        "request": "分析 AAPL 最近走势并计算均线",
        "symbol": "AAPL",
        "period": "1mo",
        "fallback_url": "https://www.reuters.com",
    }
    result = await graph.ainvoke(initial_state)
    state = to_agent_state(result, request=initial_state["request"])

    print("[INFO] Route:", state.route)
    print("[INFO] Market rows:", len(state.market_data))
    if state.scraped_data:
        print("[INFO] Scraped text sample:", state.scraped_data[:200])
    if state.sandbox_stdout:
        print("[INFO] Sandbox stdout:")
        print(state.sandbox_stdout.strip())
    if state.traceback:
        print("[WARN] Traceback:", state.traceback.error_type, state.traceback.message)


if __name__ == "__main__":
    asyncio.run(main())
