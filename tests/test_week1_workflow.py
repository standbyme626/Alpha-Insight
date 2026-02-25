from __future__ import annotations

import pytest

from agents.workflow import api_node, planner_node
from core.models import AgentState
from core.sandbox_manager import SandboxManager
from tools.market_data import MarketDataResult


@pytest.mark.asyncio
async def test_planner_routes_news_to_scraper() -> None:
    state = AgentState(request="抓取 Reuters news 并分析", symbol="AAPL")
    planned = await planner_node(state.model_dump())
    assert planned["route"] == "scraper"


@pytest.mark.asyncio
async def test_api_node_fallback_to_scraper(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch_market_data(symbol: str, period: str = "1mo") -> MarketDataResult:
        return MarketDataResult(ok=False, symbol=symbol, message="数据未找到", records=[])

    monkeypatch.setattr("agents.workflow.fetch_market_data", fake_fetch_market_data)

    result = await api_node({"symbol": "AAPL", "period": "1mo", "fallback_url": "https://example.com"})
    assert result["route"] == "scraper"


def test_parse_traceback_structured() -> None:
    stderr = """Traceback (most recent call last):
  File \"/tmp/a.py\", line 4, in <module>
    1 / 0
ZeroDivisionError: division by zero
"""
    parsed = SandboxManager._parse_traceback(stderr)
    assert parsed is not None
    assert parsed.error_type == "ZeroDivisionError"
    assert parsed.frames[0]["line"] == 4
