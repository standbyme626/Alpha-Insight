from __future__ import annotations

import pandas as pd
import pytest

from agents.scanner_engine import ScanConfig, _analyze_symbol_with_diagnostics
from core.tool_result import ToolResult
from tools.market_data import MarketDataResult, build_data_bundle, market_data_result_to_tool_result
from tools.news_data import fetch_symbol_news, fetch_symbol_news_tool_result


def _sample_records() -> list[dict]:
    frame = pd.DataFrame(
        {
            "Date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "Open": [100, 101, 102],
            "High": [101, 102, 103],
            "Low": [99, 100, 101],
            "Close": [100, 102, 101],
            "Volume": [10, 11, 12],
        }
    )
    return frame.to_dict(orient="records")


def test_market_tool_result_contract_and_legacy_adapter() -> None:
    bundle = build_data_bundle(
        symbol="AAPL",
        period="1mo",
        interval="1d",
        records=_sample_records(),
        data_source="fixture",
    )
    result = MarketDataResult(ok=True, symbol="AAPL", message="ok", records=bundle.records, bundle=bundle)

    tool_result = market_data_result_to_tool_result(result, period="1mo", interval="1d")
    assert isinstance(tool_result, ToolResult)
    assert tool_result.source == "market_data:yfinance"
    assert tool_result.error == ""
    assert isinstance(tool_result.ts, str) and tool_result.ts
    assert tool_result.confidence > 0
    assert isinstance(tool_result.raw, dict)


@pytest.mark.asyncio
async def test_news_tool_result_contract_and_legacy_list_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_fetch(_session, _url: str, *, source: str, limit: int):  # noqa: ANN001
        return [
            {
                "title": f"{source}-news",
                "link": f"https://example.com/{source}",
                "published_at": "2026-02-27T11:00:00+00:00",
                "summary": "ok",
                "source": source,
            }
        ][:limit]

    monkeypatch.setattr("tools.news_data._fetch_rss", fake_fetch)
    tool_result = await fetch_symbol_news_tool_result("AAPL", limit=3, company_name="Apple")
    rows = await fetch_symbol_news("AAPL", limit=3, company_name="Apple")

    assert tool_result.source == "news:rss"
    assert tool_result.error == ""
    assert isinstance(tool_result.raw, list)
    assert rows == tool_result.raw


@pytest.mark.asyncio
async def test_scanner_chain_emits_market_tool_result_on_failure() -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        return MarketDataResult(ok=False, symbol=symbol, message="upstream unavailable", records=[])

    analysis = await _analyze_symbol_with_diagnostics(
        symbol="AAPL",
        config=ScanConfig(watchlist=["AAPL"]),
        fetcher=fake_fetch,
    )

    assert analysis.signal is None
    assert analysis.failure is not None
    assert isinstance(analysis.market_tool_result, dict)
    assert analysis.market_tool_result.get("source") == "market_data:yfinance"
    assert analysis.market_tool_result.get("error")
