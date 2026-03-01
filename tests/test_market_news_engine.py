from __future__ import annotations

import pandas as pd
import pytest

from agents.market_news_engine import run_market_news_analysis
from tools.market_data import build_data_bundle


def _sample_records() -> list[dict]:
    df = pd.DataFrame(
        {
            "Date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05", "2026-01-06"],
            "Open": [100, 101, 102, 103, 104, 105],
            "High": [101, 102, 103, 104, 105, 106],
            "Low": [99, 100, 101, 102, 103, 104],
            "Close": [100, 102, 101, 104, 106, 107],
            "Volume": [10, 11, 12, 13, 14, 15],
        }
    )
    return df.to_dict(orient="records")


@pytest.mark.asyncio
async def test_market_news_analysis_reuses_shared_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = build_data_bundle(
        symbol="AAPL",
        period="1mo",
        interval="1d",
        records=_sample_records(),
        data_source="test-bundle",
    )

    async def fail_if_called(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("fetch_market_data should not be called when bundle is provided")

    async def fake_news(  # noqa: ANN001
        _symbol: str,
        *,
        limit: int = 10,
        timeout_seconds: int = 20,
        company_name: str = "",
    ):
        return []

    monkeypatch.setattr("agents.market_news_engine.fetch_market_data", fail_if_called)
    monkeypatch.setattr("agents.market_news_engine.fetch_symbol_news", fake_news)
    monkeypatch.setattr("agents.market_news_engine.get_company_name", lambda symbol, resolve_remote=True: symbol)  # noqa: ARG005

    out = await run_market_news_analysis(
        request="分析 AAPL",
        symbol="AAPL",
        period="1mo",
        market_data_bundle=bundle,
    )

    assert out["market_data_source"] == "test-bundle"
    assert out["market_data_rows"] == 6
    assert "复用共享行情包" in out["analysis_steps"][0]


@pytest.mark.asyncio
async def test_market_news_analysis_bundle_symbol_mismatch_fails_fast() -> None:
    bundle = build_data_bundle(
        symbol="MSFT",
        period="1mo",
        interval="1d",
        records=_sample_records(),
        data_source="test-bundle",
    )
    with pytest.raises(RuntimeError):
        await run_market_news_analysis(
            request="分析 AAPL",
            symbol="AAPL",
            period="1mo",
            market_data_bundle=bundle,
        )
