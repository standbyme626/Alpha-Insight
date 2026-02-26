from __future__ import annotations

import pandas as pd
import pytest

from tools.market_data import (
    _normalize_dataframe,
    fetch_market_data,
    get_cn_top100_constituents,
    get_cn_top100_watchlist,
    normalize_market_symbol,
)


def test_normalize_dataframe_sets_utc_and_fillna() -> None:
    df = pd.DataFrame(
        {
            "Date": ["2026-01-01", "2026-01-02"],
            "Close": [1.0, None],
            "Open": [None, 2.0],
        }
    )
    out = _normalize_dataframe(df)
    assert str(out["Date"].dtype).startswith("datetime64")
    assert out["Close"].isna().sum() == 0
    assert out["Open"].isna().sum() == 0


@pytest.mark.asyncio
async def test_fetch_market_data_empty_symbol() -> None:
    result = await fetch_market_data("   ")
    assert result.ok is False
    assert "数据未找到" in result.message


def test_normalize_market_symbol_cn_company_name() -> None:
    assert normalize_market_symbol("贵州茅台", market="cn") == "600519.SS"


def test_normalize_market_symbol_cn_numeric_code() -> None:
    assert normalize_market_symbol("600519", market="cn") == "600519.SS"
    assert normalize_market_symbol("000001", market="cn") == "000001.SZ"


def test_normalize_market_symbol_hk_numeric_code() -> None:
    assert normalize_market_symbol("881", market="hk") == "0881.HK"
    assert normalize_market_symbol("0881", market="hk") == "0881.HK"


def test_cn_top100_watchlist_size() -> None:
    watchlist = get_cn_top100_watchlist()
    assert len(watchlist) >= 100


def test_cn_top100_constituents_include_company_name() -> None:
    constituents = get_cn_top100_constituents()
    assert constituents[0]["symbol"] == "600519.SS"
    assert constituents[0]["name"]
