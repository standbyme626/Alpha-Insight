from __future__ import annotations

import pandas as pd
import pytest

from tools.market_data import _normalize_dataframe, fetch_market_data


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
