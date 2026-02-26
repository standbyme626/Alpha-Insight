from __future__ import annotations

import sys
import types

import pytest


if "streamlit" not in sys.modules:
    sys.modules["streamlit"] = types.SimpleNamespace(cache_data=lambda *args, **kwargs: (lambda fn: fn))

from ui.llm_frontend import _validate_request_symbol_consistency


def test_validate_request_symbol_consistency_passes_on_match() -> None:
    symbols = _validate_request_symbol_consistency("分析 AAPL 最近一个月走势", "AAPL")
    assert "AAPL" in symbols


def test_validate_request_symbol_consistency_accepts_equivalent_cn_code() -> None:
    symbols = _validate_request_symbol_consistency("分析 600519 最近三个月走势", "600519.SS")
    assert symbols


def test_validate_request_symbol_consistency_fails_on_conflict() -> None:
    with pytest.raises(ValueError):
        _validate_request_symbol_consistency("分析 TSLA 最近一个月走势", "AAPL")
