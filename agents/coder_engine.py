"""Week2 coder: generate sandbox-first quant code."""

from __future__ import annotations

import json
from typing import Any


CODER_STYLE_GUIDE = """
1. Always import pandas as pd.
2. Normalize Date to UTC when available.
3. All numeric outputs must be computed in sandbox Python, never in LLM text.
4. Keep variable names deterministic: df, summary, metrics.
5. Never use network calls or dynamic package installation inside sandbox code.
""".strip()


def _base_script(bundle: dict[str, Any], *, bad_column: bool = False) -> str:
    ma_source_col = "Clsoe" if bad_column else "Close"
    payload_json = json.dumps(bundle, ensure_ascii=False, sort_keys=True)
    return f"""
import json
import pandas as pd

bundle = json.loads({payload_json!r})
records = bundle.get("records", [])
if not records:
    raise ValueError("数据未找到")

df = pd.DataFrame(records)
if df.empty:
    raise ValueError("DataBundle records 为空")

if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")

for col in ["Open", "High", "Low", "Close", "Volume"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.fillna(0)
df["MA5"] = df[{ma_source_col!r}].rolling(5).mean().fillna(0)
summary = df.tail(1)[["Date", "Close", "MA5"]]
print(
    "bundle_meta"
    f" source={{bundle.get('data_source','')}}"
    f" asof={{bundle.get('asof','')}}"
    f" symbol={{bundle.get('symbol','')}}"
    f" market={{bundle.get('market','')}}"
    f" interval={{bundle.get('interval','')}}"
    f" rows={{len(records)}}"
)
print(summary.to_string(index=False))
""".strip()


def generate_code(state: dict[str, Any]) -> str:
    bundle = state.get("market_data_bundle")
    if not isinstance(bundle, dict):
        bundle = {
            "records": [],
            "metadata": {"period": str(state.get("period", "1mo")), "record_count": 0},
            "data_source": "api",
            "symbol": str(state.get("symbol", "AAPL")),
            "market": "auto",
            "interval": str(state.get("interval", "1d")),
            "asof": "",
        }

    # Test hook: force a bad first attempt to validate self-correction loop.
    inject_failure = bool(state.get("inject_failure", False))
    retry_count = int(state.get("retry_count", 0))

    if inject_failure and retry_count == 0:
        return _base_script(bundle, bad_column=True)

    debug_advice = str(state.get("debug_advice") or "")
    if "use_close_column" in debug_advice:
        return _base_script(bundle, bad_column=False)

    return _base_script(bundle, bad_column=False)
