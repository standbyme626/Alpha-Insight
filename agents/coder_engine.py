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


def _base_script(bundle: dict[str, Any], *, bad_column: bool = False, render_chart: bool = False) -> str:
    ma_source_col = "Clsoe" if bad_column else "Close"
    chart_block = ""
    if render_chart:
        chart_block = """
from pathlib import Path

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    out_dir = Path("storage/outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    symbol = str(bundle.get("symbol", "UNKNOWN")).replace("/", "_")
    chart_path = out_dir / f"chart_{symbol}_{pd.Timestamp.utcnow().strftime('%Y%m%d%H%M%S%f')}.png"
    close_series = pd.to_numeric(df["Close"], errors="coerce").fillna(0)
    plt.figure(figsize=(9, 4.5))
    plt.plot(close_series.index, close_series.values, linewidth=1.6)
    plt.title(f"{symbol} close trend")
    plt.xlabel("index")
    plt.ylabel("price")
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()
    print(f"ARTIFACT_PNG={chart_path.resolve()}")
except Exception as chart_exc:  # keep analysis result even if chart rendering fails
    print(f"CHART_RENDER_ERROR={type(chart_exc).__name__}:{chart_exc}")
""".rstrip()
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
{chart_block}
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
    need_chart = bool(state.get("need_chart", False))
    if not need_chart:
        request = str(state.get("request", "")).lower()
        need_chart = "need_chart=true" in request

    if inject_failure and retry_count == 0:
        return _base_script(bundle, bad_column=True, render_chart=need_chart)

    debug_advice = str(state.get("debug_advice") or "")
    if "use_close_column" in debug_advice:
        return _base_script(bundle, bad_column=False, render_chart=need_chart)

    return _base_script(bundle, bad_column=False, render_chart=need_chart)
