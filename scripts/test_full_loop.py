"""Integration test: host fetch market data -> offline Docker sandbox compute."""

from __future__ import annotations

import asyncio
import json

from core.sandbox import LocalDockerSandbox
from tools.market_data import fetch_market_data


def _build_sandbox_code(bundle_payload: dict) -> str:
    """Build Python code string to run inside sandbox."""
    payload_json = json.dumps(bundle_payload, ensure_ascii=False, sort_keys=True)
    return f"""
import json
import pandas as pd

bundle = json.loads({payload_json!r})
records = bundle.get("records", [])
if not records:
    raise ValueError("bundle records 为空")

df = pd.DataFrame(records)
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["MA5"] = df["Close"].rolling(window=5).mean()
last_row = df.iloc[-1][["Date", "Close", "MA5"]]
print(
    "bundle_meta"
    f" source={{bundle.get('data_source', '')}}"
    f" asof={{bundle.get('asof', '')}}"
    f" symbol={{bundle.get('symbol', '')}}"
    f" rows={{len(records)}}"
)
print(last_row.to_string())
"""


async def main() -> None:
    print("[DEBUG] Tool test_full_loop Start")
    symbol = "AAPL"
    print("[INFO] Fetching market data outside sandbox...")

    market = await fetch_market_data(symbol, period="1mo", interval="1d")
    if not market.ok or not market.bundle:
        raise RuntimeError(f"No market data returned for {symbol}: {market.message}")

    code = _build_sandbox_code(market.bundle.to_serializable_dict())

    print("[INFO] Running MA5 calculation inside LocalDockerSandbox...")
    sandbox = LocalDockerSandbox()
    result = await sandbox.execute_code(code)

    print("[INFO] Sandbox exit code:", result["exit_code"])
    print("[INFO] Sandbox backend:", result.get("execution_backend", "unknown"))
    if result["stderr"].strip():
        print("[WARN] Sandbox stderr:")
        print(result["stderr"].strip())

    print("[RESULT] Last row with MA5:")
    print(result["stdout"].strip())

    backend = str(result.get("execution_backend", ""))
    if not backend.startswith("docker:"):
        raise RuntimeError(f"Unexpected sandbox backend: {backend}")
    if result["exit_code"] != 0:
        raise RuntimeError("Sandbox execution failed. See logs above.")


if __name__ == "__main__":
    asyncio.run(main())
