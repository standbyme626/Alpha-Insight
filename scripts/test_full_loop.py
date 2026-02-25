"""Integration test: yfinance real data -> LocalDockerSandbox execution."""

from __future__ import annotations

import asyncio
import json

import yfinance as yf

from core.sandbox import LocalDockerSandbox


def _build_sandbox_code(csv_payload: str) -> str:
    """Build Python code string to run inside sandbox."""
    payload_json = json.dumps(csv_payload)
    return f"""
import json
from io import StringIO
import pandas as pd

csv_text = json.loads({payload_json!r})
df = pd.read_csv(StringIO(csv_text))
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
df["MA5"] = df["Close"].rolling(window=5).mean()
last_row = df.iloc[-1][["Date", "Close", "MA5"]]
print(last_row.to_string())
"""


async def main() -> None:
    print("[DEBUG] Tool test_full_loop Start")
    symbol = "AAPL"
    print("[INFO] Fetching real market data via yfinance...")

    data = yf.Ticker(symbol).history(period="30d")
    if data.empty:
        raise RuntimeError(f"No market data returned for {symbol}.")

    df = data.reset_index()[["Date", "Close"]]
    csv_payload = df.to_csv(index=False)

    code = _build_sandbox_code(csv_payload)

    print("[INFO] Running MA5 calculation inside LocalDockerSandbox...")
    sandbox = LocalDockerSandbox()
    result = await sandbox.execute_code(code)

    print("[INFO] Sandbox exit code:", result["exit_code"])
    if result["stderr"].strip():
        print("[WARN] Sandbox stderr:")
        print(result["stderr"].strip())

    print("[RESULT] Last row with MA5:")
    print(result["stdout"].strip())

    if result["exit_code"] != 0:
        raise RuntimeError("Sandbox execution failed. See logs above.")


if __name__ == "__main__":
    asyncio.run(main())
