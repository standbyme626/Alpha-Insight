"""Real online LLM smoke test for Week2 planner using OpenAI-compatible endpoint."""

from __future__ import annotations

import asyncio
import os

from agents.week2_planner import plan_tasks


async def _run_case(request: str) -> None:
    result = await plan_tasks(request)
    print("REQUEST:", request)
    print("PROVIDER:", result.provider)
    print("DATA_SOURCE:", result.data_source)
    print("STEPS:", result.steps)
    print("REASON:", result.reason)
    print("-" * 60)


def _ensure_env() -> None:
    required = ["OPENAI_API_KEY", "OPENAI_API_BASE", "OPENAI_MODEL_NAME"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"Missing required env vars for real LLM test: {missing}")


async def main() -> None:
    _ensure_env()
    await _run_case("分析 AAPL 最近一个月走势，给出规划步骤")
    await _run_case("抓取 Reuters 的 TSLA 新闻并给出规划步骤")


if __name__ == "__main__":
    asyncio.run(main())
