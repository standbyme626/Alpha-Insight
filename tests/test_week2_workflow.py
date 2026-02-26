from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agents.planner_engine import _extract_json_object, build_fallback_plan, route_data_source
from agents.workflow_engine import build_week2_graph
from tools.market_data import MarketDataResult, build_data_bundle


@dataclass
class _FakeTraceback:
    error_type: str
    message: str
    frames: list[dict]
    raw: str


@dataclass
class _FakeResult:
    stdout: str
    stderr: str
    exit_code: int
    images: list[str]
    traceback: _FakeTraceback | None


async def _fake_fetch_market_data(symbol: str, period: str = "1mo", interval: str = "1d") -> MarketDataResult:
    df = pd.DataFrame(
        {
            "Date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
            "Open": [100, 101, 102, 103, 104],
            "High": [101, 102, 103, 104, 105],
            "Low": [99, 100, 101, 102, 103],
            "Close": [100, 102, 101, 104, 106],
            "Volume": [10, 11, 12, 13, 14],
        }
    )
    records = df.to_dict(orient="records")
    bundle = build_data_bundle(
        symbol=symbol,
        period=period,
        interval=interval,
        records=records,
        data_source="test-fixture",
    )
    return MarketDataResult(ok=True, symbol=symbol, message="ok", records=bundle.records, bundle=bundle)


def test_week2_planner_task_breakdown() -> None:
    plan = build_fallback_plan("分析 AAPL 近一个月走势并输出图表")
    assert plan.steps == ["Data Fetch", "Logic Calc", "Plotting"]
    assert plan.data_source == "api"


def test_week2_planner_scraper_route() -> None:
    route = route_data_source("抓取 Reuters news 并给出情绪")
    assert route == "scraper"


def test_extract_json_object_from_markdown_fence() -> None:
    text = '```json\\n{\"steps\":[\"Data Fetch\",\"Logic Calc\",\"Plotting\"],\"data_source\":\"api\",\"reason\":\"ok\"}\\n```'
    parsed = _extract_json_object(text)
    assert parsed is not None
    assert parsed["data_source"] == "api"


@pytest.mark.asyncio
async def test_week2_self_correction_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    call_counter = {"n": 0}

    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str) -> _FakeResult:  # noqa: ANN001
        call_counter["n"] += 1
        if call_counter["n"] == 1:
            assert "Clsoe" in code
            return _FakeResult(
                stdout="",
                stderr="KeyError: 'Clsoe'",
                exit_code=1,
                images=[],
                traceback=_FakeTraceback(
                    error_type="KeyError",
                    message="'Clsoe'",
                    frames=[{"file": "x.py", "line": 1, "function": "<module>"}],
                    raw="KeyError: 'Clsoe'",
                ),
            )
        assert "Close" in code
        return _FakeResult(
            stdout="ok",
            stderr="",
            exit_code=0,
            images=[],
            traceback=None,
        )

    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)
    monkeypatch.setattr("agents.workflow_engine.fetch_market_data", _fake_fetch_market_data)

    app = build_week2_graph()
    output = await app.ainvoke(
        {
            "request": "分析 AAPL",
            "symbol": "AAPL",
            "period": "1mo",
            "inject_failure": True,
            "max_retries": 3,
        },
        config={"configurable": {"thread_id": "week2-loop"}},
    )

    assert output["success"] is True
    assert output["retry_count"] == 1
    assert output["debug_advice"] == "use_close_column"
    assert call_counter["n"] == 2
    assert "yfinance" not in output["sandbox_code"]
    assert "bundle_meta" in output["sandbox_code"]
    assert output["market_data_bundle"]["metadata"]["record_count"] == 5


@pytest.mark.asyncio
async def test_week2_checkpointer_persists_state(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str) -> _FakeResult:  # noqa: ANN001
        return _FakeResult(
            stdout="ok",
            stderr="",
            exit_code=0,
            images=[],
            traceback=None,
        )

    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)
    monkeypatch.setattr("agents.workflow_engine.fetch_market_data", _fake_fetch_market_data)

    checkpointer = InMemorySaver()
    app = build_week2_graph(checkpointer=checkpointer)

    config = {"configurable": {"thread_id": "week2-checkpoint"}}
    await app.ainvoke(
        {
            "request": "分析 TSLA",
            "symbol": "TSLA",
            "period": "1mo",
        },
        config=config,
    )

    checkpoint = checkpointer.get_tuple(config)
    assert checkpoint is not None
