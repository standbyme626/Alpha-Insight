from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agents.report_coder import build_week3_code
from agents.report_reviewer import extract_metrics_from_stdout
from agents.report_workflow import build_week3_graph, hitl_node, reviewer_node
from tools.artifact_extractor import build_transfer_payload


@dataclass
class _FakeResult:
    stdout: str
    stderr: str
    exit_code: int
    images: list[str]
    output_files: list[str]
    traceback: object | None


def test_week3_coder_contains_ta_and_backtest() -> None:
    code = build_week3_code({"symbol": "AAPL", "period": "6mo", "sentiment_score": 70})
    assert "MACD" in code
    assert "RSI" in code
    assert "strategy_ret" in code
    assert "METRICS_JSON=" in code


def test_extract_metrics_from_stdout() -> None:
    stdout = 'abc\nMETRICS_JSON={"symbol":"AAPL","fused_score":80,"recommendation":"BUY"}\n'
    metrics = extract_metrics_from_stdout(stdout)
    assert metrics is not None
    assert metrics["recommendation"] == "BUY"


def test_artifact_transfer_payload(tmp_path: Path) -> None:
    html = tmp_path / "report.html"
    png = tmp_path / "chart.png"
    html.write_text("<html>ok</html>", encoding="utf-8")
    png.write_bytes(b"\x89PNG\r\n")

    payload = build_transfer_payload([str(html), str(png)])
    assert len(payload) == 2
    assert payload[0]["mime"] == "text/html"
    assert payload[1]["mime"] == "image/png"


@pytest.mark.asyncio
async def test_week3_workflow_with_hitl_approved(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    html = tmp_path / "report.html"
    png = tmp_path / "chart.png"
    html.write_text("<html>report</html>", encoding="utf-8")
    png.write_bytes(b"\x89PNG\r\n")

    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str) -> _FakeResult:  # noqa: ANN001
        stdout = (
            'METRICS_JSON={"symbol":"AAPL","recommendation":"BUY","fused_score":82,'
            '"technical_score":75,"sentiment_score":90,"strategy_return":0.12,'
            '"benchmark_return":0.08,"win_rate":0.61,"max_drawdown":-0.09}'
        )
        return _FakeResult(
            stdout=stdout,
            stderr="",
            exit_code=0,
            images=[str(png)],
            output_files=[str(html), str(png)],
            traceback=None,
        )

    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)
    monkeypatch.setattr("agents.report_workflow.interrupt", lambda payload: True)

    app = build_week3_graph()
    output = await app.ainvoke(
        {
            "request": "分析 AAPL",
            "symbol": "AAPL",
            "period": "6mo",
            "sentiment_score": 88,
            "sentiment_text": "新闻情绪偏积极",
        },
        config={"configurable": {"thread_id": "week3-hitl-yes"}},
    )

    assert output["recommendation"] == "BUY"
    assert output["hitl_status"] == "approved"
    assert output["human_approved"] is True
    assert len(output["transfer_payload"]) == 2
    assert "Week3 Quant Report" in output["report_markdown"]


@pytest.mark.asyncio
async def test_week3_reviewer_history_append(tmp_path: Path) -> None:
    html = tmp_path / "report.html"
    html.write_text("<html>report</html>", encoding="utf-8")

    state = {
        "symbol": "TSLA",
        "sandbox_stdout": 'METRICS_JSON={"symbol":"TSLA","recommendation":"HOLD","fused_score":55}',
        "output_files": [str(html)],
        "metrics_history": [{"symbol": "AAPL", "fused_score": 70}],
    }

    out = await reviewer_node(state)
    assert out["recommendation"] == "HOLD"
    assert len(out["metrics_history"]) == 2


@pytest.mark.asyncio
async def test_week3_checkpointer_memory(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    html = tmp_path / "report.html"
    html.write_text("<html>report</html>", encoding="utf-8")

    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str) -> _FakeResult:  # noqa: ANN001
        stdout = 'METRICS_JSON={"symbol":"MSFT","recommendation":"HOLD","fused_score":61}'
        return _FakeResult(
            stdout=stdout,
            stderr="",
            exit_code=0,
            images=[],
            output_files=[str(html)],
            traceback=None,
        )

    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)
    monkeypatch.setattr("agents.report_workflow.interrupt", lambda payload: True)

    cp = InMemorySaver()
    app = build_week3_graph(checkpointer=cp)
    config = {"configurable": {"thread_id": "week3-memory"}}

    await app.ainvoke({"request": "run1", "symbol": "MSFT"}, config=config)
    out2 = await app.ainvoke({"request": "run2", "symbol": "MSFT"}, config=config)

    assert len(out2.get("metrics_history", [])) >= 2


@pytest.mark.asyncio
async def test_week3_hitl_reject(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("agents.report_workflow.interrupt", lambda payload: False)
    out = await hitl_node({"recommendation": "BUY", "symbol": "AAPL", "metrics": {"fused_score": 80}})
    assert out["hitl_status"] == "rejected"
    assert out["human_approved"] is False
