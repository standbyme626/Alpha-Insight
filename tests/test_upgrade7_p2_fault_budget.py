from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from agents.scanner_engine import ScanConfig, build_scan_trigger, run_watchlist_cycle
from agents.workflow_engine import run_unified_research
from core.fault_injection import FaultInjector, fault_semantic
from core.reliability_budget import evaluate_latency_error_budget
from core.sandbox_manager import SandboxManager
from tools.market_data import MarketDataResult, build_data_bundle


def test_fault_injector_rule_match_and_default_off() -> None:
    disabled = FaultInjector.from_payload(None)
    assert disabled.enabled is False
    assert disabled.maybe_inject(node="scanner.fetch") is None

    injector = FaultInjector.from_payload(
        {
            "enabled": True,
            "seed": 42,
            "rules": [
                {"node": "scanner.fetch", "fault": "rate_limit", "rate": 1.0},
            ],
        }
    )
    event = injector.maybe_inject(node="scanner.fetch", allowed_faults=("rate_limit",))
    assert event is not None
    assert event.fault == "rate_limit"
    assert event.node == "scanner.fetch"


def test_fault_semantic_contains_reliability_keywords() -> None:
    timeout = fault_semantic("timeout", node="x")
    assert timeout.error_type == "TimeoutError"
    assert "timed out" in timeout.message
    assert timeout.default_retry_delta == 1

    rate = fault_semantic("rate_limit", node="x")
    assert rate.error_type == "RateLimitError"
    assert "429" in rate.message
    assert rate.retriable is True

    parse = fault_semantic("parse", node="x")
    assert parse.error_type == "ParseError"
    assert "parse error" in parse.message.lower()
    assert parse.retriable is False


def test_latency_error_budget_evaluator_verdicts() -> None:
    passed = evaluate_latency_error_budget(
        latency_samples_ms=[100.0, 120.0, 90.0],
        error_count=0,
        total_count=10,
        fallback_count=0,
        retry_count=0,
    )
    assert passed.status == "pass"

    warned = evaluate_latency_error_budget(
        latency_samples_ms=[1300.0, 1400.0, 1100.0],
        error_count=1,
        total_count=20,
        fallback_count=0,
        retry_count=0,
    )
    assert warned.status == "warn"

    failed = evaluate_latency_error_budget(
        latency_samples_ms=[7000.0, 8000.0],
        error_count=5,
        total_count=10,
        fallback_count=6,
        retry_count=5,
    )
    assert failed.status == "fail"


@pytest.mark.asyncio
async def test_run_watchlist_cycle_fault_injection_budget_visible() -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        frame = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Open": [100, 100],
                "High": [101, 107],
                "Low": [99, 99],
                "Close": [100, 106],
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=frame.to_dict(orient="records"))

    cfg = ScanConfig(
        watchlist=["AAPL", "MSFT"],
        pct_alert_threshold=0.03,
        failure_spike_count=1,
    )
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id="p2-fi")
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=fake_fetch,
        enable_triggered_research=False,
        runtime_flags={
            "fault_injection": {
                "enabled": True,
                "seed": 7,
                "rules": [{"node": "scanner.fetch", "fault": "rate_limit", "rate": 1.0}],
            }
        },
    )

    assert result.runtime_metrics["fault_injection_enabled"] is True
    assert result.runtime_metrics["fault_injection_event_count"] >= 1
    assert result.runtime_metrics["runtime_budget_verdict"] == "fail"
    assert result.runtime_metrics["runtime_error_rate"] >= 0.5
    assert result.failure_clusters
    assert any(item["rule"] == "failure_spike" for item in result.alarms)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["timeout", "upstream_5xx", "rate_limit"])
async def test_scanner_fetch_fault_injection_matrix(fault: str) -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        frame = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Open": [100, 100],
                "High": [101, 107],
                "Low": [99, 99],
                "Close": [100, 106],
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=frame.to_dict(orient="records"))

    cfg = ScanConfig(watchlist=["AAPL"], pct_alert_threshold=0.03, failure_spike_count=1)
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id=f"matrix-{fault}")
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=fake_fetch,
        enable_triggered_research=False,
        runtime_flags={
            "fault_injection": {
                "enabled": True,
                "seed": 1,
                "rules": [{"node": "scanner.fetch", "fault": fault, "rate": 1.0}],
            }
        },
    )
    assert result.runtime_metrics["fault_injection_event_count"] >= 1
    assert result.runtime_metrics["runtime_budget_verdict"] == "fail"
    assert result.runtime_metrics["runtime_error_rate"] >= 1.0


@pytest.mark.asyncio
async def test_scanner_parse_fault_injection_matrix() -> None:
    async def fake_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
        frame = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02"],
                "Open": [100, 100],
                "High": [101, 107],
                "Low": [99, 99],
                "Close": [100, 106],
                "Volume": [1, 1],
            }
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=frame.to_dict(orient="records"))

    cfg = ScanConfig(watchlist=["AAPL"], pct_alert_threshold=0.03, failure_spike_count=1)
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id="matrix-parse")
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=fake_fetch,
        enable_triggered_research=False,
        runtime_flags={
            "fault_injection": {
                "enabled": True,
                "seed": 1,
                "rules": [{"node": "scanner.parse", "fault": "parse", "rate": 1.0}],
            }
        },
    )
    assert result.runtime_metrics["fault_injection_event_count"] >= 1
    assert result.runtime_metrics["runtime_budget_verdict"] == "fail"
    assert result.runtime_metrics["runtime_error_rate"] >= 1.0


@pytest.mark.asyncio
async def test_runtime_node_fault_injection_timeout_short_circuits_runtime() -> None:
    injector = FaultInjector.from_payload(
        {
            "enabled": True,
            "seed": 123,
            "rules": [{"node": "runtime.sandbox_execute", "fault": "timeout", "rate": 1.0}],
        }
    )
    manager = SandboxManager(fault_injector=injector)
    manager._session = "local-docker"  # noqa: SLF001
    result = await manager.execute("print('ok')")
    assert result.traceback is not None
    assert result.traceback.error_type == "TimeoutError"
    assert "timed out" in result.traceback.message
    assert result.resource_usage is not None
    assert isinstance(result.resource_usage.get("fault_injection"), dict)


@pytest.mark.asyncio
async def test_run_unified_research_workflow_fault_injection_exposes_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch_market_data(symbol: str, period: str = "1mo", interval: str = "1d") -> MarketDataResult:
        frame = pd.DataFrame(
            {
                "Date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
                "Open": [100, 101, 102, 103, 104],
                "High": [101, 102, 103, 104, 105],
                "Low": [99, 100, 101, 102, 103],
                "Close": [100, 102, 101, 104, 106],
                "Volume": [10, 11, 12, 13, 14],
            }
        )
        bundle = build_data_bundle(
            symbol=symbol,
            period=period,
            interval=interval,
            records=frame.to_dict(orient="records"),
            data_source="test-fixture",
        )
        return MarketDataResult(ok=True, symbol=symbol, message="ok", records=bundle.records, bundle=bundle)

    async def fake_create_session(self) -> str:  # noqa: ANN001
        return "fake"

    async def fake_destroy_session(self) -> None:  # noqa: ANN001
        return None

    async def fake_execute(self, code: str):  # noqa: ANN001
        raise AssertionError("workflow.executor fault injection should short-circuit before SandboxManager.execute")

    async def fake_run_market_news_analysis(**kwargs):  # noqa: ANN003
        return {
            "symbol": kwargs.get("symbol", "AAPL"),
            "period": kwargs.get("period", "1mo"),
            "latest_close": 106.0,
            "period_change_pct": 6.0,
            "ma20": 103.2,
            "rsi14": 58.1,
            "volatility_pct": 2.1,
            "volume_ratio": 1.08,
            "sentiment_score": 62.5,
            "analysis_steps": ["reuse bundle", "calc metrics", "fuse output"],
            "final_assessment": "ok",
        }

    monkeypatch.setattr("agents.workflow_engine.fetch_market_data", fake_fetch_market_data)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.create_session", fake_create_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.destroy_session", fake_destroy_session)
    monkeypatch.setattr("core.sandbox_manager.SandboxManager.execute", fake_execute)
    monkeypatch.setattr("agents.market_news_engine.run_market_news_analysis", fake_run_market_news_analysis)

    out = await run_unified_research(
        request="分析 AAPL",
        symbol="AAPL",
        period="1mo",
        max_retries=0,
        fault_injection={
            "enabled": True,
            "seed": 99,
            "rules": [{"node": "workflow.executor", "fault": "sandbox_failure", "rate": 1.0}],
        },
    )

    metrics = out["metrics"]
    assert metrics["runtime_fault_injection_enabled"] is True
    assert metrics["runtime_fault_injection_count"] >= 1
    assert metrics["runtime_budget_verdict"] == "fail"
    assert metrics["runtime_error_rate"] >= 0.5
    assert isinstance(metrics["runtime_budget_reasons"], list)
    assert out["sandbox_artifacts"]["success"] is False
    assert out["sandbox_artifacts"]["traceback"] is not None
