#!/usr/bin/env python3
"""Generate Upgrade7 P2-A evidence artifacts."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from agents.scanner_engine import ScanConfig, build_scan_trigger, run_watchlist_cycle
from agents.workflow_engine import run_unified_research
from core.reliability_budget import BudgetThresholds
from core.sandbox_manager import SandboxManager
from tools.market_data import MarketDataResult, build_data_bundle


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def _fake_fetch_market_data(symbol: str, period: str = "1mo", interval: str = "1d") -> MarketDataResult:
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
        data_source="evidence-fixture",
    )
    return MarketDataResult(ok=True, symbol=symbol, message="ok", records=bundle.records, bundle=bundle)


async def _fake_scan_fetch(symbol: str, period: str, interval: str = "1d") -> MarketDataResult:
    frame = pd.DataFrame(
        {
            "Date": ["2026-01-01", "2026-01-02"],
            "Open": [100, 100],
            "High": [101, 107],
            "Low": [99, 99],
            "Close": [100, 106] if symbol.upper() == "AAPL" else [100, 103],
            "Volume": [1, 1],
        }
    )
    return MarketDataResult(ok=True, symbol=symbol, message="ok", records=frame.to_dict(orient="records"))


async def _fake_market_news_analysis(**kwargs) -> dict[str, Any]:  # noqa: ANN003
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


async def _build_scanner_fault_evidence() -> dict[str, Any]:
    cfg = ScanConfig(
        watchlist=["AAPL", "MSFT"],
        pct_alert_threshold=0.03,
        failure_spike_count=1,
    )
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id="upgrade7-p2-scan-fi")
    baseline = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=_fake_scan_fetch,
        enable_triggered_research=False,
    )
    injected = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=_fake_scan_fetch,
        enable_triggered_research=False,
        runtime_flags={
            "fault_injection": {
                "enabled": True,
                "seed": 11,
                "rules": [
                    {"node": "scanner.fetch", "fault": "rate_limit", "rate": 1.0},
                ],
            }
        },
    )
    return {
        "baseline_budget_verdict": baseline.runtime_metrics.get("runtime_budget_verdict"),
        "injected_budget_verdict": injected.runtime_metrics.get("runtime_budget_verdict"),
        "injected_fault_events": injected.runtime_metrics.get("fault_injection_events", []),
        "injected_alarm_rules": [item.get("rule") for item in injected.alarms],
        "injected_failure_clusters": injected.failure_clusters,
        "injected_runtime_metrics": {
            "runtime_budget_verdict": injected.runtime_metrics.get("runtime_budget_verdict"),
            "runtime_budget_reasons": injected.runtime_metrics.get("runtime_budget_reasons"),
            "runtime_latency_p95_ms": injected.runtime_metrics.get("runtime_latency_p95_ms"),
            "runtime_error_rate": injected.runtime_metrics.get("runtime_error_rate"),
            "runtime_fallback_rate": injected.runtime_metrics.get("runtime_fallback_rate"),
            "runtime_retry_pressure": injected.runtime_metrics.get("runtime_retry_pressure"),
            "fault_injection_event_count": injected.runtime_metrics.get("fault_injection_event_count"),
        },
    }


async def _build_workflow_fault_evidence(*, node: str, fault: str, seed: int) -> dict[str, Any]:
    from agents import market_news_engine, workflow_engine

    original_fetch = workflow_engine.fetch_market_data
    original_market_news = market_news_engine.run_market_news_analysis
    original_create = SandboxManager.create_session
    original_destroy = SandboxManager.destroy_session

    async def fake_create(self) -> str:  # noqa: ANN001
        self._session = "local-docker"  # noqa: SLF001
        return "local-session"

    async def fake_destroy(self) -> None:  # noqa: ANN001
        self._session = None  # noqa: SLF001
        return None

    workflow_engine.fetch_market_data = _fake_fetch_market_data  # type: ignore[assignment]
    market_news_engine.run_market_news_analysis = _fake_market_news_analysis  # type: ignore[assignment]
    SandboxManager.create_session = fake_create  # type: ignore[assignment]
    SandboxManager.destroy_session = fake_destroy  # type: ignore[assignment]
    try:
        payload = await run_unified_research(
            request=f"Upgrade7 P2 evidence {node}/{fault}",
            symbol="AAPL",
            period="1mo",
            max_retries=0,
            fault_injection={
                "enabled": True,
                "seed": seed,
                "rules": [
                    {"node": node, "fault": fault, "rate": 1.0},
                ],
            },
        )
    finally:
        workflow_engine.fetch_market_data = original_fetch  # type: ignore[assignment]
        market_news_engine.run_market_news_analysis = original_market_news  # type: ignore[assignment]
        SandboxManager.create_session = original_create  # type: ignore[assignment]
        SandboxManager.destroy_session = original_destroy  # type: ignore[assignment]

    metrics = payload.get("metrics", {}) if isinstance(payload, dict) else {}
    return {
        "node": node,
        "fault": fault,
        "run_id": payload.get("run_id"),
        "runtime_budget_verdict": metrics.get("runtime_budget_verdict"),
        "runtime_budget_reasons": metrics.get("runtime_budget_reasons", []),
        "runtime_fault_injection_count": metrics.get("runtime_fault_injection_count", 0),
        "runtime_fault_injection_events": metrics.get("runtime_fault_injection_events", []),
        "runtime_latency_p95_ms": metrics.get("runtime_latency_p95_ms", 0.0),
        "runtime_error_rate": metrics.get("runtime_error_rate", 0.0),
        "runtime_fallback_rate": metrics.get("runtime_fallback_rate", 0.0),
        "runtime_retry_pressure": metrics.get("runtime_retry_pressure", 0.0),
        "fault_injection_event_count": metrics.get("runtime_fault_injection_count", 0),
        "sandbox_success": bool(payload.get("sandbox_artifacts", {}).get("success", True)),
    }


async def _build_scanner_fault_case(*, node: str, fault: str, seed: int) -> dict[str, Any]:
    cfg = ScanConfig(
        watchlist=["AAPL"],
        pct_alert_threshold=0.03,
        failure_spike_count=1,
    )
    trigger = build_scan_trigger(trigger_type="scheduled", trigger_id=f"upgrade7-p2-scan-{node}-{fault}")
    result = await run_watchlist_cycle(
        cfg,
        trigger=trigger,
        mode="anomaly",
        fetcher=_fake_scan_fetch,
        enable_triggered_research=False,
        runtime_flags={
            "fault_injection": {
                "enabled": True,
                "seed": seed,
                "rules": [
                    {"node": node, "fault": fault, "rate": 1.0},
                ],
            }
        },
    )
    return {
        "node": node,
        "fault": fault,
        "runtime_budget_verdict": result.runtime_metrics.get("runtime_budget_verdict"),
        "runtime_budget_reasons": result.runtime_metrics.get("runtime_budget_reasons", []),
        "runtime_latency_p95_ms": result.runtime_metrics.get("runtime_latency_p95_ms", 0.0),
        "runtime_error_rate": result.runtime_metrics.get("runtime_error_rate", 0.0),
        "runtime_fallback_rate": result.runtime_metrics.get("runtime_fallback_rate", 0.0),
        "runtime_retry_pressure": result.runtime_metrics.get("runtime_retry_pressure", 0.0),
        "fault_injection_event_count": result.runtime_metrics.get("fault_injection_event_count", 0),
        "fault_injection_events": result.runtime_metrics.get("fault_injection_events", []),
        "failure_clusters": result.failure_clusters,
        "alarms": result.alarms,
    }


async def _build_fault_matrix() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    scanner_cases = [
        ("scanner.fetch", "timeout"),
        ("scanner.fetch", "upstream_5xx"),
        ("scanner.fetch", "rate_limit"),
        ("scanner.parse", "parse"),
    ]
    for idx, (node, fault) in enumerate(scanner_cases, start=101):
        payload = await _build_scanner_fault_case(node=node, fault=fault, seed=idx)
        payload["scope"] = "scanner"
        rows.append(payload)

    workflow_cases = [
        ("workflow.market_data", "upstream_5xx"),
        ("workflow.executor", "sandbox_failure"),
        ("runtime.sandbox_execute", "timeout"),
    ]
    for idx, (node, fault) in enumerate(workflow_cases, start=201):
        payload = await _build_workflow_fault_evidence(node=node, fault=fault, seed=idx)
        payload["scope"] = "workflow/runtime"
        rows.append(payload)
    return rows


def _scenario_row(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "scenario": name,
        "runtime_budget_verdict": payload.get("runtime_budget_verdict"),
        "runtime_latency_p95_ms": payload.get("runtime_latency_p95_ms"),
        "runtime_error_rate": payload.get("runtime_error_rate"),
        "runtime_fallback_rate": payload.get("runtime_fallback_rate"),
        "runtime_retry_pressure": payload.get("runtime_retry_pressure"),
        "runtime_budget_reasons": payload.get("runtime_budget_reasons", []),
    }


def _summarize_verdict(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"pass": 0, "warn": 0, "fail": 0}
    for row in rows:
        verdict = str(row.get("runtime_budget_verdict", "pass"))
        if verdict not in counts:
            continue
        counts[verdict] += 1
    return counts


def _overall_verdict(counts: dict[str, int]) -> str:
    if counts.get("fail", 0) > 0:
        return "fail"
    if counts.get("warn", 0) > 0:
        return "warn"
    return "pass"


async def _main() -> None:
    scanner_payload = await _build_scanner_fault_evidence()
    workflow_payload = await _build_workflow_fault_evidence(
        node="workflow.executor",
        fault="sandbox_failure",
        seed=19,
    )
    fault_matrix = await _build_fault_matrix()

    fault_evidence = {
        "generated_at": _now(),
        "scope": "Upgrade7 P2-A fault injection + latency/error budget minimal loop",
        "scenarios": {
            "scanner_fault_injection": scanner_payload,
            "workflow_fault_injection": workflow_payload,
        },
        "fault_matrix": fault_matrix,
        "fault_matrix_case_count": len(fault_matrix),
        "fault_injection_events_total": len(scanner_payload.get("injected_fault_events", []))
        + len(workflow_payload.get("runtime_fault_injection_events", []))
        + sum(int(item.get("fault_injection_event_count", 0)) for item in fault_matrix),
    }
    rows = [
        _scenario_row("scanner_fault_injection", scanner_payload.get("injected_runtime_metrics", {})),
        _scenario_row("workflow_fault_injection", workflow_payload),
        *[
            _scenario_row(
                f"{item.get('scope')}:{item.get('node')}:{item.get('fault')}",
                item,
            )
            for item in fault_matrix
        ],
    ]
    counts = _summarize_verdict(rows)
    summary_payload = {
        "generated_at": _now(),
        "thresholds": BudgetThresholds().__dict__,
        "scenarios": rows,
        "verdict_counts": counts,
        "overall_verdict": _overall_verdict(counts),
    }

    fault_path = Path("docs/evidence/upgrade7_p2_fault_injection_budget.json")
    summary_path = Path("docs/evidence/upgrade7_p2_latency_error_budget_summary.json")
    _write_json(fault_path, fault_evidence)
    _write_json(summary_path, summary_payload)
    print(f"[OK] {fault_path}")
    print(f"[OK] {summary_path}")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
