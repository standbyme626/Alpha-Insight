#!/usr/bin/env python3
"""Generate hard-acceptance evidence artifacts for Alpha-Insight."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from agents import market_news_engine, workflow_engine
from agents.workflow_engine import run_unified_research
from tools.market_data import MarketDataResult, build_data_bundle


@dataclass
class RunRecord:
    index: int
    run_id: str
    success: bool
    fallback: bool
    retry: int
    market_latency_ms: float
    executor_latency_ms: float
    total_latency_ms: float
    backend: str
    failure_type: str
    failure_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "run_id": self.run_id,
            "success": self.success,
            "fallback": self.fallback,
            "retry": self.retry,
            "market_latency_ms": round(self.market_latency_ms, 3),
            "executor_latency_ms": round(self.executor_latency_ms, 3),
            "total_latency_ms": round(self.total_latency_ms, 3),
            "backend": self.backend,
            "failure_type": self.failure_type,
            "failure_count": self.failure_count,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_ohlcv(run_index: int) -> list[dict[str, Any]]:
    base = 100.0 + run_index
    closes = [base, base + 0.8, base + 1.1, base + 0.9, base + 1.6, base + 2.0, base + 2.3]
    rows: list[dict[str, Any]] = []
    for i, close in enumerate(closes, start=1):
        rows.append(
            {
                "Date": f"2026-02-{i:02d}",
                "Open": close - 0.3,
                "High": close + 0.4,
                "Low": close - 0.5,
                "Close": close,
                "Volume": 1000 + i * 15 + run_index,
            }
        )
    return rows


async def _offline_fetch_market_data(symbol: str, period: str = "1mo", interval: str = "1d") -> MarketDataResult:
    records = _build_ohlcv(run_index=0)
    bundle = build_data_bundle(
        symbol=symbol,
        period=period,
        interval=interval,
        records=records,
        data_source="offline-fixture",
    )
    return MarketDataResult(ok=True, symbol=symbol, message="offline fixture", records=bundle.records, bundle=bundle)


async def _offline_fetch_symbol_news(symbol: str, limit: int = 8) -> list[dict[str, Any]]:
    items = [
        {
            "title": f"{symbol} growth outlook improves",
            "summary": "Analysts see gain and profit momentum with stable demand.",
            "published_at": "2026-02-01T00:00:00+00:00",
            "source": "offline-fixture",
        },
        {
            "title": f"{symbol} risk remains manageable",
            "summary": "No major warning or lawsuit signal this week.",
            "published_at": "2026-02-02T00:00:00+00:00",
            "source": "offline-fixture",
        },
    ]
    return items[:limit]


def _offline_company_name(symbol: str, resolve_remote: bool = False) -> str:  # noqa: ARG001
    return f"OFFLINE-{symbol}"


@contextmanager
def _offline_patches() -> Any:
    original_fetch_market_data = workflow_engine.fetch_market_data
    original_fetch_news = market_news_engine.fetch_symbol_news
    original_get_company_name = market_news_engine.get_company_name

    workflow_engine.fetch_market_data = _offline_fetch_market_data
    market_news_engine.fetch_symbol_news = _offline_fetch_symbol_news
    market_news_engine.get_company_name = _offline_company_name
    try:
        yield
    finally:
        workflow_engine.fetch_market_data = original_fetch_market_data
        market_news_engine.fetch_symbol_news = original_fetch_news
        market_news_engine.get_company_name = original_get_company_name


def _failure_type(metrics: dict[str, Any]) -> tuple[str, int]:
    clusters = metrics.get("runtime_failure_clusters", {})
    if not isinstance(clusters, dict) or not clusters:
        return "none", 0
    sorted_items = sorted(clusters.items(), key=lambda kv: int(kv[1]), reverse=True)
    top_name, top_count = sorted_items[0]
    return str(top_name), int(top_count)


async def _single_run(index: int, symbol: str, period: str, interval: str) -> RunRecord:
    started = time.perf_counter()
    result = await run_unified_research(
        request=f"offline benchmark run {index}: analyze {symbol}",
        symbol=symbol,
        period=period,
        interval=interval,
        max_retries=2,
        news_limit=4,
    )
    total_latency_ms = (time.perf_counter() - started) * 1000
    metrics = result.get("metrics", {}) if isinstance(result, dict) else {}
    sandbox = result.get("sandbox_artifacts", {}) if isinstance(result, dict) else {}
    failure_type, failure_count = _failure_type(metrics if isinstance(metrics, dict) else {})

    return RunRecord(
        index=index,
        run_id=str(result.get("run_id", "")),
        success=bool(metrics.get("runtime_success", False)),
        fallback=bool(metrics.get("runtime_fallback_used", False)),
        retry=int(metrics.get("runtime_retry_count", 0)),
        market_latency_ms=float(metrics.get("runtime_market_data_latency_ms", 0.0)),
        executor_latency_ms=float(metrics.get("runtime_executor_latency_ms", 0.0)),
        total_latency_ms=total_latency_ms,
        backend=str(sandbox.get("backend", "unknown")),
        failure_type=failure_type,
        failure_count=failure_count,
    )


async def _run_batch(
    *,
    runs: int,
    symbol: str,
    period: str,
    interval: str,
    offline: bool,
) -> list[RunRecord]:
    records: list[RunRecord] = []
    if offline:
        with _offline_patches():
            for idx in range(1, runs + 1):
                records.append(await _single_run(idx, symbol=symbol, period=period, interval=interval))
        return records

    for idx in range(1, runs + 1):
        records.append(await _single_run(idx, symbol=symbol, period=period, interval=interval))
    return records


def _summarize(records: list[RunRecord]) -> dict[str, Any]:
    total = len(records)
    success_count = sum(1 for row in records if row.success)
    fallback_count = sum(1 for row in records if row.fallback)
    docker_backend_count = sum(1 for row in records if row.backend.startswith("docker:"))
    retry_total = sum(row.retry for row in records)

    frame = pd.DataFrame([row.to_dict() for row in records]) if records else pd.DataFrame()
    avg_total_latency_ms = float(frame["total_latency_ms"].mean()) if not frame.empty else 0.0
    p95_total_latency_ms = float(frame["total_latency_ms"].quantile(0.95)) if not frame.empty else 0.0

    failure_counter: dict[str, int] = {}
    for row in records:
        failure_counter[row.failure_type] = failure_counter.get(row.failure_type, 0) + 1

    return {
        "runs": total,
        "success_count": success_count,
        "success_rate": 0.0 if total == 0 else round(success_count / total, 4),
        "fallback_count": fallback_count,
        "fallback_rate": 0.0 if total == 0 else round(fallback_count / total, 4),
        "docker_backend_count": docker_backend_count,
        "docker_backend_ratio": 0.0 if total == 0 else round(docker_backend_count / total, 4),
        "retry_total": retry_total,
        "avg_total_latency_ms": round(avg_total_latency_ms, 3),
        "p95_total_latency_ms": round(p95_total_latency_ms, 3),
        "failure_type_distribution": failure_counter,
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_markdown(path: Path, title: str, payload: dict[str, Any]) -> None:
    summary = payload.get("summary", {})
    lines = [
        f"# {title}",
        "",
        f"generated_at: {payload.get('generated_at', '')}",
        f"mode: {payload.get('mode', '')}",
        f"runs: {summary.get('runs', 0)}",
        f"success_rate: {summary.get('success_rate', 0)}",
        f"docker_backend_ratio: {summary.get('docker_backend_ratio', 0)}",
        f"fallback_rate: {summary.get('fallback_rate', 0)}",
        f"avg_total_latency_ms: {summary.get('avg_total_latency_ms', 0)}",
        f"p95_total_latency_ms: {summary.get('p95_total_latency_ms', 0)}",
        f"failure_type_distribution: {json.dumps(summary.get('failure_type_distribution', {}), ensure_ascii=False)}",
        "",
        "## Sample Runs",
        "",
    ]
    for item in payload.get("records", [])[:5]:
        lines.append(
            "- "
            f"#{item.get('index')} run_id={item.get('run_id')} success={item.get('success')} "
            f"backend={item.get('backend')} fallback={item.get('fallback')} retry={item.get('retry')} "
            f"failure_type={item.get('failure_type')} total_latency_ms={item.get('total_latency_ms')}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


async def _cmd_generate(args: argparse.Namespace) -> None:
    records = await _run_batch(
        runs=args.runs,
        symbol=args.symbol,
        period=args.period,
        interval=args.interval,
        offline=args.offline,
    )
    payload = {
        "generated_at": _utc_now(),
        "mode": "offline-fixture" if args.offline else "live",
        "symbol": args.symbol,
        "period": args.period,
        "interval": args.interval,
        "summary": _summarize(records),
        "records": [row.to_dict() for row in records],
    }

    json_path = Path(args.output_json)
    md_path = Path(args.output_md)
    _write_json(json_path, payload)
    _write_markdown(md_path, title=args.title, payload=payload)
    print(f"[OK] wrote {json_path}")
    print(f"[OK] wrote {md_path}")
    print(json.dumps(payload["summary"], ensure_ascii=False, indent=2))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate hard acceptance evidence artifacts.")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="Generate run report or benchmark artifact")
    generate.add_argument("--runs", type=int, default=1)
    generate.add_argument("--symbol", default="AAPL")
    generate.add_argument("--period", default="1mo")
    generate.add_argument("--interval", default="1d")
    generate.add_argument("--offline", action="store_true", help="Patch data/news providers with offline fixtures")
    generate.add_argument("--output-json", required=True)
    generate.add_argument("--output-md", required=True)
    generate.add_argument("--title", default="Hard Acceptance Evidence")
    generate.set_defaults(func=_cmd_generate)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
