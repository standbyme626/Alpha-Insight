"""Week3 reviewer: enforce number provenance and build final report summary."""

from __future__ import annotations

import json
from typing import Any


_METRICS_PREFIX = "METRICS_JSON="


def extract_metrics_from_stdout(stdout: str) -> dict[str, Any] | None:
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith(_METRICS_PREFIX):
            raw = line[len(_METRICS_PREFIX) :]
            try:
                data = json.loads(raw)
                if isinstance(data, dict):
                    return data
            except Exception:
                return None
    return None


def build_markdown_report(metrics: dict[str, Any], *, sentiment_text: str | None = None) -> str:
    # Guardrail: all numbers come from parsed sandbox metrics dict.
    symbol = metrics.get("symbol", "N/A")
    recommendation = metrics.get("recommendation", "HOLD")

    lines = [
        f"# Week3 Quant Report: {symbol}",
        "",
        f"- Recommendation: **{recommendation}**",
        f"- Fused Score: {metrics.get('fused_score')}",
        f"- Technical Score: {metrics.get('technical_score')}",
        f"- Sentiment Score: {metrics.get('sentiment_score')}",
        f"- Strategy Return: {metrics.get('strategy_return')}",
        f"- Benchmark Return: {metrics.get('benchmark_return')}",
        f"- Win Rate: {metrics.get('win_rate')}",
        f"- Max Drawdown: {metrics.get('max_drawdown')}",
    ]
    if sentiment_text:
        lines += ["", "## Sentiment Context", sentiment_text]
    return "\n".join(lines)
