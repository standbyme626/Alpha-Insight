"""Governance hooks for runtime budget evaluation."""

from __future__ import annotations

from typing import Any

from core.reliability_budget import evaluate_latency_error_budget


def apply_runtime_budget_metrics(
    metrics: dict[str, Any],
    *,
    market_data_latency_ms: float,
    executor_latency_ms: float,
    runtime_failure_count: int,
    runtime_fallback_used: bool,
    runtime_retry_count: int,
) -> dict[str, Any]:
    budget = evaluate_latency_error_budget(
        latency_samples_ms=[market_data_latency_ms, executor_latency_ms],
        error_count=runtime_failure_count,
        total_count=2,
        fallback_count=1 if runtime_fallback_used else 0,
        retry_count=runtime_retry_count,
    )
    metrics["runtime_budget_verdict"] = budget.status
    metrics["runtime_budget_reasons"] = [item.to_dict() for item in budget.reasons]
    metrics["runtime_latency_p50_ms"] = budget.metrics["p50_latency_ms"]
    metrics["runtime_latency_p95_ms"] = budget.metrics["p95_latency_ms"]
    metrics["runtime_error_rate"] = budget.metrics["error_rate"]
    metrics["runtime_fallback_rate"] = budget.metrics["fallback_rate"]
    metrics["runtime_retry_pressure"] = budget.metrics["retry_pressure"]
    return metrics
