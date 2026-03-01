"""Latency/error budget evaluator for Upgrade7 P2-A."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal


BudgetStatus = Literal["pass", "warn", "fail"]


def _percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ratio = min(1.0, max(0.0, ratio))
    sorted_values = sorted(float(item) for item in values)
    idx = int(round(ratio * (len(sorted_values) - 1)))
    idx = max(0, min(len(sorted_values) - 1, idx))
    return float(sorted_values[idx])


@dataclass(frozen=True)
class BudgetThresholds:
    p50_latency_warn_ms: float = 1200.0
    p50_latency_fail_ms: float = 4000.0
    p95_latency_warn_ms: float = 2500.0
    p95_latency_fail_ms: float = 6000.0
    error_rate_warn: float = 0.05
    error_rate_fail: float = 0.2
    fallback_rate_warn: float = 0.2
    fallback_rate_fail: float = 0.5
    retry_pressure_warn: float = 0.1
    retry_pressure_fail: float = 0.4


@dataclass(frozen=True)
class BudgetReason:
    metric: str
    status: BudgetStatus
    value: float
    warn_threshold: float
    fail_threshold: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "status": self.status,
            "value": self.value,
            "warn_threshold": self.warn_threshold,
            "fail_threshold": self.fail_threshold,
            "message": self.message,
        }


@dataclass(frozen=True)
class BudgetVerdict:
    status: BudgetStatus
    metrics: dict[str, float]
    reasons: list[BudgetReason]

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "metrics": self.metrics,
            "reasons": [item.to_dict() for item in self.reasons],
        }


def _reason_from_threshold(
    *,
    metric: str,
    value: float,
    warn_threshold: float,
    fail_threshold: float,
) -> BudgetReason | None:
    if math.isnan(value):
        value = 0.0
    if value >= fail_threshold:
        return BudgetReason(
            metric=metric,
            status="fail",
            value=float(value),
            warn_threshold=float(warn_threshold),
            fail_threshold=float(fail_threshold),
            message=f"{metric}={value:.6f} >= fail({fail_threshold:.6f})",
        )
    if value >= warn_threshold:
        return BudgetReason(
            metric=metric,
            status="warn",
            value=float(value),
            warn_threshold=float(warn_threshold),
            fail_threshold=float(fail_threshold),
            message=f"{metric}={value:.6f} >= warn({warn_threshold:.6f})",
        )
    return None


def evaluate_latency_error_budget(
    *,
    latency_samples_ms: list[float],
    error_count: int,
    total_count: int,
    fallback_count: int,
    retry_count: int,
    thresholds: BudgetThresholds | None = None,
) -> BudgetVerdict:
    cfg = thresholds or BudgetThresholds()
    safe_total = max(1, int(total_count))
    p50_latency = _percentile(latency_samples_ms, 0.50)
    p95_latency = _percentile(latency_samples_ms, 0.95)
    error_rate = float(max(0, error_count)) / safe_total
    fallback_rate = float(max(0, fallback_count)) / safe_total
    retry_pressure = float(max(0, retry_count)) / safe_total

    metrics = {
        "p50_latency_ms": float(p50_latency),
        "p95_latency_ms": float(p95_latency),
        "error_rate": float(error_rate),
        "fallback_rate": float(fallback_rate),
        "retry_pressure": float(retry_pressure),
    }

    reasons: list[BudgetReason] = []
    for maybe_reason in (
        _reason_from_threshold(
            metric="p50_latency_ms",
            value=p50_latency,
            warn_threshold=cfg.p50_latency_warn_ms,
            fail_threshold=cfg.p50_latency_fail_ms,
        ),
        _reason_from_threshold(
            metric="p95_latency_ms",
            value=p95_latency,
            warn_threshold=cfg.p95_latency_warn_ms,
            fail_threshold=cfg.p95_latency_fail_ms,
        ),
        _reason_from_threshold(
            metric="error_rate",
            value=error_rate,
            warn_threshold=cfg.error_rate_warn,
            fail_threshold=cfg.error_rate_fail,
        ),
        _reason_from_threshold(
            metric="fallback_rate",
            value=fallback_rate,
            warn_threshold=cfg.fallback_rate_warn,
            fail_threshold=cfg.fallback_rate_fail,
        ),
        _reason_from_threshold(
            metric="retry_pressure",
            value=retry_pressure,
            warn_threshold=cfg.retry_pressure_warn,
            fail_threshold=cfg.retry_pressure_fail,
        ),
    ):
        if maybe_reason is not None:
            reasons.append(maybe_reason)

    status: BudgetStatus = "pass"
    if any(item.status == "fail" for item in reasons):
        status = "fail"
    elif any(item.status == "warn" for item in reasons):
        status = "warn"

    return BudgetVerdict(status=status, metrics=metrics, reasons=reasons)
