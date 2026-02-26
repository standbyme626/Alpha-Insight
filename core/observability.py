"""Observability helpers with optional Arize Phoenix integration."""

from __future__ import annotations

import os
import re
import time
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Generator, Iterable, Literal


@dataclass
class SpanEvent:
    name: str
    duration_ms: float
    status: str
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class RuntimeMetric:
    name: str
    value: float
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class FailureEvent:
    source: str
    error_type: str
    message: str
    cluster: str
    tag: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "error_type": self.error_type,
            "message": self.message,
            "cluster": self.cluster,
            "tag": self.tag,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> FailureEvent:
        ts_raw = payload.get("timestamp")
        timestamp = datetime.now(timezone.utc)
        if isinstance(ts_raw, str):
            try:
                timestamp = datetime.fromisoformat(ts_raw)
            except ValueError:
                timestamp = datetime.now(timezone.utc)
        return cls(
            source=str(payload.get("source", "unknown")),
            error_type=str(payload.get("error_type", "RuntimeError")),
            message=str(payload.get("message", "")),
            cluster=str(payload.get("cluster", "unknown")),
            tag=str(payload.get("tag", "unknown:unknown")),
            timestamp=timestamp,
        )


@dataclass
class ThresholdAlarm:
    rule: Literal["fallback_spike", "failure_spike", "latency_anomaly"]
    severity: Literal["warning", "critical"]
    value: float
    threshold: float
    message: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule": self.rule,
            "severity": self.severity,
            "value": self.value,
            "threshold": self.threshold,
            "message": self.message,
        }


_PERMISSION_PATTERNS = [r"permission denied", r"access denied", r"not permitted"]
_TIMEOUT_PATTERNS = [r"timeout", r"timed out", r"deadline exceeded"]
_NETWORK_PATTERNS = [r"connection", r"network", r"dns", r"http", r"ssl", r"telegram"]
_DATA_PATTERNS = [r"no data", r"empty", r"not found", r"record", r"invalid dataframe", r"invalid close"]
_LOGIC_PATTERNS = [r"keyerror", r"valueerror", r"typeerror", r"assertionerror", r"indexerror"]


def _matches_any(text: str, patterns: list[str]) -> bool:
    return any(re.search(pattern, text) for pattern in patterns)


def classify_failure(
    *,
    source: str,
    error_type: str,
    message: str,
    backend: str = "",
) -> FailureEvent:
    blob = " ".join([source, error_type, message, backend]).lower()
    if "fallback" in blob or "local-process" in blob:
        cluster = "fallback"
    elif _matches_any(blob, _PERMISSION_PATTERNS):
        cluster = "permission"
    elif _matches_any(blob, _TIMEOUT_PATTERNS):
        cluster = "timeout"
    elif _matches_any(blob, _NETWORK_PATTERNS):
        cluster = "network"
    elif _matches_any(blob, _DATA_PATTERNS):
        cluster = "data"
    elif source.startswith("workflow.executor"):
        cluster = "sandbox"
    elif _matches_any(blob, _LOGIC_PATTERNS):
        cluster = "logic"
    else:
        cluster = "unknown"
    return FailureEvent(
        source=source,
        error_type=error_type,
        message=message,
        cluster=cluster,
        tag=f"{source}:{cluster}",
    )


def aggregate_failure_clusters(failures: Iterable[FailureEvent]) -> dict[str, int]:
    counter = Counter(item.cluster for item in failures)
    return dict(counter)


def aggregate_failure_tags(failures: Iterable[FailureEvent]) -> dict[str, int]:
    counter = Counter(item.tag for item in failures)
    return dict(counter)


def evaluate_threshold_alarms(
    *,
    fallback_rate: float,
    failure_count: int,
    latency_ms: float,
    fallback_spike_rate: float = 0.25,
    failure_spike_count: int = 3,
    latency_anomaly_ms: float = 2500.0,
) -> list[ThresholdAlarm]:
    alarms: list[ThresholdAlarm] = []
    if fallback_rate >= fallback_spike_rate:
        alarms.append(
            ThresholdAlarm(
                rule="fallback_spike",
                severity="warning",
                value=float(fallback_rate),
                threshold=float(fallback_spike_rate),
                message=f"fallback spike: rate={fallback_rate:.2%} >= {fallback_spike_rate:.2%}",
            )
        )
    if failure_count >= failure_spike_count:
        alarms.append(
            ThresholdAlarm(
                rule="failure_spike",
                severity="critical",
                value=float(failure_count),
                threshold=float(failure_spike_count),
                message=f"failure spike: count={failure_count} >= {failure_spike_count}",
            )
        )
    if latency_ms >= latency_anomaly_ms:
        alarms.append(
            ThresholdAlarm(
                rule="latency_anomaly",
                severity="warning",
                value=float(latency_ms),
                threshold=float(latency_anomaly_ms),
                message=f"latency anomaly: {latency_ms:.1f}ms >= {latency_anomaly_ms:.1f}ms",
            )
        )
    return alarms


class QuantTelemetry:
    def __init__(self) -> None:
        self._events: list[SpanEvent] = []
        self._metrics: list[RuntimeMetric] = []
        self._failures: list[FailureEvent] = []
        self._enabled = bool(os.getenv("PHOENIX_COLLECTOR_ENDPOINT"))

    @contextmanager
    def span(self, name: str, *, tags: dict[str, str] | None = None) -> Generator[None, None, None]:
        start = time.perf_counter()
        status = "ok"
        try:
            yield
        except Exception:
            status = "error"
            raise
        finally:
            duration_ms = (time.perf_counter() - start) * 1000
            event_tags = dict(tags or {})
            self._events.append(SpanEvent(name=name, duration_ms=duration_ms, status=status, tags=event_tags))
            self.record_metric("runtime.latency_ms", duration_ms, node=name, status=status)
            self.record_success(node=name, success=status == "ok")

    def record_metric(self, name: str, value: float, **tags: str) -> None:
        self._metrics.append(RuntimeMetric(name=name, value=float(value), tags=dict(tags)))

    def record_success(self, *, node: str, success: bool) -> None:
        metric = "runtime.success_count" if success else "runtime.failure_count"
        self.record_metric(metric, 1.0, node=node)

    def record_retry(self, *, node: str, retry_count: int) -> None:
        if retry_count <= 0:
            return
        self.record_metric("runtime.retry_count", float(retry_count), node=node)

    def record_fallback(self, *, node: str, used_fallback: bool, reason: str = "") -> None:
        if not used_fallback:
            return
        tags = {"node": node}
        if reason:
            tags["reason"] = reason
        self.record_metric("runtime.fallback_count", 1.0, **tags)

    def record_failure(
        self,
        *,
        source: str,
        error_type: str,
        message: str,
        backend: str = "",
    ) -> FailureEvent:
        event = classify_failure(source=source, error_type=error_type, message=message, backend=backend)
        self._failures.append(event)
        self.record_metric("runtime.failure_cluster_count", 1.0, source=source, cluster=event.cluster)
        return event

    def add_token_usage(self, prompt_tokens: int, completion_tokens: int) -> None:
        total = prompt_tokens + completion_tokens
        self._events.append(SpanEvent(name=f"tokens:{total}", duration_ms=0.0, status="ok"))
        self.record_metric("runtime.tokens_total", float(total))

    def flush(self) -> list[SpanEvent]:
        # In real deployment, this is where Phoenix exporter hooks in.
        events = list(self._events)
        self._events.clear()
        return events

    def flush_metrics(self) -> list[RuntimeMetric]:
        metrics = list(self._metrics)
        self._metrics.clear()
        return metrics

    def flush_failures(self) -> list[FailureEvent]:
        failures = list(self._failures)
        self._failures.clear()
        return failures

    @property
    def enabled(self) -> bool:
        return self._enabled
