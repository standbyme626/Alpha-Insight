"""Deterministic fault-injection harness for Upgrade7 P2-A."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

FaultType = Literal["timeout", "upstream_5xx", "parse", "rate_limit", "sandbox_failure"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class FaultInjectionRule:
    node: str
    fault: FaultType
    rate: float = 1.0
    enabled: bool = True

    def matches(self, node: str, fault: FaultType | None = None) -> bool:
        if not self.enabled:
            return False
        if not self.node.strip():
            return False
        if self.node.endswith("*"):
            if not node.startswith(self.node[:-1]):
                return False
        elif self.node != node:
            return False
        if fault is not None and self.fault != fault:
            return False
        return True


@dataclass(frozen=True)
class FaultInjectionEvent:
    node: str
    fault: FaultType
    rate: float
    sample: float
    reason: str
    ts: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "fault": self.fault,
            "rate": self.rate,
            "sample": self.sample,
            "reason": self.reason,
            "ts": self.ts,
        }


@dataclass(frozen=True)
class FaultSemantic:
    fault: FaultType
    error_type: str
    message: str
    retriable: bool
    default_retry_delta: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "fault": self.fault,
            "error_type": self.error_type,
            "message": self.message,
            "retriable": self.retriable,
            "default_retry_delta": self.default_retry_delta,
        }


def _normalize_fault(value: Any) -> FaultType | None:
    raw = str(value or "").strip().lower()
    allowed: set[str] = {"timeout", "upstream_5xx", "parse", "rate_limit", "sandbox_failure"}
    if raw in allowed:
        return raw  # type: ignore[return-value]
    return None


def fault_semantic(fault: FaultType, *, node: str = "") -> FaultSemantic:
    node_text = f" at node={node}" if node else ""
    if fault == "timeout":
        return FaultSemantic(
            fault=fault,
            error_type="TimeoutError",
            message=f"fault injection timeout{node_text}: execution timed out",
            retriable=True,
            default_retry_delta=1,
        )
    if fault == "upstream_5xx":
        return FaultSemantic(
            fault=fault,
            error_type="UpstreamServiceError",
            message=f"fault injection upstream_5xx{node_text}: upstream server error 503",
            retriable=True,
            default_retry_delta=1,
        )
    if fault == "rate_limit":
        return FaultSemantic(
            fault=fault,
            error_type="RateLimitError",
            message=f"fault injection rate_limit{node_text}: rate limit 429 too many requests",
            retriable=True,
            default_retry_delta=1,
        )
    if fault == "parse":
        return FaultSemantic(
            fault=fault,
            error_type="ParseError",
            message=f"fault injection parse{node_text}: parse error invalid payload",
            retriable=False,
            default_retry_delta=0,
        )
    return FaultSemantic(
        fault=fault,
        error_type="SandboxExecutionError",
        message=f"fault injection sandbox_failure{node_text}: sandbox runtime failed",
        retriable=False,
        default_retry_delta=0,
    )


def _normalize_rate(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    if parsed <= 0:
        return 0.0
    if parsed >= 1:
        return 1.0
    return parsed


def _parse_rules(raw_rules: Any) -> list[FaultInjectionRule]:
    out: list[FaultInjectionRule] = []
    if isinstance(raw_rules, list):
        for item in raw_rules:
            if not isinstance(item, dict):
                continue
            fault = _normalize_fault(item.get("fault"))
            if fault is None:
                continue
            node = str(item.get("node", "")).strip()
            if not node:
                continue
            out.append(
                FaultInjectionRule(
                    node=node,
                    fault=fault,
                    rate=_normalize_rate(item.get("rate", 1.0)),
                    enabled=bool(item.get("enabled", True)),
                )
            )
    elif isinstance(raw_rules, dict):
        for node, node_rules in raw_rules.items():
            if not isinstance(node_rules, dict):
                continue
            for fault_key, rate in node_rules.items():
                fault = _normalize_fault(fault_key)
                if fault is None:
                    continue
                out.append(
                    FaultInjectionRule(
                        node=str(node).strip(),
                        fault=fault,
                        rate=_normalize_rate(rate),
                        enabled=True,
                    )
                )
    return out


class FaultInjector:
    """Controlled fault injection with node-level matching and probability."""

    def __init__(
        self,
        *,
        enabled: bool = False,
        rules: list[FaultInjectionRule] | None = None,
        seed: int | None = None,
    ) -> None:
        self._enabled = bool(enabled)
        self._rules = list(rules or [])
        self._rng = random.Random(seed)

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self._rules)

    @property
    def rules(self) -> list[FaultInjectionRule]:
        return list(self._rules)

    @classmethod
    def disabled(cls) -> "FaultInjector":
        return cls(enabled=False, rules=[])

    @classmethod
    def from_payload(cls, payload: dict[str, Any] | None) -> "FaultInjector":
        if not isinstance(payload, dict):
            return cls.disabled()
        enabled = bool(payload.get("enabled", False))
        rules = _parse_rules(payload.get("rules"))
        seed_raw = payload.get("seed")
        seed: int | None = None
        if seed_raw is not None:
            try:
                seed = int(seed_raw)
            except Exception:
                seed = None
        return cls(enabled=enabled, rules=rules, seed=seed)

    def maybe_inject(
        self,
        *,
        node: str,
        allowed_faults: tuple[FaultType, ...] | None = None,
    ) -> FaultInjectionEvent | None:
        if not self.enabled:
            return None
        for rule in self._rules:
            if not rule.matches(node):
                continue
            if allowed_faults is not None and rule.fault not in allowed_faults:
                continue
            if rule.rate <= 0:
                continue
            sample = self._rng.random()
            if sample > rule.rate:
                continue
            return FaultInjectionEvent(
                node=node,
                fault=rule.fault,
                rate=rule.rate,
                sample=sample,
                reason=f"injected {rule.fault} at node={node}",
                ts=_utc_now(),
            )
        return None


def resolve_fault_injection(runtime_flags: dict[str, Any] | None) -> tuple[dict[str, Any], FaultInjector]:
    """Split runtime flags into typed runtime config flags + fault-injection flags."""
    if not isinstance(runtime_flags, dict):
        return {}, FaultInjector.disabled()
    copied = dict(runtime_flags)
    fault_payload = copied.pop("fault_injection", None)
    injector = FaultInjector.from_payload(fault_payload if isinstance(fault_payload, dict) else None)
    return copied, injector
