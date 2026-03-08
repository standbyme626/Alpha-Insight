"""Standardized tool result contract with legacy adapters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

TOOL_RESULT_SCHEMA_VERSION = "upgrade10.tool_result.v1"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ToolResult:
    source: str
    ts: str
    confidence: float
    schema_version: str = TOOL_RESULT_SCHEMA_VERSION
    raw: Any = None
    error: str = ""
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.error.strip() == ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_tool_result(
    *,
    source: str,
    confidence: float,
    raw: Any,
    error: str = "",
    meta: dict[str, Any] | None = None,
) -> ToolResult:
    return ToolResult(
        source=source,
        ts=utc_now_iso(),
        confidence=float(max(0.0, min(1.0, confidence))),
        raw=raw,
        error=str(error).strip(),
        meta=dict(meta or {}),
    )
