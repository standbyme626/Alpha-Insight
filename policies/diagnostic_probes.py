"""Diagnostic plugins used by contract tests/evidence generation."""

from __future__ import annotations

import asyncio
from typing import Any


class TimeoutProbePolicyPlugin:
    name = "timeout_probe"
    kind = "policies"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        sleep_ms = int(params.get("sleep_ms", 300))
        await asyncio.sleep(max(0.0, sleep_ms / 1000.0))
        return {"allow_triggered_research": bool(payload.get("allow_triggered_research", True))}


class ErrorProbePolicyPlugin:
    name = "error_probe"
    kind = "policies"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        raise RuntimeError(str(params.get("message", "probe failure")))

