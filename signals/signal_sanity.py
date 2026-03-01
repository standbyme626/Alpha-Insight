"""Default signal plugin: normalize order and keep pipeline stable."""

from __future__ import annotations

from typing import Any


class SignalSanityPlugin:
    name = "signal_sanity"
    kind = "signals"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        signals = list(payload.get("signals", []) or [])
        preserve_order = bool(params.get("preserve_order", False))
        if not preserve_order:
            order = {"critical": 0, "high": 1, "normal": 2}
            signals = sorted(signals, key=lambda item: order.get(str(getattr(item, "priority", "")).lower(), 9))
        return {
            "signals": signals,
            "observability_tags": {
                "plugin_action": "signal_order_normalized",
                "trigger_id": str(getattr(context, "trigger_id", "")),
            },
        }

