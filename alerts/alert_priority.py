"""Default alert plugin: keep critical alerts first for dispatch."""

from __future__ import annotations

from typing import Any


class AlertPriorityPlugin:
    name = "alert_priority"
    kind = "alerts"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        selected = list(payload.get("selected_alerts", []) or [])
        strict = bool(params.get("strict_priority", True))
        if strict:
            order = {"critical": 0, "high": 1, "normal": 2}
            selected = sorted(selected, key=lambda item: order.get(str(getattr(item, "priority", "")).lower(), 9))
        return {
            "selected_alerts": selected,
            "observability_tags": {
                "plugin_action": "alert_priority_applied",
                "trigger_id": str(getattr(context, "trigger_id", "")),
            },
        }

