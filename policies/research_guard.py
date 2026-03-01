"""Default policy plugin: optional safety gate for triggered research."""

from __future__ import annotations

from typing import Any


class ResearchGuardPolicyPlugin:
    name = "research_guard"
    kind = "policies"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        params: dict[str, Any],
        context: Any,  # noqa: ANN401
    ) -> dict[str, Any]:
        selected_alerts = list(payload.get("selected_alerts", []) or [])
        current_allow = bool(payload.get("allow_triggered_research", True))
        max_critical = int(params.get("max_critical_per_cycle", 1000))
        critical_count = sum(1 for item in selected_alerts if str(getattr(item, "priority", "")).lower() == "critical")
        allow = current_allow and critical_count <= max_critical
        return {
            "allow_triggered_research": allow,
            "policy_note": f"critical_count={critical_count}, max={max_critical}",
            "observability_tags": {
                "plugin_action": "research_gate_evaluated",
                "trigger_id": str(getattr(context, "trigger_id", "")),
            },
        }

