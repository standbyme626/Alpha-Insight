#!/usr/bin/env python3
"""Generate frontend smoke evidence for Upgrade7 P1 console."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ui.typed_resource_client import FrontendResourceClient


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate frontend smoke evidence for Upgrade7 console.")
    parser.add_argument("--output", default="docs/evidence/upgrade7_frontend_console_smoke.json")
    parser.add_argument("--run-limit", type=int, default=20)
    parser.add_argument("--alert-limit", type=int, default=20)
    parser.add_argument("--evidence-limit", type=int, default=40)
    args = parser.parse_args()

    client = FrontendResourceClient()
    snapshot = client.build_snapshot(
        run_limit=args.run_limit,
        alert_limit=args.alert_limit,
        evidence_limit=args.evidence_limit,
    )
    tier_distribution: dict[str, int] = {}
    guarded_tier_distribution: dict[str, int] = {}
    for item in snapshot.alerts:
        tier = item.strategy_tier or "execution-ready"
        tier_distribution[tier] = tier_distribution.get(tier, 0) + 1
        if item.tier_guarded:
            guarded_tier_distribution[tier] = guarded_tier_distribution.get(tier, 0) + 1
    payload: dict[str, Any] = {
        "generated_at": _utc_now(),
        "db_path": snapshot.db_path,
        "counts": {
            "runs": len(snapshot.runs),
            "alerts": len(snapshot.alerts),
            "evidence": len(snapshot.evidence),
            "degradation_states": len(snapshot.degradation_states),
        },
        "samples": {
            "run_ids": [item.run_id for item in snapshot.runs[:5]],
            "alert_event_ids": [item.event_id for item in snapshot.alerts[:5]],
            "evidence_names": [item.name for item in snapshot.evidence[:8]],
            "degradation_state_keys": [item.state_key for item in snapshot.degradation_states[:8]],
        },
        "tier_governance": {
            "tier_distribution": tier_distribution,
            "guarded_tier_distribution": guarded_tier_distribution,
        },
        "frontend_contract": {
            "resources": ["runs", "alerts", "evidence", "degradation_states"],
            "typed_client": "ui.typed_resource_client.FrontendResourceClient",
            "console_entry": "ui.upgrade7_console.py",
            "next_console_entry": "web_console/app/(dashboard)/runs/page.tsx",
            "next_console_routes": ["/runs", "/alerts", "/evidence", "/governance"],
            "reference_mapping": {
                "next-shadcn-dashboard-starter": [
                    "sidebar layout shell",
                    "app router dashboard route organization"
                ],
                "nextjs-fastapi-template": [
                    "typed client pattern (lib/client.ts + server route bridge)"
                ],
                "react-admin/refine": [
                    "resource-first navigation (runs/alerts/evidence/governance)"
                ]
            }
        },
    }
    output = Path(args.output)
    _write_json(output, payload)
    print(f"[OK] {output}")


if __name__ == "__main__":
    main()
