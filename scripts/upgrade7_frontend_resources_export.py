#!/usr/bin/env python3
"""Export typed frontend resources for the Next.js Upgrade7 console."""

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
    parser = argparse.ArgumentParser(description="Export Upgrade7 frontend resources")
    parser.add_argument("--output", default="docs/evidence/upgrade7_frontend_resources.json")
    parser.add_argument("--run-limit", type=int, default=100)
    parser.add_argument("--alert-limit", type=int, default=200)
    parser.add_argument("--evidence-limit", type=int, default=200)
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
    payload = {
        "generated_at": _utc_now(),
        "db_path": snapshot.db_path,
        "runs": [item.model_dump(mode="python") for item in snapshot.runs],
        "alerts": [item.model_dump(mode="python") for item in snapshot.alerts],
        "evidence": [item.model_dump(mode="python") for item in snapshot.evidence],
        "degradation_states": [item.model_dump(mode="python") for item in snapshot.degradation_states],
        "tier_distribution": tier_distribution,
        "guarded_tier_distribution": guarded_tier_distribution,
    }
    output = Path(args.output)
    _write_json(output, payload)
    print(f"[OK] {output}")


if __name__ == "__main__":
    main()
