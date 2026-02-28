#!/usr/bin/env python3
"""Generate Upgrade7 P2 overview evidence and a single smoke acceptance entry."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass(frozen=True)
class StepSpec:
    name: str
    script: str


def _run_step(step: StepSpec) -> dict[str, Any]:
    started = time.perf_counter()
    cmd = [sys.executable, step.script]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    elapsed = (time.perf_counter() - started) * 1000
    return {
        "name": step.name,
        "script": step.script,
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "duration_ms": round(elapsed, 3),
        "stdout_tail": (proc.stdout or "").strip().splitlines()[-6:],
        "stderr_tail": (proc.stderr or "").strip().splitlines()[-6:],
    }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"missing": True, "path": str(path)}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"invalid_json": True, "path": str(path)}
    if not isinstance(payload, dict):
        return {"invalid_payload": True, "path": str(path), "type": type(payload).__name__}
    return payload


def _summary_payload(artifacts: dict[str, dict[str, Any]], steps: list[dict[str, Any]]) -> dict[str, Any]:
    p2a = artifacts.get("upgrade7_p2_latency_error_budget_summary.json", {})
    p2b = artifacts.get("upgrade7_p2_channel_adapter_matrix.json", {})
    p2c = artifacts.get("upgrade7_p2_strategy_tier_matrix.json", {})
    frontend = artifacts.get("upgrade7_frontend_resources.json", {})
    return {
        "overall_ok": all(bool(item.get("ok", False)) for item in steps),
        "p2a_overall_verdict": p2a.get("overall_verdict", "unknown"),
        "p2b_strategies_covered": p2b.get("strategies_covered", []),
        "p2c_strategy_tiers_covered": p2c.get("strategy_tiers_covered", []),
        "p2c_guarded_tier_distribution": p2c.get("guarded_tier_distribution", {}),
        "frontend_tier_distribution": frontend.get("tier_distribution", {}),
        "frontend_guarded_tier_distribution": frontend.get("guarded_tier_distribution", {}),
    }


def main() -> None:
    steps = [
        StepSpec(name="p2a_fault_budget", script="scripts/upgrade7_p2_evidence.py"),
        StepSpec(name="p2b_channel_matrix", script="scripts/upgrade7_p2b_channel_adapter_evidence.py"),
        StepSpec(name="p2c_strategy_tier", script="scripts/upgrade7_p2c_strategy_tier_evidence.py"),
        StepSpec(name="frontend_resources_export", script="scripts/upgrade7_frontend_resources_export.py"),
        StepSpec(name="frontend_console_smoke", script="scripts/upgrade7_frontend_console_evidence.py"),
    ]
    step_rows = [_run_step(step) for step in steps]
    failed_steps = [item for item in step_rows if not item.get("ok", False)]
    if failed_steps:
        failed_names = ", ".join(str(item.get("name", "")) for item in failed_steps)
        raise SystemExit(f"upgrade7 P2 overview evidence failed: {failed_names}")

    artifact_paths = {
        "upgrade7_p2_fault_injection_budget.json": Path("docs/evidence/upgrade7_p2_fault_injection_budget.json"),
        "upgrade7_p2_latency_error_budget_summary.json": Path("docs/evidence/upgrade7_p2_latency_error_budget_summary.json"),
        "upgrade7_p2_channel_adapter_matrix.json": Path("docs/evidence/upgrade7_p2_channel_adapter_matrix.json"),
        "upgrade7_p2_strategy_tier_matrix.json": Path("docs/evidence/upgrade7_p2_strategy_tier_matrix.json"),
        "upgrade7_frontend_resources.json": Path("docs/evidence/upgrade7_frontend_resources.json"),
        "upgrade7_frontend_console_smoke.json": Path("docs/evidence/upgrade7_frontend_console_smoke.json"),
    }
    artifacts = {name: _load_json(path) for name, path in artifact_paths.items()}
    summary = _summary_payload(artifacts, step_rows)

    overview_payload = {
        "generated_at": _now(),
        "scope": "Upgrade7 P2 overview (A/B/C + frontend governance export)",
        "steps": step_rows,
        "artifacts": artifacts,
        "summary": summary,
    }
    smoke_payload = {
        "generated_at": _now(),
        "scope": "Upgrade7 P2 smoke e2e single acceptance entry",
        "summary": summary,
        "artifact_refs": {name: str(path) for name, path in artifact_paths.items()},
    }
    overview_path = Path("docs/evidence/upgrade7_p2_overview.json")
    smoke_path = Path("docs/evidence/upgrade7_p2_smoke_e2e.json")
    _write_json(overview_path, overview_payload)
    _write_json(smoke_path, smoke_payload)
    print(f"[OK] {overview_path}")
    print(f"[OK] {smoke_path}")


if __name__ == "__main__":
    main()
