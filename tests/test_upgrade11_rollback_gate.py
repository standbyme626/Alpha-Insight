from __future__ import annotations

import subprocess
from pathlib import Path


def test_upgrade11_frontend_fallback_gate_exists() -> None:
    resources = Path("web_console/lib/resources.ts").read_text(encoding="utf-8")

    assert "fetchUpstream(`/api/runs?limit=${normalizedLimit}`)" in resources
    assert "return snapshot.runs.slice(0, normalizedLimit);" in resources
    assert "fetchUpstream(`/api/alerts?limit=${normalizedLimit}`)" in resources
    assert "return snapshot.alerts.slice(0, normalizedLimit);" in resources
    assert "fetchUpstream(`/api/governance?limit=${normalizedLimit}`)" in resources
    assert "return snapshot.degradation_states.slice(0, normalizedLimit);" in resources
    assert "fetchUpstream(`/api/monitors?limit=${normalizedLimit}`)" in resources
    assert "return snapshot.monitors.slice(0, normalizedLimit);" in resources
    assert "fetchUpstream(`/api/evidence?limit=${normalizedLimit}`)" in resources
    assert "return snapshot.evidence.slice(0, normalizedLimit);" in resources
    assert "return applyEventFilters(deriveEventsFromSnapshot(snapshot), { since, limit });" in resources


def test_upgrade11_rollback_ports_documented() -> None:
    runbook = Path("docs/runbook.md").read_text(encoding="utf-8")
    assert "8501" in runbook
    assert "8502" in runbook
    assert "8503" in runbook
    assert "回退" in runbook


def test_upgrade11_env_governance_gate() -> None:
    gitignore = Path(".gitignore").read_text(encoding="utf-8")
    assert ".env" in gitignore
    assert "!.env.example" in gitignore
    assert Path(".env.example").exists()

    tracked_env = subprocess.run(["git", "ls-files", ".env"], check=True, capture_output=True, text=True).stdout.strip()
    assert tracked_env == ""
