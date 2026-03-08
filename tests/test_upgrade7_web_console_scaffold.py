from __future__ import annotations

import json
from pathlib import Path


def test_web_console_scaffold_and_reference_patterns_exist() -> None:
    root = Path("web_console")
    required_files = [
        root / "package.json",
        root / "app/layout.tsx",
        root / "app/globals.css",
        root / "app/(dashboard)/layout.tsx",
        root / "app/(dashboard)/runs/page.tsx",
        root / "app/(dashboard)/alerts/page.tsx",
        root / "app/(dashboard)/monitors/page.tsx",
        root / "app/(dashboard)/evidence/page.tsx",
        root / "app/(dashboard)/governance/page.tsx",
        root / "app/api/resources/monitors/route.ts",
        root / "lib/client.ts",
        root / "lib/resources.ts",
        root / "lib/parsers.ts",
        root / "lib/type_guards.ts",
        root / "components/dashboard-shell.tsx",
    ]
    missing = [str(path) for path in required_files if not path.exists()]
    assert not missing, f"missing files: {missing}"

    package = json.loads((root / "package.json").read_text(encoding="utf-8"))
    deps = package.get("dependencies", {})
    assert "next" in deps
    assert "react" in deps
    assert "react-dom" in deps

    runs_table = (root / "components/tables.tsx").read_text(encoding="utf-8")
    assert "runtime_budget_verdict" in runs_table


def test_exported_frontend_resources_contract() -> None:
    export_path = Path("docs/evidence/upgrade7_frontend_resources.json")
    if not export_path.exists():
        return
    payload = json.loads(export_path.read_text(encoding="utf-8"))

    for key in ("runs", "alerts", "evidence", "degradation_states"):
        assert key in payload
        assert isinstance(payload[key], list)
