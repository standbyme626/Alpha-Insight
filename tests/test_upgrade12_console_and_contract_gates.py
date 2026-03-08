from __future__ import annotations

from pathlib import Path


def test_upgrade12_console_chinese_bilingual_gate() -> None:
    shell = Path("web_console/components/dashboard-shell.tsx").read_text(encoding="utf-8")
    runs_page = Path("web_console/app/(dashboard)/runs/page.tsx").read_text(encoding="utf-8")
    alerts_page = Path("web_console/app/(dashboard)/alerts/page.tsx").read_text(encoding="utf-8")

    assert "升级12 控制台" in shell
    assert "运行记录 Runs" in shell
    assert "自动刷新" in runs_page
    assert "立即刷新" in runs_page
    assert "事件流状态" in alerts_page


def test_upgrade12_sse_routes_exist() -> None:
    events_stream = Path("web_console/app/api/resources/events/stream/route.ts")
    alerts_stream = Path("web_console/app/api/resources/alerts/stream/route.ts")
    assert events_stream.exists()
    assert alerts_stream.exists()

    events_text = events_stream.read_text(encoding="utf-8")
    alerts_text = alerts_stream.read_text(encoding="utf-8")
    assert "text/event-stream" in events_text
    assert "text/event-stream" in alerts_text


def test_upgrade12_resource_contract_version_gate() -> None:
    backend_api = Path("services/resource_api.py").read_text(encoding="utf-8")
    frontend_types = Path("web_console/lib/types.ts").read_text(encoding="utf-8")

    assert 'RESOURCE_API_SCHEMA_VERSION = "upgrade12.resource_api.v2"' in backend_api
    assert '"upgrade12.resource.events.v1"' in backend_api
    assert 'RESOURCE_API_SCHEMA_VERSION = "upgrade12.resource_api.v2"' in frontend_types
    assert '"upgrade12.resource.events.v1"' in frontend_types


def test_upgrade12_docs_closure_gate() -> None:
    compliance = Path("docs/compliance.md").read_text(encoding="utf-8")
    systemd_doc = Path("deploy/systemd/README.md").read_text(encoding="utf-8")
    webhook_contract = Path("docs/webhook_contract.md").read_text(encoding="utf-8")

    assert "Remaining Verify Items" not in compliance
    assert "Status: closed on Upgrade12" in compliance
    assert "Log Rotation" in systemd_doc
    assert "Backup and Restore" in systemd_doc
    assert "Staging vs Production" in systemd_doc
    assert "Contract Change Log" in webhook_contract
