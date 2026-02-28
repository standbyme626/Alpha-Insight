from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from ui.typed_resource_client import FrontendResourceClient


def _seed_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE analysis_reports(
            run_id TEXT PRIMARY KEY,
            request_id TEXT,
            chat_id TEXT,
            symbol TEXT,
            summary TEXT,
            key_metrics TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE watch_events(
            event_id TEXT PRIMARY KEY,
            symbol TEXT,
            priority TEXT,
            rule TEXT,
            trigger_ts TEXT,
            run_id TEXT,
            strategy_tier TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE notifications(
            notification_id TEXT PRIMARY KEY,
            event_id TEXT,
            channel TEXT,
            state TEXT,
            suppressed_reason TEXT,
            last_error TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE degradation_states(
            state_key TEXT PRIMARY KEY,
            status TEXT,
            triggered_at TEXT,
            recovered_at TEXT,
            reason TEXT,
            updated_at TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO analysis_reports(run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at)
        VALUES(?,?,?,?,?,?,?,?)
        """,
        (
            "run-1",
            "req-1",
            "chat-1",
            "AAPL",
            "summary",
            json.dumps(
                {
                    "runtime_success": True,
                    "runtime_retry_count": 0,
                    "runtime_budget_verdict": "pass",
                    "runtime_latency_p95_ms": 120.0,
                    "runtime_error_rate": 0.0,
                }
            ),
            "2026-02-28T00:00:00+00:00",
            "2026-02-28T00:00:00+00:00",
        ),
    )
    cur.execute(
        """
        INSERT INTO watch_events(event_id, symbol, priority, rule, trigger_ts, run_id, strategy_tier)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            "evt-1",
            "AAPL",
            "high",
            "price_or_rsi",
            "2026-02-28T00:01:00+00:00",
            "run-1",
            "research-only",
        ),
    )
    cur.execute(
        """
        INSERT INTO notifications(notification_id, event_id, channel, state, suppressed_reason, last_error, updated_at)
        VALUES(?,?,?,?,?,?,?)
        """,
        (
            "ntf-1",
            "evt-1",
            "telegram",
            "delivered",
            None,
            None,
            "2026-02-28T00:02:00+00:00",
        ),
    )
    cur.execute(
        """
        INSERT INTO degradation_states(state_key, status, triggered_at, recovered_at, reason, updated_at)
        VALUES(?,?,?,?,?,?)
        """,
        (
            "no_monitor_push",
            "recovered",
            "2026-02-28T00:00:00+00:00",
            "2026-02-28T00:05:00+00:00",
            "recovered",
            "2026-02-28T00:05:00+00:00",
        ),
    )
    conn.commit()
    conn.close()


def test_frontend_typed_resource_client_reads_resources(tmp_path) -> None:  # noqa: ANN001
    db_path = tmp_path / "store.db"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "upgrade7_plugin_loading_matrix.json").write_text(
        json.dumps({"generated_at": "2026-02-28T00:00:00+00:00", "runtime_flags_applied": True}),
        encoding="utf-8",
    )
    _seed_db(db_path)

    client = FrontendResourceClient(db_path=db_path, evidence_dir=evidence_dir)
    runs = client.list_runs(limit=10)
    alerts = client.list_alerts(limit=10)
    evidence = client.list_evidence(limit=10)
    states = client.list_degradation_states()

    assert len(runs) == 1
    assert runs[0].run_id == "run-1"
    assert runs[0].key_metrics["runtime_success"] is True
    assert runs[0].key_metrics["runtime_budget_verdict"] == "pass"

    assert len(alerts) == 1
    assert alerts[0].event_id == "evt-1"
    assert alerts[0].status == "delivered"
    assert alerts[0].strategy_tier == "research-only"

    assert len(evidence) == 1
    assert evidence[0].name == "upgrade7_plugin_loading_matrix.json"
    assert evidence[0].summary["runtime_flags_applied"] is True

    assert len(states) == 1
    assert states[0].state_key == "no_monitor_push"


def test_frontend_snapshot_contract(tmp_path) -> None:  # noqa: ANN001
    db_path = tmp_path / "store.db"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    _seed_db(db_path)
    (evidence_dir / "x.json").write_text(json.dumps({"k": "v"}), encoding="utf-8")

    client = FrontendResourceClient(db_path=db_path, evidence_dir=evidence_dir)
    snapshot = client.build_snapshot(run_limit=5, alert_limit=5, evidence_limit=5)

    assert snapshot.db_path.endswith("store.db")
    assert len(snapshot.runs) == 1
    assert len(snapshot.alerts) == 1
    assert len(snapshot.evidence) == 1
    assert len(snapshot.degradation_states) == 1


def test_frontend_evidence_summary_includes_p2b_channel_matrix_keys(tmp_path) -> None:  # noqa: ANN001
    db_path = tmp_path / "store.db"
    evidence_dir = tmp_path / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    _seed_db(db_path)
    (evidence_dir / "upgrade7_p2_channel_adapter_matrix.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-02-28T00:00:00+00:00",
                "strategies_covered": ["telegram_only", "email_only"],
                "strategy_matrix": [{"strategy": "telegram_only", "dispatch_counts": {"telegram": 1}}],
            }
        ),
        encoding="utf-8",
    )

    client = FrontendResourceClient(db_path=db_path, evidence_dir=evidence_dir)
    evidence = client.list_evidence(limit=5)

    assert evidence
    assert evidence[0].summary["strategies_covered"] == ["telegram_only", "email_only"]
