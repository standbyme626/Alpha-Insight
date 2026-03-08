from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from services.artifact_store import ArtifactStore
from services.run_store import RunStore
from services.telegram_store import TelegramTaskStore
from ui.typed_resource_client import FrontendResourceClient


def _seed(db_path: Path, evidence_dir: Path) -> None:
    store = TelegramTaskStore(db_path)
    store.upsert_telegram_chat(chat_id="chat-1", user_id="u-1", username="demo")
    store.upsert_analysis_report(
        run_id="run-1",
        request_id="req-1",
        chat_id="chat-1",
        symbol="AAPL",
        summary="ok",
        key_metrics={"runtime_success": True, "runtime_budget_verdict": "pass"},
    )
    job = store.create_watch_job(
        chat_id="chat-1",
        symbol="AAPL",
        interval_sec=300,
        threshold=0.03,
        mode="anomaly",
        route_strategy="dual_channel",
        strategy_tier="research-only",
    )
    event_id, _ = store.record_watch_event_if_new(
        job_id=job.job_id,
        symbol="AAPL",
        trigger_ts=datetime.now(timezone.utc),
        price=100.0,
        pct_change=1.2,
        reason="test",
        rule="price_or_rsi",
        priority="high",
        strategy_tier="research-only",
        run_id="run-1",
    )
    store.upsert_notification_state(
        event_id=event_id,
        channel="telegram",
        state="suppressed",
        suppressed_reason="strategy_tier_guard_research_only",
    )
    store.set_degradation_state(state_key="no_monitor_push", status="active", reason="push fail")

    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "upgrade10_store_layers.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-03-08T00:00:00+00:00",
                "strategies_covered": ["dual_channel"],
                "summary": "ok",
            }
        ),
        encoding="utf-8",
    )


def test_run_store_and_artifact_store(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    evidence_dir = tmp_path / "evidence"
    _seed(db_path, evidence_dir)

    run_store = RunStore(db_path=db_path)
    artifact_store = ArtifactStore(evidence_dir=evidence_dir)

    runs = run_store.list_runs(limit=10)
    alerts = run_store.list_alerts(limit=10)
    monitors = run_store.list_monitors(limit=10)
    states = run_store.list_degradation_states(limit=10)
    evidence = artifact_store.list_evidence(limit=10)

    assert len(runs) == 1
    assert runs[0].run_id == "run-1"
    assert len(alerts) == 1
    assert alerts[0].tier_guarded is True
    assert len(monitors) == 1
    assert monitors[0].strategy_tier == "research-only"
    assert len(states) == 1
    assert states[0].state_key == "no_monitor_push"
    assert len(evidence) == 1
    assert evidence[0].summary["strategies_covered"] == ["dual_channel"]


def test_typed_client_compatibility_wrapper_uses_store_layer(tmp_path: Path) -> None:
    db_path = tmp_path / "store.db"
    evidence_dir = tmp_path / "evidence"
    _seed(db_path, evidence_dir)

    client = FrontendResourceClient(db_path=db_path, evidence_dir=evidence_dir)

    assert len(client.list_runs(limit=10)) == 1
    assert len(client.list_alerts(limit=10)) == 1
    assert len(client.list_degradation_states(limit=10)) == 1
    assert len(client.list_evidence(limit=10)) == 1
