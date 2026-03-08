from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from services.events_read_model import EventsReadModel
from services.resource_api import RESOURCE_API_SCHEMA_VERSION, ResourceAPIService, create_resource_api_app
from services.telegram_store import TelegramTaskStore


def _seed_store(db_path: Path, evidence_dir: Path) -> TelegramTaskStore:
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

    event_id_ok, _ = store.record_watch_event_if_new(
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
    store.upsert_notification_state(event_id=event_id_ok, channel="telegram", state="delivered")

    event_id_fail, _ = store.record_watch_event_if_new(
        job_id=job.job_id,
        symbol="AAPL",
        trigger_ts=datetime.now(timezone.utc),
        price=101.0,
        pct_change=1.8,
        reason="test-failure",
        rule="volatility_spike",
        priority="high",
        strategy_tier="research-only",
        run_id="run-1",
    )
    store.upsert_notification_state(
        event_id=event_id_fail,
        channel="telegram",
        state="retry_pending",
        last_error="upstream unavailable",
    )

    event_id_guard, _ = store.record_watch_event_if_new(
        job_id=job.job_id,
        symbol="AAPL",
        trigger_ts=datetime.now(timezone.utc),
        price=102.0,
        pct_change=2.1,
        reason="test-guard",
        rule="strategy_tier_guard",
        priority="medium",
        strategy_tier="research-only",
        run_id="run-1",
    )
    store.upsert_notification_state(
        event_id=event_id_guard,
        channel="telegram",
        state="suppressed",
        suppressed_reason="strategy_tier_guard_research_only",
    )

    store.set_degradation_state(state_key="no_monitor_push", status="active", reason="push fail")
    store.set_degradation_state(state_key="no_monitor_push", status="recovered", reason="recovered")
    store.record_metric(metric_name="push_attempt", metric_value=10)
    store.record_metric(metric_name="push_success", metric_value=9)

    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "upgrade10_test_evidence.json").write_text(
        json.dumps({"generated_at": "2026-03-08T00:00:00+00:00", "summary": "ok"}),
        encoding="utf-8",
    )
    return store


def test_events_read_model_emits_timeline(tmp_path: Path) -> None:
    db_path = tmp_path / "resource_api.db"
    evidence_dir = tmp_path / "evidence"
    _seed_store(db_path, evidence_dir)

    model = EventsReadModel(db_path)
    events = model.list_events(limit=20)
    assert events
    event_types = {item["event_type"] for item in events}
    assert "degrade_started" in event_types
    assert "recover_finished" in event_types
    assert "guard_triggered" in event_types
    assert "delivery_failed" in event_types


def test_resource_api_service_returns_realtime_payloads(tmp_path: Path) -> None:
    db_path = tmp_path / "resource_api.db"
    evidence_dir = tmp_path / "evidence"
    store = _seed_store(db_path, evidence_dir)

    svc = ResourceAPIService(store=store, evidence_dir=evidence_dir)
    assert len(svc.list_runs(limit=10)) == 1
    assert len(svc.list_alerts(limit=10)) >= 2
    assert len(svc.list_monitors(limit=10)) == 1
    assert len(svc.list_events(limit=20)) >= 2
    governance = svc.list_governance(limit=20)
    assert governance["push_attempt_24h"] == 1
    assert governance["push_success_24h"] == 1
    assert len(svc.list_evidence(limit=10)) == 1


@pytest.mark.asyncio
async def test_resource_api_http_routes(tmp_path: Path) -> None:
    db_path = tmp_path / "resource_api.db"
    evidence_dir = tmp_path / "evidence"
    store = _seed_store(db_path, evidence_dir)
    app = create_resource_api_app(ResourceAPIService(store=store, evidence_dir=evidence_dir))

    async with TestServer(app) as server:
        async with TestClient(server) as client:
            runs_resp = await client.get("/api/runs?limit=5")
            runs_payload = await runs_resp.json()
            assert runs_resp.status == 200
            assert runs_payload["schema_version"] == RESOURCE_API_SCHEMA_VERSION
            assert isinstance(runs_payload["data"], list)
            run_id = runs_payload["data"][0]["run_id"]

            run_resp = await client.get(f"/api/runs/{run_id}")
            run_payload = await run_resp.json()
            assert run_resp.status == 200
            assert run_payload["data"]["run_id"] == run_id

            events_resp = await client.get("/api/events?limit=10")
            events_payload = await events_resp.json()
            assert events_resp.status == 200
            assert events_payload["schema_version"] == RESOURCE_API_SCHEMA_VERSION
            assert isinstance(events_payload["data"], list)
            assert any(item["event_type"] == "guard_triggered" for item in events_payload["data"])
