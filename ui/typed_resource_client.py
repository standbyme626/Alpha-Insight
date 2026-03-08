"""Compatibility typed resource client for export/evidence tooling."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from services.artifact_store import ArtifactStore
from services.run_store import RunStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class RunResource(BaseModel):
    run_id: str
    request_id: str
    chat_id: str
    symbol: str
    summary: str
    key_metrics: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class AlertResource(BaseModel):
    event_id: str
    symbol: str
    priority: str
    rule: str
    strategy_tier: str = "execution-ready"
    trigger_ts: str
    run_id: str | None = None
    channel: str = ""
    status: str = ""
    tier_guarded: bool = False
    suppressed_reason: str | None = None
    last_error: str | None = None
    updated_at: str = ""


class EvidenceResource(BaseModel):
    name: str
    path: str
    generated_at: str | None = None
    size_bytes: int
    summary: dict[str, Any] = Field(default_factory=dict)
    updated_at: str


class DegradationStateResource(BaseModel):
    state_key: str
    status: str
    reason: str
    triggered_at: str | None = None
    recovered_at: str | None = None
    updated_at: str


class MonitorResource(BaseModel):
    job_id: str
    chat_id: str
    symbol: str
    market: str
    interval_sec: int
    threshold: float
    mode: str
    scope: str
    route_strategy: str
    strategy_tier: str
    enabled: bool
    next_run_at: str
    last_run_at: str | None = None
    last_triggered_at: str | None = None
    last_error: str | None = None
    updated_at: str


class ConsoleSnapshot(BaseModel):
    generated_at: str
    db_path: str
    runs: list[RunResource]
    alerts: list[AlertResource]
    evidence: list[EvidenceResource]
    degradation_states: list[DegradationStateResource]
    monitors: list[MonitorResource]


class FrontendResourceClient:
    """Compatibility layer used by export scripts and smoke evidence tooling."""

    def __init__(
        self,
        *,
        db_path: str | Path | None = None,
        evidence_dir: str | Path = "docs/evidence",
    ) -> None:
        self._run_store = RunStore(db_path=db_path)
        self._artifact_store = ArtifactStore(evidence_dir=evidence_dir)

    @property
    def db_path(self) -> Path:
        return self._run_store.db_path

    def list_runs(self, *, limit: int = 50) -> list[RunResource]:
        return [RunResource.model_validate(item.model_dump(mode="python")) for item in self._run_store.list_runs(limit=limit)]

    def list_alerts(self, *, limit: int = 100) -> list[AlertResource]:
        return [AlertResource.model_validate(item.model_dump(mode="python")) for item in self._run_store.list_alerts(limit=limit)]

    def list_degradation_states(self, *, limit: int = 200) -> list[DegradationStateResource]:
        return [
            DegradationStateResource.model_validate(item.model_dump(mode="python"))
            for item in self._run_store.list_degradation_states(limit=limit)
        ]

    def list_evidence(self, *, limit: int = 100) -> list[EvidenceResource]:
        return [
            EvidenceResource.model_validate(item.model_dump(mode="python"))
            for item in self._artifact_store.list_evidence(limit=limit)
        ]

    def list_monitors(self, *, limit: int = 200) -> list[MonitorResource]:
        return [MonitorResource.model_validate(item.model_dump(mode="python")) for item in self._run_store.list_monitors(limit=limit)]

    def build_snapshot(
        self,
        *,
        run_limit: int = 50,
        alert_limit: int = 100,
        evidence_limit: int = 100,
    ) -> ConsoleSnapshot:
        return ConsoleSnapshot(
            generated_at=_utc_now(),
            db_path=str(self.db_path),
            runs=self.list_runs(limit=run_limit),
            alerts=self.list_alerts(limit=alert_limit),
            evidence=self.list_evidence(limit=evidence_limit),
            degradation_states=self.list_degradation_states(),
            monitors=self.list_monitors(),
        )
