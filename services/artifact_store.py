from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class EvidenceArtifactRecord(BaseModel):
    name: str
    path: str
    generated_at: str | None = None
    size_bytes: int
    summary: dict[str, Any] = Field(default_factory=dict)
    updated_at: str


class ArtifactStore:
    def __init__(self, *, evidence_dir: str | Path = "docs/evidence") -> None:
        self._evidence_dir = Path(evidence_dir)

    @staticmethod
    def _evidence_summary(payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {"type": type(payload).__name__}
        summary: dict[str, Any] = {}
        for key in (
            "generated_at",
            "summary",
            "mode",
            "runs",
            "merge_priority",
            "runtime_flags_applied",
            "base_connector_retry",
            "rss_connector_retry",
            "runtime_budget_verdict",
            "runtime_budget_reasons",
            "fault_injection_event_count",
            "fault_injection_events_total",
            "strategies_covered",
            "strategy_matrix",
            "strategy_tiers_covered",
            "tier_matrix",
            "tier_distribution",
            "guarded_tier_distribution",
            "snapshot_role",
            "primary_data_source",
        ):
            if key in payload:
                summary[key] = payload.get(key)
        return summary

    def list_evidence(self, *, limit: int = 100) -> list[EvidenceArtifactRecord]:
        if not self._evidence_dir.exists():
            return []
        files = sorted(self._evidence_dir.glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
        out: list[EvidenceArtifactRecord] = []
        for item in files[: max(1, int(limit))]:
            updated_at = datetime.fromtimestamp(item.stat().st_mtime, tz=timezone.utc).isoformat()
            summary: dict[str, Any] = {}
            generated_at: str | None = None
            try:
                payload = json.loads(item.read_text(encoding="utf-8"))
                summary = self._evidence_summary(payload)
                raw_generated_at = payload.get("generated_at") if isinstance(payload, dict) else None
                if isinstance(raw_generated_at, str):
                    generated_at = raw_generated_at
            except Exception:
                summary = {"parse_error": True}
            out.append(
                EvidenceArtifactRecord(
                    name=item.name,
                    path=str(item),
                    generated_at=generated_at,
                    size_bytes=int(item.stat().st_size),
                    summary=summary,
                    updated_at=updated_at,
                )
            )
        return out
