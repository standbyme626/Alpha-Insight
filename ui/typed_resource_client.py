"""Typed resource client for Upgrade7 frontend console."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


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


class ConsoleSnapshot(BaseModel):
    generated_at: str
    db_path: str
    runs: list[RunResource]
    alerts: list[AlertResource]
    evidence: list[EvidenceResource]
    degradation_states: list[DegradationStateResource]


class FrontendResourceClient:
    """Typed read client for runs / alerts / evidence resources."""

    def __init__(
        self,
        *,
        db_path: str | Path | None = None,
        evidence_dir: str | Path = "docs/evidence",
    ) -> None:
        self._db_path = self._resolve_db_path(db_path)
        self._evidence_dir = Path(evidence_dir)

    @staticmethod
    def _resolve_db_path(db_path: str | Path | None) -> Path:
        if db_path is not None:
            return Path(db_path)
        env_path = os.getenv("ALPHA_INSIGHT_STORE_DB", "").strip()
        if env_path:
            return Path(env_path)
        candidates = [
            Path("storage/telegram_gateway_live.db"),
            Path("storage/telegram_gateway.db"),
            Path("storage/telegram_gateway_human_sim.db"),
            Path("storage/telegram_gateway_service_path.db"),
            Path("storage/telegram_gateway_e2e.db"),
        ]
        for item in candidates:
            if item.exists():
                return item
        return candidates[0]

    @property
    def db_path(self) -> Path:
        return self._db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    def _table_columns(self, conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}

    def list_runs(self, *, limit: int = 50) -> list[RunResource]:
        if not self._db_path.exists():
            return []
        with self._connect() as conn:
            if not self._table_exists(conn, "analysis_reports"):
                return []
            rows = conn.execute(
                """
                SELECT run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at
                FROM analysis_reports
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        out: list[RunResource] = []
        for row in rows:
            raw_metrics = str(row["key_metrics"] or "{}")
            try:
                metrics = json.loads(raw_metrics)
                if not isinstance(metrics, dict):
                    metrics = {"raw": raw_metrics}
            except json.JSONDecodeError:
                metrics = {"raw": raw_metrics}
            out.append(
                RunResource(
                    run_id=str(row["run_id"]),
                    request_id=str(row["request_id"]),
                    chat_id=str(row["chat_id"]),
                    symbol=str(row["symbol"]),
                    summary=str(row["summary"]),
                    key_metrics=metrics,
                    created_at=str(row["created_at"]),
                    updated_at=str(row["updated_at"]),
                )
            )
        return out

    def list_alerts(self, *, limit: int = 100) -> list[AlertResource]:
        if not self._db_path.exists():
            return []
        with self._connect() as conn:
            required = ("watch_events", "notifications")
            if not all(self._table_exists(conn, name) for name in required):
                return []
            we_cols = self._table_columns(conn, "watch_events")
            n_cols = self._table_columns(conn, "notifications")
            has_watch_jobs = self._table_exists(conn, "watch_jobs")
            wj_cols = self._table_columns(conn, "watch_jobs") if has_watch_jobs else set()

            symbol_expr = "''"
            if "symbol" in we_cols:
                symbol_expr = "we.symbol"
            elif has_watch_jobs and "job_id" in we_cols and "symbol" in wj_cols:
                symbol_expr = "wj.symbol"

            priority_expr = "we.priority" if "priority" in we_cols else "'unknown'"
            rule_expr = "we.rule" if "rule" in we_cols else "''"
            trigger_ts_expr = "we.trigger_ts" if "trigger_ts" in we_cols else "''"
            run_id_expr = "we.run_id" if "run_id" in we_cols else "NULL"
            tier_expr = (
                "we.strategy_tier"
                if "strategy_tier" in we_cols
                else ("wj.strategy_tier" if has_watch_jobs and "strategy_tier" in wj_cols else "'execution-ready'")
            )
            suppressed_expr = "n.suppressed_reason" if "suppressed_reason" in n_cols else "NULL"
            error_expr = "n.last_error" if "last_error" in n_cols else "NULL"
            updated_expr = "n.updated_at" if "updated_at" in n_cols else "''"
            state_expr = "n.state" if "state" in n_cols else "''"
            channel_expr = "n.channel" if "channel" in n_cols else "''"
            join_watch_jobs = ""
            if has_watch_jobs and "job_id" in we_cols:
                join_watch_jobs = "LEFT JOIN watch_jobs wj ON wj.job_id = we.job_id"

            rows = conn.execute(
                f"""
                SELECT we.event_id AS event_id,
                       {symbol_expr} AS symbol,
                       {priority_expr} AS priority,
                       {rule_expr} AS rule,
                       {tier_expr} AS strategy_tier,
                       {trigger_ts_expr} AS trigger_ts,
                       {run_id_expr} AS run_id,
                       {channel_expr} AS channel,
                       {state_expr} AS state,
                       {suppressed_expr} AS suppressed_reason,
                       {error_expr} AS last_error,
                       {updated_expr} AS updated_at
                FROM watch_events we
                JOIN notifications n ON n.event_id = we.event_id
                {join_watch_jobs}
                ORDER BY {updated_expr} DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [
            AlertResource(
                event_id=str(row["event_id"]),
                symbol=str(row["symbol"]),
                priority=str(row["priority"]),
                rule=str(row["rule"]),
                strategy_tier=str(row["strategy_tier"] or "execution-ready"),
                trigger_ts=str(row["trigger_ts"]),
                run_id=str(row["run_id"]) if row["run_id"] is not None else None,
                channel=str(row["channel"]),
                status=str(row["state"]),
                tier_guarded=str(row["suppressed_reason"] or "").startswith("strategy_tier_guard"),
                suppressed_reason=str(row["suppressed_reason"]) if row["suppressed_reason"] is not None else None,
                last_error=str(row["last_error"]) if row["last_error"] is not None else None,
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def list_degradation_states(self) -> list[DegradationStateResource]:
        if not self._db_path.exists():
            return []
        with self._connect() as conn:
            if not self._table_exists(conn, "degradation_states"):
                return []
            rows = conn.execute(
                """
                SELECT state_key, status, reason, triggered_at, recovered_at, updated_at
                FROM degradation_states
                ORDER BY updated_at DESC
                """
            ).fetchall()
        return [
            DegradationStateResource(
                state_key=str(row["state_key"]),
                status=str(row["status"]),
                reason=str(row["reason"] or ""),
                triggered_at=str(row["triggered_at"]) if row["triggered_at"] is not None else None,
                recovered_at=str(row["recovered_at"]) if row["recovered_at"] is not None else None,
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

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
        ):
            if key in payload:
                summary[key] = payload.get(key)
        return summary

    def list_evidence(self, *, limit: int = 100) -> list[EvidenceResource]:
        if not self._evidence_dir.exists():
            return []
        files = sorted(self._evidence_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        out: list[EvidenceResource] = []
        for path in files[: max(1, int(limit))]:
            updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
            summary: dict[str, Any] = {}
            generated_at: str | None = None
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                summary = self._evidence_summary(payload)
                generated_raw = payload.get("generated_at") if isinstance(payload, dict) else None
                if isinstance(generated_raw, str):
                    generated_at = generated_raw
            except Exception:
                summary = {"parse_error": True}
            out.append(
                EvidenceResource(
                    name=path.name,
                    path=str(path),
                    generated_at=generated_at,
                    size_bytes=int(path.stat().st_size),
                    summary=summary,
                    updated_at=updated_at,
                )
            )
        return out

    def build_snapshot(
        self,
        *,
        run_limit: int = 50,
        alert_limit: int = 100,
        evidence_limit: int = 100,
    ) -> ConsoleSnapshot:
        return ConsoleSnapshot(
            generated_at=_utc_now(),
            db_path=str(self._db_path),
            runs=self.list_runs(limit=run_limit),
            alerts=self.list_alerts(limit=alert_limit),
            evidence=self.list_evidence(limit=evidence_limit),
            degradation_states=self.list_degradation_states(),
        )
