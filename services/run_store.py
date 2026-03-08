from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from services.store_adapter import SQLiteStoreAdapter


def _safe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {"raw": str(value)}
    if isinstance(parsed, dict):
        return parsed
    return {"raw": str(value)}


class RunResourceRecord(BaseModel):
    run_id: str
    request_id: str
    chat_id: str
    symbol: str
    summary: str
    key_metrics: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class AlertResourceRecord(BaseModel):
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


class DegradationStateRecord(BaseModel):
    state_key: str
    status: str
    reason: str
    triggered_at: str | None = None
    recovered_at: str | None = None
    updated_at: str


class MonitorResourceRecord(BaseModel):
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


class RunStore:
    def __init__(self, *, db_path: str | Path | None = None, adapter: SQLiteStoreAdapter | None = None) -> None:
        self._adapter = adapter if adapter is not None else SQLiteStoreAdapter(db_path)

    @property
    def db_path(self) -> Path:
        return self._adapter.db_path

    def get_run(self, *, run_id: str) -> RunResourceRecord | None:
        run_id_text = str(run_id).strip()
        if not run_id_text or not self._adapter.exists():
            return None
        with self._adapter.connect() as conn:
            if not self._adapter.table_exists(conn, "analysis_reports"):
                return None
            row = conn.execute(
                """
                SELECT run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at
                FROM analysis_reports
                WHERE run_id = ? OR request_id = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (run_id_text, run_id_text),
            ).fetchone()
        if row is None:
            return None
        return RunResourceRecord(
            run_id=str(row["run_id"]),
            request_id=str(row["request_id"]),
            chat_id=str(row["chat_id"]),
            symbol=str(row["symbol"]),
            summary=str(row["summary"]),
            key_metrics=_safe_dict(row["key_metrics"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def list_runs(self, *, limit: int = 50) -> list[RunResourceRecord]:
        if not self._adapter.exists():
            return []
        with self._adapter.connect() as conn:
            if not self._adapter.table_exists(conn, "analysis_reports"):
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
        return [
            RunResourceRecord(
                run_id=str(row["run_id"]),
                request_id=str(row["request_id"]),
                chat_id=str(row["chat_id"]),
                symbol=str(row["symbol"]),
                summary=str(row["summary"]),
                key_metrics=_safe_dict(row["key_metrics"]),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def list_alerts(self, *, limit: int = 100) -> list[AlertResourceRecord]:
        if not self._adapter.exists():
            return []
        with self._adapter.connect() as conn:
            required = ("watch_events", "notifications")
            if not all(self._adapter.table_exists(conn, name) for name in required):
                return []
            we_cols = self._adapter.table_columns(conn, "watch_events")
            n_cols = self._adapter.table_columns(conn, "notifications")
            has_watch_jobs = self._adapter.table_exists(conn, "watch_jobs")
            wj_cols = self._adapter.table_columns(conn, "watch_jobs") if has_watch_jobs else set()

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
            AlertResourceRecord(
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

    def list_degradation_states(self, *, limit: int = 200) -> list[DegradationStateRecord]:
        if not self._adapter.exists():
            return []
        with self._adapter.connect() as conn:
            if not self._adapter.table_exists(conn, "degradation_states"):
                return []
            rows = conn.execute(
                """
                SELECT state_key, status, reason, triggered_at, recovered_at, updated_at
                FROM degradation_states
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [
            DegradationStateRecord(
                state_key=str(row["state_key"]),
                status=str(row["status"]),
                reason=str(row["reason"] or ""),
                triggered_at=str(row["triggered_at"]) if row["triggered_at"] else None,
                recovered_at=str(row["recovered_at"]) if row["recovered_at"] else None,
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def list_monitors(self, *, limit: int = 200) -> list[MonitorResourceRecord]:
        if not self._adapter.exists():
            return []
        with self._adapter.connect() as conn:
            if not self._adapter.table_exists(conn, "watch_jobs"):
                return []
            rows = conn.execute(
                """
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, route_strategy, strategy_tier,
                       enabled, next_run_at, last_run_at, last_triggered_at, last_error, updated_at
                FROM watch_jobs
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()

        return [
            MonitorResourceRecord(
                job_id=str(row["job_id"]),
                chat_id=str(row["chat_id"]),
                symbol=str(row["symbol"]),
                market=str(row["market"]),
                interval_sec=int(row["interval_sec"]),
                threshold=float(row["threshold"]),
                mode=str(row["mode"]),
                scope=str(row["scope"]) if row["scope"] else "single",
                route_strategy=str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
                strategy_tier=str(row["strategy_tier"]) if row["strategy_tier"] else "execution-ready",
                enabled=bool(int(row["enabled"])),
                next_run_at=str(row["next_run_at"]),
                last_run_at=str(row["last_run_at"]) if row["last_run_at"] else None,
                last_triggered_at=str(row["last_triggered_at"]) if row["last_triggered_at"] else None,
                last_error=str(row["last_error"]) if row["last_error"] else None,
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]
