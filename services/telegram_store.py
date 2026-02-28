from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from core.strategy_tier import DEFAULT_STRATEGY_TIER, normalize_strategy_tier


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


@dataclass
class AnalysisRequestRecord:
    request_id: str
    update_id: int
    chat_id: str
    payload: dict[str, Any]
    status: str
    run_id: str | None
    created_at: str
    updated_at: str
    last_error: str | None


@dataclass
class WatchJobRecord:
    job_id: str
    chat_id: str
    symbol: str
    market: str
    interval_sec: int
    threshold: float
    mode: str
    scope: str
    group_id: str | None
    route_strategy: str
    strategy_tier: str
    template_id: str | None
    enabled: bool
    next_run_at: str
    created_at: str
    updated_at: str
    last_run_at: str | None
    last_triggered_at: str | None
    last_error: str | None


@dataclass
class DueWatchJob:
    job_id: str
    chat_id: str
    symbol: str
    market: str
    interval_sec: int
    threshold: float
    mode: str
    scope: str
    group_id: str | None
    route_strategy: str
    strategy_tier: str
    next_run_at: str


@dataclass
class DueNotification:
    notification_id: str
    event_id: str
    channel: str
    retry_count: int
    last_error: str | None


@dataclass
class DueAnalysisRecovery:
    request_id: str
    chat_id: str
    symbol: str
    retry_count: int


@dataclass
class DegradationState:
    state_key: str
    status: str
    triggered_at: str | None
    recovered_at: str | None
    reason: str | None
    updated_at: str


@dataclass
class AnalysisReportRecord:
    run_id: str
    request_id: str
    chat_id: str
    symbol: str
    summary: str
    key_metrics: dict[str, Any]
    created_at: str
    updated_at: str


@dataclass
class NLRequestRecord:
    request_id: str
    update_id: int
    chat_id: str
    intent: str
    slots: dict[str, Any]
    confidence: float
    needs_confirm: bool
    status: str
    text_dedupe_key: str
    intent_dedupe_key: str
    normalized_text: str
    normalized_request: str
    action_version: str
    risk_level: str
    raw_text_hash: str
    intent_candidate: str
    reject_reason: str | None
    confirm_deadline_at: str | None
    last_error: str | None
    created_at: str
    updated_at: str
    archived_at: str | None = None


@dataclass
class ClarifyPendingRecord:
    chat_id: str
    request_id: str
    intent: str
    slots: dict[str, Any]
    missing_slots: list[str]
    command_template: str
    action_version: str
    schema_version: str
    expires_at: str
    created_at: str
    updated_at: str


@dataclass
class ConversationContextRecord:
    scope_key: str
    last_symbol_context: str | None
    last_period_context: str | None
    expires_at: str
    updated_at: str


@dataclass
class PendingCandidateSelectionRecord:
    request_id: str
    chat_id: str
    scope_key: str
    candidates: list[str]
    command_template: str
    status: str
    expires_at: str
    created_at: str
    updated_at: str


@dataclass
class RequestChartStateRecord:
    request_id: str
    chart_state: str
    chart_updated_at: str


@dataclass
class ConversationArchiveRecord:
    archive_id: int
    scope_key: str
    chat_id: str
    from_request_id: str
    to_request_id: str
    request_count: int
    summary: dict[str, Any]
    created_at: str


@dataclass
class NotificationRoute:
    chat_id: str
    channel: str
    target: str
    enabled: bool
    created_at: str
    updated_at: str


@dataclass
class AlertHubRecord:
    event_id: str
    chat_id: str
    symbol: str
    priority: str
    strategy_tier: str
    channel: str
    status: str
    suppressed_reason: str | None
    last_error: str | None
    trigger_ts: str
    updated_at: str


@dataclass
class WatchlistGroupRecord:
    group_id: str
    chat_id: str
    name: str
    created_at: str
    symbols: list[str]


@dataclass
class OutboundWebhookRecord:
    webhook_id: str
    chat_id: str
    url: str
    secret: str
    enabled: bool
    timeout_ms: int
    created_at: str
    updated_at: str


@dataclass
class ChatPreferenceRecord:
    chat_id: str
    min_priority: str
    quiet_hours: str | None
    summary_mode: str
    digest_schedule: str | None
    updated_at: str


class TelegramTaskStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS bot_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    update_id INTEGER NOT NULL UNIQUE,
                    chat_id TEXT NOT NULL,
                    command TEXT,
                    payload TEXT NOT NULL,
                    request_id TEXT,
                    status TEXT NOT NULL,
                    error TEXT,
                    processed_at TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    update_id INTEGER NOT NULL,
                    chat_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL,
                    run_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_error TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_reports (
                    run_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    chat_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    key_metrics TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telegram_chats (
                    chat_id TEXT PRIMARY KEY,
                    user_id TEXT,
                    username TEXT,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watch_jobs (
                    job_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    market TEXT NOT NULL,
                    interval_sec INTEGER NOT NULL,
                    threshold REAL NOT NULL,
                    mode TEXT NOT NULL,
                    scope TEXT NOT NULL DEFAULT 'single',
                    group_id TEXT,
                    route_strategy TEXT NOT NULL DEFAULT 'dual_channel',
                    strategy_tier TEXT NOT NULL DEFAULT 'execution-ready',
                    template_id TEXT,
                    enabled INTEGER NOT NULL,
                    next_run_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_run_at TEXT,
                    last_triggered_at TEXT,
                    last_error TEXT,
                    FOREIGN KEY(chat_id) REFERENCES telegram_chats(chat_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watch_events (
                    event_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    trigger_ts TEXT NOT NULL,
                    price REAL NOT NULL,
                    pct_change REAL NOT NULL,
                    reason TEXT NOT NULL,
                    rule TEXT NOT NULL,
                    priority TEXT NOT NULL DEFAULT 'medium',
                    strategy_tier TEXT NOT NULL DEFAULT 'execution-ready',
                    bucket_ts TEXT NOT NULL,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    pushed INTEGER NOT NULL,
                    run_id TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES watch_jobs(job_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notifications (
                    notification_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    state TEXT NOT NULL,
                    suppressed_reason TEXT,
                    retry_count INTEGER NOT NULL,
                    next_retry_at TEXT,
                    last_error TEXT,
                    delivered_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(event_id, channel),
                    FOREIGN KEY(event_id) REFERENCES watch_events(event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watchlist_groups (
                    group_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watchlist_group_symbols (
                    group_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    PRIMARY KEY(group_id, symbol),
                    FOREIGN KEY(group_id) REFERENCES watchlist_groups(group_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbound_webhooks (
                    webhook_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    secret TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    timeout_ms INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chat_preferences (
                    chat_id TEXT PRIMARY KEY,
                    min_priority TEXT NOT NULL,
                    quiet_hours TEXT,
                    summary_mode TEXT NOT NULL,
                    digest_schedule TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_state_transitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    from_state TEXT,
                    to_state TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_recovery_queue (
                    request_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    next_retry_at TEXT NOT NULL,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS command_rate_limits (
                    chat_id TEXT NOT NULL,
                    window_start TEXT NOT NULL,
                    command_count INTEGER NOT NULL,
                    PRIMARY KEY(chat_id, window_start)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nl_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT NOT NULL UNIQUE,
                    update_id INTEGER NOT NULL,
                    chat_id TEXT NOT NULL,
                    intent TEXT NOT NULL,
                    slots TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    needs_confirm INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    text_dedupe_key TEXT NOT NULL,
                    intent_dedupe_key TEXT NOT NULL,
                    normalized_text TEXT NOT NULL,
                    normalized_request TEXT NOT NULL,
                    action_version TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    raw_text_hash TEXT NOT NULL,
                    intent_candidate TEXT NOT NULL,
                    reject_reason TEXT,
                    confirm_deadline_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metric_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    tags TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS clarify_pending (
                    chat_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    intent TEXT NOT NULL,
                    slots TEXT NOT NULL,
                    missing_slots TEXT NOT NULL,
                    command_template TEXT NOT NULL,
                    action_version TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS degradation_states (
                    state_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    triggered_at TEXT,
                    recovered_at TEXT,
                    reason TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS degradation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    state_key TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    chat_id TEXT,
                    update_id INTEGER,
                    action TEXT,
                    reason TEXT,
                    metadata TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feature_flags (
                    flag_key TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_contexts (
                    scope_key TEXT PRIMARY KEY,
                    last_symbol_context TEXT,
                    last_period_context TEXT,
                    expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_candidate_selection (
                    request_id TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    candidates TEXT NOT NULL,
                    command_template TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS request_chart_states (
                    request_id TEXT PRIMARY KEY,
                    chart_state TEXT NOT NULL,
                    chart_updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS conversation_archives (
                    archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_key TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    from_request_id TEXT NOT NULL,
                    to_request_id TEXT NOT NULL,
                    request_count INTEGER NOT NULL,
                    summary TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS allowlist_chats (
                    chat_id TEXT PRIMARY KEY,
                    can_monitor INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS notification_routes (
                    chat_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    target TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(chat_id, channel, target)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_jobs_chat_enabled ON watch_jobs(chat_id, enabled)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_jobs_next_run_at ON watch_jobs(next_run_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_state_next_retry ON notifications(state, next_retry_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_events_created ON watch_events(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_outbound_webhooks_chat_enabled ON outbound_webhooks(chat_id, enabled)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metric_events_name_ts ON metric_events(metric_name, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_clarify_pending_expires ON clarify_pending(expires_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_ts ON audit_events(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_analysis_reports_chat_created ON analysis_reports(chat_id, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_notification_routes_chat_enabled ON notification_routes(chat_id, enabled)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nl_requests_chat_status ON nl_requests(chat_id, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nl_requests_text_dedupe ON nl_requests(chat_id, text_dedupe_key)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nl_requests_intent_dedupe ON nl_requests(chat_id, intent_dedupe_key)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_candidate_chat_status ON pending_candidate_selection(chat_id, status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_candidate_expires ON pending_candidate_selection(expires_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_conversation_archives_scope_created ON conversation_archives(scope_key, created_at)")
            self._ensure_column(conn, "watch_jobs", "scope", "TEXT NOT NULL DEFAULT 'single'")
            self._ensure_column(conn, "watch_jobs", "group_id", "TEXT")
            self._ensure_column(conn, "watch_jobs", "route_strategy", "TEXT NOT NULL DEFAULT 'dual_channel'")
            self._ensure_column(conn, "watch_jobs", "strategy_tier", "TEXT NOT NULL DEFAULT 'execution-ready'")
            self._ensure_column(conn, "watch_jobs", "template_id", "TEXT")
            self._ensure_column(conn, "watch_events", "priority", "TEXT NOT NULL DEFAULT 'medium'")
            self._ensure_column(conn, "watch_events", "strategy_tier", "TEXT NOT NULL DEFAULT 'execution-ready'")
            self._ensure_column(conn, "notifications", "suppressed_reason", "TEXT")
            self._ensure_column(conn, "nl_requests", "archived_at", "TEXT")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl_type: str) -> None:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        if any(str(row["name"]) == column for row in rows):
            return
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}")

    def insert_bot_update_if_new(self, *, update_id: int, chat_id: str, payload: dict[str, Any]) -> bool:
        now = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO bot_updates(update_id, chat_id, payload, status, created_at)
                VALUES(?, ?, ?, 'processing', ?)
                """,
                (update_id, str(chat_id), _json_dumps(payload), now),
            )
            return cursor.rowcount > 0

    def get_bot_update_payload(self, *, update_id: int) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload
                FROM bot_updates
                WHERE update_id = ?
                """,
                (update_id,),
            ).fetchone()
        if row is None:
            return None
        return json.loads(str(row["payload"]))

    def get_latest_update_id(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(update_id), 0) AS max_update_id
                FROM bot_updates
                """
            ).fetchone()
        return int(row["max_update_id"])

    def list_pending_bot_update_ids(self, *, limit: int = 100) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT update_id
                FROM bot_updates
                WHERE status = 'processing'
                ORDER BY update_id ASC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [int(row["update_id"]) for row in rows]

    def update_bot_update_status(
        self,
        *,
        update_id: int,
        status: str,
        command: str | None = None,
        request_id: str | None = None,
        error: str | None = None,
    ) -> None:
        processed_at = _utc_now() if status in {"processed", "failed", "retried"} else None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE bot_updates
                SET status = ?,
                    command = COALESCE(?, command),
                    request_id = COALESCE(?, request_id),
                    error = ?,
                    processed_at = COALESCE(?, processed_at)
                WHERE update_id = ?
                """,
                (status, command, request_id, error, processed_at, update_id),
            )

    def create_analysis_request_if_new(
        self,
        *,
        request_id: str,
        update_id: int,
        chat_id: str,
        payload: dict[str, Any],
        status: str = "queued",
    ) -> bool:
        now = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO analysis_requests(
                    request_id, update_id, chat_id, payload, status, run_id, created_at, updated_at, last_error
                )
                VALUES(?, ?, ?, ?, ?, NULL, ?, ?, NULL)
                """,
                (request_id, update_id, str(chat_id), _json_dumps(payload), status, now, now),
            )
            return cursor.rowcount > 0

    def transition_analysis_request_status(
        self,
        *,
        request_id: str,
        from_statuses: Iterable[str],
        to_status: str,
        run_id: str | None = None,
        last_error: str | None = None,
    ) -> bool:
        from_list = list(dict.fromkeys(from_statuses))
        if not from_list:
            return False
        placeholders = ",".join(["?"] * len(from_list))
        params: list[Any] = [
            to_status,
            run_id,
            last_error,
            _utc_now(),
            request_id,
            *from_list,
        ]
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE analysis_requests
                SET status = ?,
                    run_id = COALESCE(?, run_id),
                    last_error = ?,
                    updated_at = ?
                WHERE request_id = ?
                  AND status IN ({placeholders})
                """,
                params,
            )
            return cursor.rowcount > 0

    def get_analysis_request(self, request_id: str) -> AnalysisRequestRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, payload, status, run_id, created_at, updated_at, last_error
                FROM analysis_requests
                WHERE request_id = ?
                """,
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        return AnalysisRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            payload=json.loads(str(row["payload"])),
            status=str(row["status"]),
            run_id=str(row["run_id"]) if row["run_id"] is not None else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
        )

    def create_nl_request(
        self,
        *,
        request_id: str,
        update_id: int,
        chat_id: str,
        intent: str,
        slots: dict[str, Any],
        confidence: float,
        needs_confirm: bool,
        status: str,
        text_dedupe_key: str,
        intent_dedupe_key: str,
        normalized_text: str,
        normalized_request: str,
        action_version: str,
        risk_level: str,
        raw_text_hash: str,
        intent_candidate: str,
        reject_reason: str | None = None,
        confirm_deadline_at: str | None = None,
        last_error: str | None = None,
    ) -> bool:
        now = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO nl_requests(
                    request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                    text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                    raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    int(update_id),
                    str(chat_id),
                    intent,
                    _json_dumps(slots),
                    float(confidence),
                    1 if needs_confirm else 0,
                    status,
                    text_dedupe_key,
                    intent_dedupe_key,
                    normalized_text,
                    normalized_request,
                    action_version,
                    risk_level,
                    raw_text_hash,
                    intent_candidate,
                    reject_reason,
                    confirm_deadline_at,
                    last_error,
                    now,
                    now,
                ),
            )
            return cursor.rowcount > 0

    def transition_nl_request_status(
        self,
        *,
        request_id: str,
        from_statuses: Iterable[str],
        to_status: str,
        reject_reason: str | None = None,
        last_error: str | None = None,
        confirm_deadline_at: str | None = None,
    ) -> bool:
        allowed = list(dict.fromkeys(from_statuses))
        if not allowed:
            return False
        placeholders = ",".join("?" for _ in allowed)
        params: list[Any] = [
            to_status,
            reject_reason,
            last_error,
            confirm_deadline_at,
            _utc_now(),
            request_id,
            *allowed,
        ]
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE nl_requests
                SET status = ?,
                    reject_reason = COALESCE(?, reject_reason),
                    last_error = ?,
                    confirm_deadline_at = COALESCE(?, confirm_deadline_at),
                    updated_at = ?
                WHERE request_id = ?
                  AND status IN ({placeholders})
                """,
                params,
            )
            return cursor.rowcount > 0

    def set_nl_request_status(
        self,
        *,
        request_id: str,
        to_status: str,
        reject_reason: str | None = None,
        last_error: str | None = None,
        confirm_deadline_at: str | None = None,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE nl_requests
                SET status = ?,
                    reject_reason = COALESCE(?, reject_reason),
                    last_error = ?,
                    confirm_deadline_at = COALESCE(?, confirm_deadline_at),
                    updated_at = ?
                WHERE request_id = ?
                """,
                (to_status, reject_reason, last_error, confirm_deadline_at, _utc_now(), request_id),
            )
            return cursor.rowcount > 0

    def get_nl_request(self, *, request_id: str) -> NLRequestRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                       text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                       raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                FROM nl_requests
                WHERE request_id = ?
                LIMIT 1
                """,
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        return NLRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            confidence=float(row["confidence"]),
            needs_confirm=bool(int(row["needs_confirm"])),
            status=str(row["status"]),
            text_dedupe_key=str(row["text_dedupe_key"]),
            intent_dedupe_key=str(row["intent_dedupe_key"]),
            normalized_text=str(row["normalized_text"]),
            normalized_request=str(row["normalized_request"]),
            action_version=str(row["action_version"]),
            risk_level=str(row["risk_level"]),
            raw_text_hash=str(row["raw_text_hash"]),
            intent_candidate=str(row["intent_candidate"]),
            reject_reason=str(row["reject_reason"]) if row["reject_reason"] else None,
            confirm_deadline_at=str(row["confirm_deadline_at"]) if row["confirm_deadline_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def get_pending_confirm_request(self, *, chat_id: str) -> NLRequestRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                       text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                       raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                FROM nl_requests
                WHERE chat_id = ? AND status = 'pending_confirm'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id),),
            ).fetchone()
        if row is None:
            return None
        return NLRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            confidence=float(row["confidence"]),
            needs_confirm=bool(int(row["needs_confirm"])),
            status=str(row["status"]),
            text_dedupe_key=str(row["text_dedupe_key"]),
            intent_dedupe_key=str(row["intent_dedupe_key"]),
            normalized_text=str(row["normalized_text"]),
            normalized_request=str(row["normalized_request"]),
            action_version=str(row["action_version"]),
            risk_level=str(row["risk_level"]),
            raw_text_hash=str(row["raw_text_hash"]),
            intent_candidate=str(row["intent_candidate"]),
            reject_reason=str(row["reject_reason"]) if row["reject_reason"] else None,
            confirm_deadline_at=str(row["confirm_deadline_at"]) if row["confirm_deadline_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def get_pending_confirm_by_ref(self, *, chat_id: str, request_ref: str) -> NLRequestRecord | None:
        ref = (request_ref or "").strip()
        if not ref:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                       text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                       raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                FROM nl_requests
                WHERE chat_id = ?
                  AND status = 'pending_confirm'
                  AND (
                      request_id = ?
                      OR substr(request_id, -6) = ?
                  )
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id), ref, ref[-6:]),
            ).fetchone()
        if row is None:
            return None
        return NLRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            confidence=float(row["confidence"]),
            needs_confirm=bool(int(row["needs_confirm"])),
            status=str(row["status"]),
            text_dedupe_key=str(row["text_dedupe_key"]),
            intent_dedupe_key=str(row["intent_dedupe_key"]),
            normalized_text=str(row["normalized_text"]),
            normalized_request=str(row["normalized_request"]),
            action_version=str(row["action_version"]),
            risk_level=str(row["risk_level"]),
            raw_text_hash=str(row["raw_text_hash"]),
            intent_candidate=str(row["intent_candidate"]),
            reject_reason=str(row["reject_reason"]) if row["reject_reason"] else None,
            confirm_deadline_at=str(row["confirm_deadline_at"]) if row["confirm_deadline_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def has_executing_nl_request(self, *, chat_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM nl_requests WHERE chat_id = ? AND status = 'executing'",
                (str(chat_id),),
            ).fetchone()
        return int(row["c"]) > 0

    def get_executing_nl_request(self, *, chat_id: str) -> NLRequestRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                       text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                       raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                FROM nl_requests
                WHERE chat_id = ? AND status = 'executing'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(chat_id),),
            ).fetchone()
        if row is None:
            return None
        return NLRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            confidence=float(row["confidence"]),
            needs_confirm=bool(int(row["needs_confirm"])),
            status=str(row["status"]),
            text_dedupe_key=str(row["text_dedupe_key"]),
            intent_dedupe_key=str(row["intent_dedupe_key"]),
            normalized_text=str(row["normalized_text"]),
            normalized_request=str(row["normalized_request"]),
            action_version=str(row["action_version"]),
            risk_level=str(row["risk_level"]),
            raw_text_hash=str(row["raw_text_hash"]),
            intent_candidate=str(row["intent_candidate"]),
            reject_reason=str(row["reject_reason"]) if row["reject_reason"] else None,
            confirm_deadline_at=str(row["confirm_deadline_at"]) if row["confirm_deadline_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def find_recent_snapshot_singleflight(
        self,
        *,
        chat_id: str,
        scope_key: str,
        symbol: str,
        period: str,
        interval: str,
        ttl_seconds: int = 120,
    ) -> NLRequestRecord | None:
        window_start = _isoformat(_utc_now_dt() - timedelta(seconds=max(1, int(ttl_seconds))))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT request_id
                FROM nl_requests
                WHERE chat_id = ?
                  AND intent = 'analyze_snapshot'
                  AND status IN ('queued', 'executing', 'completed')
                  AND created_at >= ?
                ORDER BY updated_at DESC
                LIMIT 20
                """,
                (str(chat_id), window_start),
            ).fetchall()

        for row in rows:
            request_id = str(row["request_id"])
            record = self.get_nl_request(request_id=request_id)
            if record is None:
                continue
            slots = record.slots if isinstance(record.slots, dict) else {}
            if str(slots.get("_context_scope_key", "")).strip() != str(scope_key).strip():
                continue
            slot_symbol = str(slots.get("symbol", "")).strip().upper()
            slot_period = str(slots.get("period", "")).strip().lower()
            slot_interval = str(slots.get("interval", "")).strip().lower()
            if slot_symbol == str(symbol).strip().upper() and slot_period == str(period).strip().lower() and slot_interval == str(interval).strip().lower():
                return record
        return None

    def compact_conversation_history(
        self,
        *,
        chat_id: str,
        scope_key: str,
        keep_recent: int = 8,
        min_batch: int = 8,
    ) -> dict[str, Any] | None:
        keep_recent_count = max(1, int(keep_recent))
        min_batch_count = max(1, int(min_batch))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT request_id, intent, slots, status, created_at
                FROM nl_requests
                WHERE chat_id = ?
                  AND archived_at IS NULL
                  AND status IN ('completed', 'rejected', 'failed')
                ORDER BY created_at DESC
                """,
                (str(chat_id),),
            ).fetchall()
            if len(rows) <= keep_recent_count + min_batch_count:
                return None
            archive_candidates = rows[keep_recent_count:]
            if len(archive_candidates) < min_batch_count:
                return None

            oldest_to_newest = list(reversed(archive_candidates))
            from_request_id = str(oldest_to_newest[0]["request_id"])
            to_request_id = str(oldest_to_newest[-1]["request_id"])
            intents: dict[str, int] = {}
            symbols: dict[str, int] = {}
            for item in oldest_to_newest:
                intent = str(item["intent"])
                intents[intent] = intents.get(intent, 0) + 1
                slots = json.loads(str(item["slots"]))
                if isinstance(slots, dict):
                    symbol = str(slots.get("symbol", "")).strip().upper()
                    if symbol:
                        symbols[symbol] = symbols.get(symbol, 0) + 1

            summary_payload = {
                "scope_key": str(scope_key),
                "chat_id": str(chat_id),
                "from_request_id": from_request_id,
                "to_request_id": to_request_id,
                "request_count": len(oldest_to_newest),
                "intent_distribution": intents,
                "symbol_topk": sorted(symbols.items(), key=lambda item: item[1], reverse=True)[:5],
            }
            now = _utc_now()
            conn.execute(
                """
                INSERT INTO conversation_archives(
                    scope_key, chat_id, from_request_id, to_request_id, request_count, summary, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(scope_key),
                    str(chat_id),
                    from_request_id,
                    to_request_id,
                    len(oldest_to_newest),
                    _json_dumps(summary_payload),
                    now,
                ),
            )
            ids = [str(item["request_id"]) for item in oldest_to_newest]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(
                f"""
                UPDATE nl_requests
                SET archived_at = ?
                WHERE request_id IN ({placeholders})
                """,
                (now, *ids),
            )
        return summary_payload

    def list_conversation_archives(self, *, scope_key: str, limit: int = 20) -> list[ConversationArchiveRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT archive_id, scope_key, chat_id, from_request_id, to_request_id, request_count, summary, created_at
                FROM conversation_archives
                WHERE scope_key = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (str(scope_key), max(1, int(limit))),
            ).fetchall()
        return [
            ConversationArchiveRecord(
                archive_id=int(row["archive_id"]),
                scope_key=str(row["scope_key"]),
                chat_id=str(row["chat_id"]),
                from_request_id=str(row["from_request_id"]),
                to_request_id=str(row["to_request_id"]),
                request_count=int(row["request_count"]),
                summary=json.loads(str(row["summary"])),
                created_at=str(row["created_at"]),
            )
            for row in rows
        ]

    def upsert_clarify_pending(
        self,
        *,
        chat_id: str,
        request_id: str,
        intent: str,
        slots: dict[str, Any],
        missing_slots: list[str],
        command_template: str,
        action_version: str,
        schema_version: str,
        ttl_seconds: int = 300,
    ) -> None:
        now = _utc_now_dt()
        now_iso = _isoformat(now)
        expires_at = _isoformat(now + timedelta(seconds=max(1, int(ttl_seconds))))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO clarify_pending(
                    chat_id, request_id, intent, slots, missing_slots, command_template,
                    action_version, schema_version, expires_at, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    request_id = excluded.request_id,
                    intent = excluded.intent,
                    slots = excluded.slots,
                    missing_slots = excluded.missing_slots,
                    command_template = excluded.command_template,
                    action_version = excluded.action_version,
                    schema_version = excluded.schema_version,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(chat_id),
                    request_id,
                    intent,
                    _json_dumps(slots),
                    _json_dumps({"slots": missing_slots}),
                    command_template,
                    action_version,
                    schema_version,
                    expires_at,
                    now_iso,
                    now_iso,
                ),
            )

    @staticmethod
    def _row_to_clarify_pending(row: sqlite3.Row) -> ClarifyPendingRecord:
        payload = json.loads(str(row["missing_slots"]))
        missing_slots = payload.get("slots") if isinstance(payload, dict) else []
        return ClarifyPendingRecord(
            chat_id=str(row["chat_id"]),
            request_id=str(row["request_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            missing_slots=[str(item) for item in missing_slots if isinstance(item, str)],
            command_template=str(row["command_template"]),
            action_version=str(row["action_version"]),
            schema_version=str(row["schema_version"]),
            expires_at=str(row["expires_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def get_clarify_pending(self, *, chat_id: str) -> ClarifyPendingRecord | None:
        record, expired = self.get_clarify_pending_state(chat_id=chat_id)
        if expired:
            self.clear_clarify_pending(chat_id=chat_id)
            return None
        return record

    def get_clarify_pending_state(self, *, chat_id: str) -> tuple[ClarifyPendingRecord | None, bool]:
        now_iso = _isoformat(_utc_now_dt())
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT chat_id, request_id, intent, slots, missing_slots, command_template,
                       action_version, schema_version, expires_at, created_at, updated_at
                FROM clarify_pending
                WHERE chat_id = ?
                LIMIT 1
                """,
                (str(chat_id),),
            ).fetchone()
        if row is None:
            return None, False
        record = self._row_to_clarify_pending(row)
        return record, str(record.expires_at) <= now_iso

    def clear_clarify_pending(self, *, chat_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM clarify_pending WHERE chat_id = ?", (str(chat_id),))
        return cursor.rowcount > 0

    def upsert_conversation_context(
        self,
        *,
        scope_key: str,
        last_symbol_context: str | None,
        last_period_context: str | None,
        ttl_seconds: int = 1800,
    ) -> None:
        now = _utc_now_dt()
        now_iso = _isoformat(now)
        expires_at = _isoformat(now + timedelta(seconds=max(1, int(ttl_seconds))))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO conversation_contexts(scope_key, last_symbol_context, last_period_context, expires_at, updated_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    last_symbol_context = excluded.last_symbol_context,
                    last_period_context = excluded.last_period_context,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (
                    scope_key,
                    last_symbol_context.upper() if last_symbol_context else None,
                    last_period_context.lower() if last_period_context else None,
                    expires_at,
                    now_iso,
                ),
            )

    def get_conversation_context(self, *, scope_key: str) -> ConversationContextRecord | None:
        now_iso = _isoformat(_utc_now_dt())
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT scope_key, last_symbol_context, last_period_context, expires_at, updated_at
                FROM conversation_contexts
                WHERE scope_key = ?
                LIMIT 1
                """,
                (scope_key,),
            ).fetchone()
        if row is None:
            return None
        record = ConversationContextRecord(
            scope_key=str(row["scope_key"]),
            last_symbol_context=str(row["last_symbol_context"]) if row["last_symbol_context"] else None,
            last_period_context=str(row["last_period_context"]) if row["last_period_context"] else None,
            expires_at=str(row["expires_at"]),
            updated_at=str(row["updated_at"]),
        )
        if str(record.expires_at) <= now_iso:
            self.clear_conversation_context(scope_key=scope_key)
            return None
        return record

    def clear_conversation_context(self, *, scope_key: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM conversation_contexts WHERE scope_key = ?", (scope_key,))
        return cursor.rowcount > 0

    def update_nl_request_slots(self, *, request_id: str, slots: dict[str, Any]) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE nl_requests
                SET slots = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (_json_dumps(slots), _utc_now(), request_id),
            )
        return cursor.rowcount > 0

    def upsert_pending_candidate_selection(
        self,
        *,
        request_id: str,
        chat_id: str,
        scope_key: str,
        candidates: list[str],
        command_template: str,
        ttl_seconds: int = 300,
    ) -> None:
        now = _utc_now_dt()
        now_iso = _isoformat(now)
        expires_at = _isoformat(now + timedelta(seconds=max(1, int(ttl_seconds))))
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pending_candidate_selection(
                    request_id, chat_id, scope_key, candidates, command_template, status, expires_at, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    scope_key = excluded.scope_key,
                    candidates = excluded.candidates,
                    command_template = excluded.command_template,
                    status = excluded.status,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (
                    request_id,
                    str(chat_id),
                    scope_key,
                    _json_dumps({"candidates": [str(item).upper() for item in candidates]}),
                    command_template,
                    expires_at,
                    now_iso,
                    now_iso,
                ),
            )

    def get_pending_candidate_by_ref(self, *, chat_id: str, request_ref: str) -> PendingCandidateSelectionRecord | None:
        ref = (request_ref or "").strip()
        if not ref:
            return None
        now_iso = _isoformat(_utc_now_dt())
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, chat_id, scope_key, candidates, command_template, status, expires_at, created_at, updated_at
                FROM pending_candidate_selection
                WHERE chat_id = ?
                  AND status = 'pending'
                  AND (request_id = ? OR substr(request_id, -6) = ?)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id), ref, ref[-6:]),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row["candidates"]))
        candidates = payload.get("candidates") if isinstance(payload, dict) else []
        record = PendingCandidateSelectionRecord(
            request_id=str(row["request_id"]),
            chat_id=str(row["chat_id"]),
            scope_key=str(row["scope_key"]),
            candidates=[str(item).upper() for item in candidates if isinstance(item, str)],
            command_template=str(row["command_template"]),
            status=str(row["status"]),
            expires_at=str(row["expires_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )
        if str(record.expires_at) <= now_iso:
            self.mark_pending_candidate_selection(request_id=record.request_id, status="expired")
            return None
        return record

    def upsert_request_chart_state(self, *, request_id: str, chart_state: str) -> None:
        state = str(chart_state or "none").strip().lower()
        if state not in {"none", "rendering", "ready", "failed"}:
            state = "none"
        now_iso = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO request_chart_states(request_id, chart_state, chart_updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    chart_state = excluded.chart_state,
                    chart_updated_at = excluded.chart_updated_at
                """,
                (request_id, state, now_iso),
            )

    def get_request_chart_state(self, *, request_id: str) -> RequestChartStateRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, chart_state, chart_updated_at
                FROM request_chart_states
                WHERE request_id = ?
                LIMIT 1
                """,
                (request_id,),
            ).fetchone()
        if row is None:
            return None
        return RequestChartStateRecord(
            request_id=str(row["request_id"]),
            chart_state=str(row["chart_state"]),
            chart_updated_at=str(row["chart_updated_at"]),
        )

    def mark_pending_candidate_selection(self, *, request_id: str, status: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE pending_candidate_selection
                SET status = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (status, _utc_now(), request_id),
            )
        return cursor.rowcount > 0

    def clear_pending_candidate_selection(self, *, chat_id: str) -> int:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM pending_candidate_selection WHERE chat_id = ?", (str(chat_id),))
        return int(cursor.rowcount)

    def reset_conversation_runtime_state(self, *, chat_id: str, scope_key: str) -> None:
        self.clear_conversation_context(scope_key=scope_key)
        self.clear_pending_candidate_selection(chat_id=chat_id)
        self.clear_clarify_pending(chat_id=chat_id)
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE nl_requests
                SET status = 'rejected',
                    reject_reason = 'reset',
                    confirm_deadline_at = NULL,
                    updated_at = ?
                WHERE chat_id = ?
                  AND status = 'pending_confirm'
                """,
                (_utc_now(), str(chat_id)),
            )

    def find_recent_nl_duplicates(
        self,
        *,
        chat_id: str,
        text_dedupe_key: str,
        intent_dedupe_key: str,
        intent: str,
        current_request_id: str,
    ) -> tuple[bool, str | None]:
        with self._connect() as conn:
            text_hit = conn.execute(
                """
                SELECT request_id
                FROM nl_requests
                WHERE chat_id = ?
                  AND text_dedupe_key = ?
                  AND request_id != ?
                  AND status IN ('pending_confirm', 'executing', 'completed')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id), text_dedupe_key, current_request_id),
            ).fetchone()
            intent_hit = conn.execute(
                """
                SELECT request_id
                FROM nl_requests
                WHERE chat_id = ?
                  AND intent_dedupe_key = ?
                  AND request_id != ?
                  AND status IN ('pending_confirm', 'executing', 'completed')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id), intent_dedupe_key, current_request_id),
            ).fetchone()

        text_req = str(text_hit["request_id"]) if text_hit else None
        intent_req = str(intent_hit["request_id"]) if intent_hit else None
        if intent == "create_monitor":
            return intent_req is not None, intent_req
        if intent == "analyze_snapshot":
            if text_req and intent_req:
                return True, intent_req
            return False, None
        return False, None

    def get_nl_request_by_ref(self, *, chat_id: str, request_ref: str) -> NLRequestRecord | None:
        ref = (request_ref or "").strip()
        if not ref:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT request_id, update_id, chat_id, intent, slots, confidence, needs_confirm, status,
                       text_dedupe_key, intent_dedupe_key, normalized_text, normalized_request, action_version, risk_level,
                       raw_text_hash, intent_candidate, reject_reason, confirm_deadline_at, last_error, created_at, updated_at
                FROM nl_requests
                WHERE chat_id = ?
                  AND (request_id = ? OR substr(request_id, -6) = ?)
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (str(chat_id), ref, ref[-6:]),
            ).fetchone()
        if row is None:
            return None
        return NLRequestRecord(
            request_id=str(row["request_id"]),
            update_id=int(row["update_id"]),
            chat_id=str(row["chat_id"]),
            intent=str(row["intent"]),
            slots=json.loads(str(row["slots"])),
            confidence=float(row["confidence"]),
            needs_confirm=bool(int(row["needs_confirm"])),
            status=str(row["status"]),
            text_dedupe_key=str(row["text_dedupe_key"]),
            intent_dedupe_key=str(row["intent_dedupe_key"]),
            normalized_text=str(row["normalized_text"]),
            normalized_request=str(row["normalized_request"]),
            action_version=str(row["action_version"]),
            risk_level=str(row["risk_level"]),
            raw_text_hash=str(row["raw_text_hash"]),
            intent_candidate=str(row["intent_candidate"]),
            reject_reason=str(row["reject_reason"]) if row["reject_reason"] is not None else None,
            confirm_deadline_at=str(row["confirm_deadline_at"]) if row["confirm_deadline_at"] is not None else None,
            last_error=str(row["last_error"]) if row["last_error"] is not None else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def upsert_analysis_report(
        self,
        *,
        run_id: str,
        request_id: str,
        chat_id: str,
        symbol: str,
        summary: str,
        key_metrics: dict[str, Any] | None = None,
    ) -> None:
        now = _utc_now()
        payload = (
            run_id,
            request_id,
            str(chat_id),
            symbol.upper(),
            summary.strip(),
            _json_dumps(key_metrics or {}),
            now,
            now,
        )
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO analysis_reports(run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        request_id = excluded.request_id,
                        chat_id = excluded.chat_id,
                        symbol = excluded.symbol,
                        summary = excluded.summary,
                        key_metrics = excluded.key_metrics,
                        updated_at = excluded.updated_at
                    """,
                    payload,
                )
            except sqlite3.IntegrityError as exc:
                # Same request can be retried and produce a new run_id; keep this path idempotent.
                if "analysis_reports.request_id" not in str(exc):
                    raise
                try:
                    conn.execute(
                        """
                        INSERT INTO analysis_reports(run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(request_id) DO UPDATE SET
                            run_id = excluded.run_id,
                            chat_id = excluded.chat_id,
                            symbol = excluded.symbol,
                            summary = excluded.summary,
                            key_metrics = excluded.key_metrics,
                            updated_at = excluded.updated_at
                        """,
                        payload,
                    )
                except sqlite3.IntegrityError as run_conflict:
                    if "analysis_reports.run_id" not in str(run_conflict):
                        raise
                    conn.execute(
                        """
                        UPDATE analysis_reports
                        SET chat_id = ?, symbol = ?, summary = ?, key_metrics = ?, updated_at = ?
                        WHERE request_id = ?
                        """,
                        (
                            str(chat_id),
                            symbol.upper(),
                            summary.strip(),
                            _json_dumps(key_metrics or {}),
                            now,
                            request_id,
                        ),
                    )

    def get_analysis_report(self, *, report_id: str, chat_id: str | None = None) -> AnalysisReportRecord | None:
        report_id_str = report_id.strip()
        if not report_id_str:
            return None
        with self._connect() as conn:
            if chat_id is None:
                row = conn.execute(
                    """
                    SELECT run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at
                    FROM analysis_reports
                    WHERE run_id = ? OR request_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (report_id_str, report_id_str),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT run_id, request_id, chat_id, symbol, summary, key_metrics, created_at, updated_at
                    FROM analysis_reports
                    WHERE (run_id = ? OR request_id = ?) AND chat_id = ?
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (report_id_str, report_id_str, str(chat_id)),
                ).fetchone()
        if row is None:
            return None
        return AnalysisReportRecord(
            run_id=str(row["run_id"]),
            request_id=str(row["request_id"]),
            chat_id=str(row["chat_id"]),
            symbol=str(row["symbol"]),
            summary=str(row["summary"]),
            key_metrics=json.loads(str(row["key_metrics"])),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def enqueue_analysis_recovery(
        self,
        *,
        request_id: str,
        chat_id: str,
        symbol: str,
        retry_count: int,
        next_retry_at: datetime,
        last_error: str | None,
    ) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO analysis_recovery_queue(request_id, chat_id, symbol, retry_count, next_retry_at, last_error, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    retry_count = excluded.retry_count,
                    next_retry_at = excluded.next_retry_at,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    request_id,
                    chat_id,
                    symbol,
                    int(retry_count),
                    _isoformat(next_retry_at),
                    last_error,
                    now,
                    now,
                ),
            )

    def claim_due_analysis_recovery(self, *, now: datetime | None = None, limit: int = 20) -> list[DueAnalysisRecovery]:
        current = now or _utc_now_dt()
        now_iso = _isoformat(current)
        claimed: list[DueAnalysisRecovery] = []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT request_id, chat_id, symbol, retry_count
                FROM analysis_recovery_queue
                WHERE next_retry_at <= ?
                ORDER BY next_retry_at ASC
                LIMIT ?
                """,
                (now_iso, max(1, int(limit))),
            ).fetchall()
            for row in rows:
                conn.execute("DELETE FROM analysis_recovery_queue WHERE request_id = ?", (str(row["request_id"]),))
                claimed.append(
                    DueAnalysisRecovery(
                        request_id=str(row["request_id"]),
                        chat_id=str(row["chat_id"]),
                        symbol=str(row["symbol"]),
                        retry_count=int(row["retry_count"]),
                    )
                )
            conn.commit()
        return claimed

    def upsert_telegram_chat(self, *, chat_id: str, user_id: str | None, username: str | None, status: str = "active") -> None:
        now = _utc_now()
        chat_str = str(chat_id)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO telegram_chats(chat_id, user_id, username, created_at, status)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_id = COALESCE(excluded.user_id, telegram_chats.user_id),
                    username = COALESCE(excluded.username, telegram_chats.username),
                    status = excluded.status
                """,
                (chat_str, str(user_id) if user_id else None, username, now, status),
            )
            conn.execute(
                """
                INSERT INTO notification_routes(chat_id, channel, target, enabled, created_at, updated_at)
                VALUES(?, 'telegram', ?, 1, ?, ?)
                ON CONFLICT(chat_id, channel, target) DO UPDATE SET
                    enabled = 1,
                    updated_at = excluded.updated_at
                """,
                (chat_str, chat_str, now, now),
            )

    def set_allowlist_chat(self, *, chat_id: str, can_monitor: bool) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO allowlist_chats(chat_id, can_monitor)
                VALUES(?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET can_monitor = excluded.can_monitor
                """,
                (str(chat_id), 1 if can_monitor else 0),
            )

    def can_chat_monitor(self, *, chat_id: str) -> bool:
        with self._connect() as conn:
            count = int(conn.execute("SELECT COUNT(*) AS c FROM allowlist_chats").fetchone()["c"])
            if count == 0:
                return True
            row = conn.execute(
                "SELECT can_monitor FROM allowlist_chats WHERE chat_id = ?",
                (str(chat_id),),
            ).fetchone()
        return bool(row and int(row["can_monitor"]) == 1)

    def is_chat_allowlisted(self, *, chat_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT can_monitor FROM allowlist_chats WHERE chat_id = ?",
                (str(chat_id),),
            ).fetchone()
        return bool(row and int(row["can_monitor"]) == 1)

    def upsert_notification_route(self, *, chat_id: str, channel: str, target: str, enabled: bool = True) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notification_routes(chat_id, channel, target, enabled, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, channel, target) DO UPDATE SET
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (str(chat_id), channel, target, 1 if enabled else 0, now, now),
            )

    def list_notification_routes(self, *, chat_id: str, enabled_only: bool = True) -> list[NotificationRoute]:
        where = "chat_id = ? AND enabled = 1" if enabled_only else "chat_id = ?"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT chat_id, channel, target, enabled, created_at, updated_at
                FROM notification_routes
                WHERE {where}
                ORDER BY channel ASC, created_at ASC
                """,
                (str(chat_id),),
            ).fetchall()
        return [
            NotificationRoute(
                chat_id=str(row["chat_id"]),
                channel=str(row["channel"]),
                target=str(row["target"]),
                enabled=bool(int(row["enabled"])),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def get_notification_route_target(self, *, chat_id: str, channel: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT target
                FROM notification_routes
                WHERE chat_id = ? AND channel = ? AND enabled = 1
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(chat_id), channel),
            ).fetchone()
        if row is None:
            return None
        return str(row["target"])

    def upsert_outbound_webhook(
        self,
        *,
        chat_id: str,
        url: str,
        secret: str = "",
        timeout_ms: int = 3000,
        enabled: bool = True,
    ) -> OutboundWebhookRecord:
        now = _utc_now()
        webhook_id = f"wh-{uuid4().hex[:8]}"
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT webhook_id
                FROM outbound_webhooks
                WHERE chat_id = ? AND url = ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (str(chat_id), url),
            ).fetchone()
            if existing is not None:
                webhook_id = str(existing["webhook_id"])
            conn.execute(
                """
                INSERT INTO outbound_webhooks(webhook_id, chat_id, url, secret, enabled, timeout_ms, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(webhook_id) DO UPDATE SET
                    secret = excluded.secret,
                    enabled = excluded.enabled,
                    timeout_ms = excluded.timeout_ms,
                    updated_at = excluded.updated_at
                """,
                (webhook_id, str(chat_id), url, secret, 1 if enabled else 0, int(timeout_ms), now, now),
            )
        return self.list_outbound_webhooks(chat_id=chat_id, enabled_only=False, webhook_id=webhook_id)[0]

    def disable_outbound_webhook(self, *, chat_id: str, webhook_id: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE outbound_webhooks
                SET enabled = 0, updated_at = ?
                WHERE chat_id = ? AND webhook_id = ?
                """,
                (_utc_now(), str(chat_id), webhook_id),
            )
        return cursor.rowcount > 0

    def list_outbound_webhooks(
        self,
        *,
        chat_id: str,
        enabled_only: bool = True,
        webhook_id: str | None = None,
    ) -> list[OutboundWebhookRecord]:
        where = ["chat_id = ?"]
        params: list[Any] = [str(chat_id)]
        if enabled_only:
            where.append("enabled = 1")
        if webhook_id:
            where.append("webhook_id = ?")
            params.append(webhook_id)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT webhook_id, chat_id, url, secret, enabled, timeout_ms, created_at, updated_at
                FROM outbound_webhooks
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC
                """,
                tuple(params),
            ).fetchall()
        return [
            OutboundWebhookRecord(
                webhook_id=str(row["webhook_id"]),
                chat_id=str(row["chat_id"]),
                url=str(row["url"]),
                secret=str(row["secret"]),
                enabled=bool(int(row["enabled"])),
                timeout_ms=int(row["timeout_ms"]),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def get_outbound_webhook(self, *, webhook_id: str) -> OutboundWebhookRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT webhook_id, chat_id, url, secret, enabled, timeout_ms, created_at, updated_at
                FROM outbound_webhooks
                WHERE webhook_id = ?
                LIMIT 1
                """,
                (webhook_id,),
            ).fetchone()
        if row is None:
            return None
        return OutboundWebhookRecord(
            webhook_id=str(row["webhook_id"]),
            chat_id=str(row["chat_id"]),
            url=str(row["url"]),
            secret=str(row["secret"]),
            enabled=bool(int(row["enabled"])),
            timeout_ms=int(row["timeout_ms"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def create_or_replace_watchlist_group(self, *, chat_id: str, name: str, symbols: list[str]) -> WatchlistGroupRecord:
        normalized = [item.strip().upper() for item in symbols if item.strip()]
        normalized = list(dict.fromkeys(normalized))
        if not normalized:
            raise ValueError("watchlist group requires at least one symbol")
        now = _utc_now()
        group_id = f"grp-{uuid4().hex[:8]}"
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT group_id
                FROM watchlist_groups
                WHERE chat_id = ? AND name = ?
                LIMIT 1
                """,
                (str(chat_id), name.strip()),
            ).fetchone()
            if existing is not None:
                group_id = str(existing["group_id"])
                conn.execute("DELETE FROM watchlist_group_symbols WHERE group_id = ?", (group_id,))
            conn.execute(
                """
                INSERT INTO watchlist_groups(group_id, chat_id, name, created_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    name = excluded.name
                """,
                (group_id, str(chat_id), name.strip(), now),
            )
            for symbol in normalized:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO watchlist_group_symbols(group_id, symbol)
                    VALUES(?, ?)
                    """,
                    (group_id, symbol),
                )
        return self.get_watchlist_group(group_id=group_id)

    def get_watchlist_group(self, *, group_id: str) -> WatchlistGroupRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT group_id, chat_id, name, created_at
                FROM watchlist_groups
                WHERE group_id = ?
                """,
                (group_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"watchlist group not found: {group_id}")
            symbol_rows = conn.execute(
                """
                SELECT symbol
                FROM watchlist_group_symbols
                WHERE group_id = ?
                ORDER BY symbol ASC
                """,
                (group_id,),
            ).fetchall()
        return WatchlistGroupRecord(
            group_id=str(row["group_id"]),
            chat_id=str(row["chat_id"]),
            name=str(row["name"]),
            created_at=str(row["created_at"]),
            symbols=[str(item["symbol"]) for item in symbol_rows],
        )

    def get_watchlist_group_symbols(self, *, group_id: str) -> list[str]:
        return self.get_watchlist_group(group_id=group_id).symbols

    def get_chat_preferences(self, *, chat_id: str) -> ChatPreferenceRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT chat_id, min_priority, quiet_hours, summary_mode, digest_schedule, updated_at
                FROM chat_preferences
                WHERE chat_id = ?
                """,
                (str(chat_id),),
            ).fetchone()
        if row is None:
            return ChatPreferenceRecord(
                chat_id=str(chat_id),
                min_priority="high",
                quiet_hours=None,
                summary_mode="short",
                digest_schedule=None,
                updated_at=_utc_now(),
            )
        return ChatPreferenceRecord(
            chat_id=str(row["chat_id"]),
            min_priority=str(row["min_priority"]),
            quiet_hours=str(row["quiet_hours"]) if row["quiet_hours"] else None,
            summary_mode=str(row["summary_mode"]),
            digest_schedule=str(row["digest_schedule"]) if row["digest_schedule"] else None,
            updated_at=str(row["updated_at"]),
        )

    def upsert_chat_preferences(
        self,
        *,
        chat_id: str,
        min_priority: str | None = None,
        quiet_hours: str | None = None,
        summary_mode: str | None = None,
        digest_schedule: str | None = None,
    ) -> ChatPreferenceRecord:
        current = self.get_chat_preferences(chat_id=chat_id)
        next_min_priority = min_priority or current.min_priority
        next_quiet_hours = quiet_hours if quiet_hours is not None else current.quiet_hours
        next_summary_mode = summary_mode or current.summary_mode
        next_digest_schedule = digest_schedule if digest_schedule is not None else current.digest_schedule
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO chat_preferences(chat_id, min_priority, quiet_hours, summary_mode, digest_schedule, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    min_priority = excluded.min_priority,
                    quiet_hours = excluded.quiet_hours,
                    summary_mode = excluded.summary_mode,
                    digest_schedule = excluded.digest_schedule,
                    updated_at = excluded.updated_at
                """,
                (str(chat_id), next_min_priority, next_quiet_hours, next_summary_mode, next_digest_schedule, now),
            )
        return self.get_chat_preferences(chat_id=chat_id)

    def bulk_update_watch_jobs(
        self,
        *,
        chat_id: str,
        action: str,
        target: str,
        value: str = "",
    ) -> int:
        selector = target.strip().lower()
        clauses = ["chat_id = ?"]
        params: list[Any] = [str(chat_id)]
        if selector != "all":
            items = [item.strip() for item in target.split(",") if item.strip()]
            if not items:
                return 0
            job_ids = [item.lower() for item in items if item.lower().startswith("job-")]
            symbols = [item.upper() for item in items if not item.lower().startswith("job-")]
            local_clause: list[str] = []
            if job_ids:
                local_clause.append(f"job_id IN ({','.join('?' for _ in job_ids)})")
                params.extend(job_ids)
            if symbols:
                local_clause.append(f"symbol IN ({','.join('?' for _ in symbols)})")
                params.extend(symbols)
            if not local_clause:
                return 0
            clauses.append("(" + " OR ".join(local_clause) + ")")

        now = _utc_now()
        with self._connect() as conn:
            if action == "enable":
                sql = f"UPDATE watch_jobs SET enabled = 1, updated_at = ? WHERE {' AND '.join(clauses)}"
                cursor = conn.execute(sql, (now, *params))
            elif action == "disable":
                sql = f"UPDATE watch_jobs SET enabled = 0, updated_at = ? WHERE {' AND '.join(clauses)}"
                cursor = conn.execute(sql, (now, *params))
            elif action == "interval":
                sql = f"UPDATE watch_jobs SET interval_sec = ?, updated_at = ? WHERE {' AND '.join(clauses)}"
                cursor = conn.execute(sql, (int(value), now, *params))
            elif action == "threshold":
                sql = f"UPDATE watch_jobs SET threshold = ?, updated_at = ? WHERE {' AND '.join(clauses)}"
                cursor = conn.execute(sql, (float(value), now, *params))
            else:
                raise ValueError(f"unsupported bulk action: {action}")
        return int(cursor.rowcount)

    def list_alert_hub(
        self,
        *,
        chat_id: str,
        view: str = "triggered",
        limit: int = 10,
        symbol: str | None = None,
        channel: str | None = None,
    ) -> list[AlertHubRecord]:
        filters = ["wj.chat_id = ?"]
        params: list[Any] = [str(chat_id)]
        if symbol:
            filters.append("wj.symbol = ?")
            params.append(symbol.upper())
        if channel:
            filters.append("n.channel = ?")
            params.append(channel)
        if view == "failed":
            filters.append("n.state IN ('retry_pending', 'retrying', 'dlq')")
        elif view == "suppressed":
            filters.append("n.state = 'suppressed'")
        else:
            filters.append("n.state = 'delivered'")
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT we.event_id, wj.chat_id, wj.symbol, we.priority, we.strategy_tier, n.channel, n.state, n.suppressed_reason, n.last_error,
                       we.trigger_ts, n.updated_at
                FROM notifications n
                JOIN watch_events we ON we.event_id = n.event_id
                JOIN watch_jobs wj ON wj.job_id = we.job_id
                WHERE {' AND '.join(filters)}
                ORDER BY n.updated_at DESC
                LIMIT ?
                """,
                (*params, max(1, int(limit))),
            ).fetchall()
        return [
            AlertHubRecord(
                event_id=str(row["event_id"]),
                chat_id=str(row["chat_id"]),
                symbol=str(row["symbol"]),
                priority=str(row["priority"]),
                strategy_tier=normalize_strategy_tier(str(row["strategy_tier"]) if row["strategy_tier"] else DEFAULT_STRATEGY_TIER),
                channel=str(row["channel"]),
                status=str(row["state"]),
                suppressed_reason=str(row["suppressed_reason"]) if row["suppressed_reason"] else None,
                last_error=str(row["last_error"]) if row["last_error"] else None,
                trigger_ts=str(row["trigger_ts"]),
                updated_at=str(row["updated_at"]),
            )
            for row in rows
        ]

    def check_and_increment_command_rate_limit(
        self,
        *,
        chat_id: str,
        max_per_minute: int,
        rate_scope: str = "command",
        now: datetime | None = None,
    ) -> tuple[bool, int]:
        current = now or _utc_now_dt()
        window_start = current.replace(second=0, microsecond=0)
        window_iso = _isoformat(window_start)
        scope_key = f"{rate_scope}:{chat_id}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO command_rate_limits(chat_id, window_start, command_count)
                VALUES(?, ?, 0)
                ON CONFLICT(chat_id, window_start) DO NOTHING
                """,
                (scope_key, window_iso),
            )
            row = conn.execute(
                """
                SELECT command_count
                FROM command_rate_limits
                WHERE chat_id = ? AND window_start = ?
                """,
                (scope_key, window_iso),
            ).fetchone()
            current_count = int(row["command_count"]) if row else 0
            if current_count >= max_per_minute:
                return False, current_count
            next_count = current_count + 1
            conn.execute(
                """
                UPDATE command_rate_limits
                SET command_count = ?
                WHERE chat_id = ? AND window_start = ?
                """,
                (next_count, scope_key, window_iso),
            )
            return True, next_count

    def count_recent_nl_requests(self, *, chat_id: str, since: datetime) -> int:
        since_iso = _isoformat(since)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM nl_requests
                WHERE chat_id = ? AND created_at >= ?
                """,
                (str(chat_id), since_iso),
            ).fetchone()
        return int(row["c"])

    def count_active_watch_jobs(self, *, chat_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM watch_jobs WHERE chat_id = ? AND enabled = 1",
                (str(chat_id),),
            ).fetchone()
        return int(row["c"])

    def create_watch_job(
        self,
        *,
        chat_id: str,
        symbol: str,
        interval_sec: int,
        scope: str = "single",
        group_id: str | None = None,
        route_strategy: str = "dual_channel",
        strategy_tier: str = DEFAULT_STRATEGY_TIER,
        template_id: str | None = None,
        market: str = "auto",
        threshold: float = 0.03,
        mode: str = "anomaly",
        now: datetime | None = None,
    ) -> WatchJobRecord:
        ts = now or _utc_now_dt()
        now_iso = _isoformat(ts)
        next_run_at = _isoformat(ts + timedelta(seconds=interval_sec))
        existing = self.find_enabled_watch_job(chat_id=chat_id, symbol=symbol)
        if (
            existing is not None
            and existing.interval_sec == interval_sec
            and existing.mode == mode
            and existing.market == market
            and existing.scope == scope
            and existing.route_strategy == route_strategy
            and existing.strategy_tier == normalize_strategy_tier(strategy_tier)
            and existing.group_id == group_id
        ):
            return existing

        job_id = f"job-{uuid4().hex[:8]}"
        normalized_tier = normalize_strategy_tier(strategy_tier)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watch_jobs(
                    job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, group_id, route_strategy, strategy_tier, template_id,
                    enabled, next_run_at,
                    created_at, updated_at, last_run_at, last_triggered_at, last_error
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, NULL, NULL)
                """,
                (
                    job_id,
                    str(chat_id),
                    symbol.upper(),
                    market,
                    int(interval_sec),
                    float(threshold),
                    mode,
                    scope,
                    group_id,
                    route_strategy,
                    normalized_tier,
                    template_id,
                    next_run_at,
                    now_iso,
                    now_iso,
                ),
            )
        return self.get_watch_job(job_id=job_id)

    def get_watch_job(self, *, job_id: str) -> WatchJobRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, group_id, route_strategy, strategy_tier, template_id, enabled,
                       next_run_at, created_at, updated_at, last_run_at, last_triggered_at, last_error
                FROM watch_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"watch job not found: {job_id}")
        return WatchJobRecord(
            job_id=str(row["job_id"]),
            chat_id=str(row["chat_id"]),
            symbol=str(row["symbol"]),
            market=str(row["market"]),
            interval_sec=int(row["interval_sec"]),
            threshold=float(row["threshold"]),
            mode=str(row["mode"]),
            scope=str(row["scope"]) if row["scope"] else "single",
            group_id=str(row["group_id"]) if row["group_id"] else None,
            route_strategy=str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
            strategy_tier=normalize_strategy_tier(str(row["strategy_tier"]) if row["strategy_tier"] else DEFAULT_STRATEGY_TIER),
            template_id=str(row["template_id"]) if row["template_id"] else None,
            enabled=bool(int(row["enabled"])),
            next_run_at=str(row["next_run_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            last_run_at=str(row["last_run_at"]) if row["last_run_at"] else None,
            last_triggered_at=str(row["last_triggered_at"]) if row["last_triggered_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
        )

    def find_enabled_watch_job(self, *, chat_id: str, symbol: str) -> WatchJobRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, group_id, route_strategy, strategy_tier, template_id, enabled,
                       next_run_at, created_at, updated_at, last_run_at, last_triggered_at, last_error
                FROM watch_jobs
                WHERE chat_id = ? AND symbol = ? AND enabled = 1
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (str(chat_id), symbol.upper()),
            ).fetchone()
        if row is None:
            return None
        return WatchJobRecord(
            job_id=str(row["job_id"]),
            chat_id=str(row["chat_id"]),
            symbol=str(row["symbol"]),
            market=str(row["market"]),
            interval_sec=int(row["interval_sec"]),
            threshold=float(row["threshold"]),
            mode=str(row["mode"]),
            scope=str(row["scope"]) if row["scope"] else "single",
            group_id=str(row["group_id"]) if row["group_id"] else None,
            route_strategy=str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
            strategy_tier=normalize_strategy_tier(str(row["strategy_tier"]) if row["strategy_tier"] else DEFAULT_STRATEGY_TIER),
            template_id=str(row["template_id"]) if row["template_id"] else None,
            enabled=bool(int(row["enabled"])),
            next_run_at=str(row["next_run_at"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            last_run_at=str(row["last_run_at"]) if row["last_run_at"] else None,
            last_triggered_at=str(row["last_triggered_at"]) if row["last_triggered_at"] else None,
            last_error=str(row["last_error"]) if row["last_error"] else None,
        )

    def list_watch_jobs(self, *, chat_id: str, include_disabled: bool = False) -> list[WatchJobRecord]:
        where_clause = "chat_id = ?" if include_disabled else "chat_id = ? AND enabled = 1"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, group_id, route_strategy, strategy_tier, template_id, enabled,
                       next_run_at, created_at, updated_at, last_run_at, last_triggered_at, last_error
                FROM watch_jobs
                WHERE {where_clause}
                ORDER BY created_at ASC
                """,
                (str(chat_id),),
            ).fetchall()
        return [
            WatchJobRecord(
                job_id=str(row["job_id"]),
                chat_id=str(row["chat_id"]),
                symbol=str(row["symbol"]),
                market=str(row["market"]),
                interval_sec=int(row["interval_sec"]),
                threshold=float(row["threshold"]),
                mode=str(row["mode"]),
                scope=str(row["scope"]) if row["scope"] else "single",
                group_id=str(row["group_id"]) if row["group_id"] else None,
                route_strategy=str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
                strategy_tier=normalize_strategy_tier(str(row["strategy_tier"]) if row["strategy_tier"] else DEFAULT_STRATEGY_TIER),
                template_id=str(row["template_id"]) if row["template_id"] else None,
                enabled=bool(int(row["enabled"])),
                next_run_at=str(row["next_run_at"]),
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
                last_run_at=str(row["last_run_at"]) if row["last_run_at"] else None,
                last_triggered_at=str(row["last_triggered_at"]) if row["last_triggered_at"] else None,
                last_error=str(row["last_error"]) if row["last_error"] else None,
            )
            for row in rows
        ]

    def disable_watch_job(self, *, chat_id: str, target: str, target_type: str) -> int:
        now = _utc_now()
        with self._connect() as conn:
            if target_type == "job_id":
                cursor = conn.execute(
                    """
                    UPDATE watch_jobs
                    SET enabled = 0, updated_at = ?
                    WHERE chat_id = ? AND job_id = ? AND enabled = 1
                    """,
                    (now, str(chat_id), target),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE watch_jobs
                    SET enabled = 0, updated_at = ?
                    WHERE chat_id = ? AND symbol = ? AND enabled = 1
                    """,
                    (now, str(chat_id), target.upper()),
                )
            return int(cursor.rowcount)

    def claim_due_watch_jobs(self, *, now: datetime | None = None, limit: int = 20) -> list[DueWatchJob]:
        current = now or _utc_now_dt()
        now_iso = _isoformat(current)
        claimed: list[DueWatchJob] = []

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, scope, group_id, route_strategy, strategy_tier, next_run_at
                FROM watch_jobs
                WHERE enabled = 1 AND next_run_at <= ?
                ORDER BY next_run_at ASC
                LIMIT ?
                """,
                (now_iso, limit),
            ).fetchall()

            for row in rows:
                interval_sec = int(row["interval_sec"])
                previous_next_run_at = str(row["next_run_at"])
                next_run_at = _isoformat(current + timedelta(seconds=interval_sec))
                updated = conn.execute(
                    """
                    UPDATE watch_jobs
                    SET next_run_at = ?, updated_at = ?, last_run_at = ?
                    WHERE job_id = ? AND next_run_at = ? AND enabled = 1
                    """,
                    (next_run_at, now_iso, now_iso, str(row["job_id"]), previous_next_run_at),
                )
                if updated.rowcount <= 0:
                    continue
                claimed.append(
                    DueWatchJob(
                        job_id=str(row["job_id"]),
                        chat_id=str(row["chat_id"]),
                        symbol=str(row["symbol"]),
                        market=str(row["market"]),
                        interval_sec=interval_sec,
                        threshold=float(row["threshold"]),
                        mode=str(row["mode"]),
                        scope=str(row["scope"]) if row["scope"] else "single",
                        group_id=str(row["group_id"]) if row["group_id"] else None,
                        route_strategy=str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
                        strategy_tier=normalize_strategy_tier(
                            str(row["strategy_tier"]) if row["strategy_tier"] else DEFAULT_STRATEGY_TIER
                        ),
                        next_run_at=previous_next_run_at,
                    )
                )
            conn.commit()

        return claimed

    def mark_watch_job_error(self, *, job_id: str, error: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE watch_jobs
                SET last_error = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (error, _utc_now(), job_id),
            )

    def mark_watch_job_triggered(self, *, job_id: str, triggered_at: datetime | None = None) -> None:
        ts = _isoformat(triggered_at or _utc_now_dt())
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE watch_jobs
                SET last_triggered_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (ts, ts, job_id),
            )

    def get_recent_watch_event_summary(self, *, job_id: str) -> tuple[str | None, float | None]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT trigger_ts, pct_change
                FROM watch_events
                WHERE job_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None, None
        return str(row["trigger_ts"]), float(row["pct_change"])

    def get_watch_event(self, *, event_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT we.event_id, we.job_id, we.trigger_ts, we.price, we.pct_change, we.reason, we.rule, we.run_id, we.priority, we.dedupe_key,
                       we.strategy_tier,
                       wj.chat_id, wj.symbol, wj.route_strategy, wj.strategy_tier AS job_strategy_tier
                FROM watch_events we
                JOIN watch_jobs wj ON wj.job_id = we.job_id
                WHERE we.event_id = ?
                """,
                (event_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "event_id": str(row["event_id"]),
            "job_id": str(row["job_id"]),
            "trigger_ts": str(row["trigger_ts"]),
            "price": float(row["price"]),
            "pct_change": float(row["pct_change"]),
            "reason": str(row["reason"]),
            "rule": str(row["rule"]),
            "run_id": str(row["run_id"]) if row["run_id"] else None,
            "priority": str(row["priority"]) if row["priority"] else "medium",
            "dedupe_key": str(row["dedupe_key"]),
            "chat_id": str(row["chat_id"]),
            "symbol": str(row["symbol"]),
            "route_strategy": str(row["route_strategy"]) if row["route_strategy"] else "dual_channel",
            "strategy_tier": normalize_strategy_tier(
                str(row["strategy_tier"] or row["job_strategy_tier"] or DEFAULT_STRATEGY_TIER)
            ),
        }

    def record_watch_event_if_new(
        self,
        *,
        job_id: str,
        symbol: str,
        trigger_ts: datetime,
        price: float,
        pct_change: float,
        reason: str,
        rule: str,
        priority: str,
        strategy_tier: str = DEFAULT_STRATEGY_TIER,
        run_id: str | None,
        bucket_minutes: int = 15,
    ) -> tuple[str, bool]:
        bucket_seconds = max(60, int(bucket_minutes) * 60)
        trigger_epoch = int(trigger_ts.timestamp())
        bucket_epoch = trigger_epoch - (trigger_epoch % bucket_seconds)
        bucket_dt = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        bucket_ts = _isoformat(bucket_dt)
        dedupe_key = f"{job_id}:{symbol.upper()}:{rule}:{bucket_ts}"
        event_id = f"evt-{uuid4().hex[:10]}"
        now = _utc_now()

        normalized_tier = normalize_strategy_tier(strategy_tier)
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO watch_events(
                    event_id, job_id, trigger_ts, price, pct_change, reason, rule, priority, strategy_tier, bucket_ts,
                    dedupe_key, pushed, run_id, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    event_id,
                    job_id,
                    _isoformat(trigger_ts),
                    float(price),
                    float(pct_change),
                    reason,
                    rule,
                    priority,
                    normalized_tier,
                    bucket_ts,
                    dedupe_key,
                    run_id,
                    now,
                ),
            )
            if cursor.rowcount > 0:
                return event_id, True

            row = conn.execute(
                "SELECT event_id FROM watch_events WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            existing_event_id = str(row["event_id"]) if row else event_id
            return existing_event_id, False

    def mark_watch_event_pushed(self, *, event_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE watch_events SET pushed = 1 WHERE event_id = ?",
                (event_id,),
            )

    def upsert_notification_state(
        self,
        *,
        event_id: str,
        channel: str,
        state: str,
        retry_count: int = 0,
        next_retry_at: str | None = None,
        last_error: str | None = None,
        delivered_at: str | None = None,
        suppressed_reason: str | None = None,
        reason: str | None = None,
    ) -> None:
        now = _utc_now()
        notification_id = f"ntf-{uuid4().hex[:10]}"
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT notification_id, state FROM notifications WHERE event_id = ? AND channel = ?",
                (event_id, channel),
            ).fetchone()
            from_state = str(existing["state"]) if existing else None
            existing_notification_id = str(existing["notification_id"]) if existing else None

            conn.execute(
                """
                INSERT INTO notifications(
                    notification_id, event_id, channel, state, retry_count, next_retry_at,
                    last_error, delivered_at, suppressed_reason, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id, channel) DO UPDATE SET
                    state = excluded.state,
                    retry_count = excluded.retry_count,
                    next_retry_at = excluded.next_retry_at,
                    last_error = excluded.last_error,
                    delivered_at = excluded.delivered_at,
                    suppressed_reason = excluded.suppressed_reason,
                    updated_at = excluded.updated_at
                """,
                (
                    existing_notification_id or notification_id,
                    event_id,
                    channel,
                    state,
                    int(retry_count),
                    next_retry_at,
                    last_error,
                    delivered_at,
                    suppressed_reason,
                    now,
                    now,
                ),
            )

            if from_state != state:
                conn.execute(
                    """
                    INSERT INTO notification_state_transitions(notification_id, event_id, channel, from_state, to_state, reason, created_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (existing_notification_id or notification_id, event_id, channel, from_state, state, reason, now),
                )

    def claim_due_notification_retries(self, *, now: datetime | None = None, limit: int = 20) -> list[DueNotification]:
        current = now or _utc_now_dt()
        now_iso = _isoformat(current)
        claimed: list[DueNotification] = []
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT notification_id, event_id, channel, retry_count, last_error
                FROM notifications
                WHERE state = 'retry_pending' AND next_retry_at IS NOT NULL AND next_retry_at <= ?
                ORDER BY next_retry_at ASC
                LIMIT ?
                """,
                (now_iso, max(1, int(limit))),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """
                    UPDATE notifications
                    SET state = 'retrying', updated_at = ?
                    WHERE notification_id = ?
                    """,
                    (now_iso, str(row["notification_id"])),
                )
                conn.execute(
                    """
                    INSERT INTO notification_state_transitions(notification_id, event_id, channel, from_state, to_state, reason, created_at)
                    VALUES(?, ?, ?, 'retry_pending', 'retrying', 'retry dispatch', ?)
                    """,
                    (str(row["notification_id"]), str(row["event_id"]), str(row["channel"]), now_iso),
                )
                claimed.append(
                    DueNotification(
                        notification_id=str(row["notification_id"]),
                        event_id=str(row["event_id"]),
                        channel=str(row["channel"]),
                        retry_count=int(row["retry_count"]),
                        last_error=str(row["last_error"]) if row["last_error"] else None,
                    )
                )
            conn.commit()
        return claimed

    def add_audit_event(
        self,
        *,
        event_type: str,
        chat_id: str | None,
        update_id: int | None,
        action: str | None,
        reason: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_events(event_type, chat_id, update_id, action, reason, metadata, created_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_type,
                    str(chat_id) if chat_id is not None else None,
                    update_id,
                    action,
                    reason,
                    _json_dumps(metadata or {}),
                    _utc_now(),
                ),
            )

    def count_audit_events(self, *, event_type: str | None = None) -> int:
        with self._connect() as conn:
            if event_type:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_events WHERE event_type = ?", (event_type,)).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM audit_events").fetchone()
        return int(row["c"])

    def record_metric(self, *, metric_name: str, metric_value: float, tags: dict[str, Any] | None = None, created_at: datetime | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO metric_events(metric_name, metric_value, tags, created_at)
                VALUES(?, ?, ?, ?)
                """,
                (
                    metric_name,
                    float(metric_value),
                    _json_dumps({k: str(v) for k, v in (tags or {}).items()}),
                    _isoformat(created_at or _utc_now_dt()),
                ),
            )

    def metric_values(self, *, metric_name: str, since: datetime | None = None) -> list[float]:
        since_iso = _isoformat(since) if since else None
        query = "SELECT metric_value FROM metric_events WHERE metric_name = ?"
        params: list[Any] = [metric_name]
        if since_iso:
            query += " AND created_at >= ?"
            params.append(since_iso)
        query += " ORDER BY created_at ASC"
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [float(row["metric_value"]) for row in rows]

    def count_metric_events(self, *, metric_name: str, since: datetime | None = None) -> int:
        since_iso = _isoformat(since) if since else None
        query = "SELECT COUNT(*) AS c FROM metric_events WHERE metric_name = ?"
        params: list[Any] = [metric_name]
        if since_iso:
            query += " AND created_at >= ?"
            params.append(since_iso)
        with self._connect() as conn:
            row = conn.execute(query, tuple(params)).fetchone()
        return int(row["c"])

    def metric_tag_topk(self, *, metric_name: str, tag_key: str, limit: int = 3) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT tags
                FROM metric_events
                WHERE metric_name = ?
                ORDER BY created_at DESC
                LIMIT 2000
                """,
                (metric_name,),
            ).fetchall()
        counter: dict[str, int] = {}
        for row in rows:
            raw = row["tags"]
            if not raw:
                continue
            try:
                tags = json.loads(str(raw))
            except json.JSONDecodeError:
                continue
            if not isinstance(tags, dict):
                continue
            value = str(tags.get(tag_key, "")).strip() or "unknown"
            counter[value] = counter.get(value, 0) + 1
        ranked = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        return [{"reason": reason, "count": count} for reason, count in ranked[: max(1, int(limit))]]

    def get_degradation_state(self, *, state_key: str) -> DegradationState | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT state_key, status, triggered_at, recovered_at, reason, updated_at
                FROM degradation_states
                WHERE state_key = ?
                """,
                (state_key,),
            ).fetchone()
        if row is None:
            return None
        return DegradationState(
            state_key=str(row["state_key"]),
            status=str(row["status"]),
            triggered_at=str(row["triggered_at"]) if row["triggered_at"] else None,
            recovered_at=str(row["recovered_at"]) if row["recovered_at"] else None,
            reason=str(row["reason"]) if row["reason"] else None,
            updated_at=str(row["updated_at"]),
        )

    def set_degradation_state(self, *, state_key: str, status: str, reason: str) -> None:
        now = _utc_now()
        state = self.get_degradation_state(state_key=state_key)
        changed = state is None or state.status != status
        with self._connect() as conn:
            if status == "active":
                conn.execute(
                    """
                    INSERT INTO degradation_states(state_key, status, triggered_at, recovered_at, reason, updated_at)
                    VALUES(?, 'active', ?, NULL, ?, ?)
                    ON CONFLICT(state_key) DO UPDATE SET
                        status = 'active',
                        triggered_at = COALESCE(degradation_states.triggered_at, excluded.triggered_at),
                        recovered_at = NULL,
                        reason = excluded.reason,
                        updated_at = excluded.updated_at
                    """,
                    (state_key, now, reason, now),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO degradation_states(state_key, status, triggered_at, recovered_at, reason, updated_at)
                    VALUES(?, 'recovered', NULL, ?, ?, ?)
                    ON CONFLICT(state_key) DO UPDATE SET
                        status = 'recovered',
                        recovered_at = excluded.recovered_at,
                        reason = excluded.reason,
                        updated_at = excluded.updated_at
                    """,
                    (state_key, now, reason, now),
                )
            if changed:
                conn.execute(
                    """
                    INSERT INTO degradation_events(state_key, event_type, reason, created_at)
                    VALUES(?, ?, ?, ?)
                    """,
                    (state_key, "triggered" if status == "active" else "recovered", reason, now),
                )

    def is_degradation_active(self, *, state_key: str) -> bool:
        state = self.get_degradation_state(state_key=state_key)
        return bool(state and state.status == "active")

    def count_watch_events(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM watch_events").fetchone()
        return int(row["c"])

    def count_delivered_notifications(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM notifications WHERE state = 'delivered'"
            ).fetchone()
        return int(row["c"])

    def count_retry_queue_depth(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM notifications WHERE state IN ('retry_pending', 'retrying')"
            ).fetchone()
        return int(row["c"])

    def count_dlq(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM notifications WHERE state = 'dlq'").fetchone()
        return int(row["c"])

    def count_suppressed_notifications(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM notifications WHERE state = 'suppressed'").fetchone()
        return int(row["c"])

    def count_notification_state_transitions(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM notification_state_transitions").fetchone()
        return int(row["c"])

    def build_daily_digest(self, *, chat_id: str, now: datetime | None = None) -> dict[str, Any]:
        current = now or _utc_now_dt()
        since = current - timedelta(days=1)
        since_iso = _isoformat(since)
        with self._connect() as conn:
            active_jobs = int(
                conn.execute(
                    "SELECT COUNT(*) AS c FROM watch_jobs WHERE chat_id = ? AND enabled = 1",
                    (str(chat_id),),
                ).fetchone()["c"]
            )
            alerts_triggered = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM watch_events we
                    JOIN watch_jobs wj ON wj.job_id = we.job_id
                    WHERE wj.chat_id = ? AND we.created_at >= ?
                    """,
                    (str(chat_id), since_iso),
                ).fetchone()["c"]
            )
            delivered = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM notifications n
                    JOIN watch_events we ON we.event_id = n.event_id
                    JOIN watch_jobs wj ON wj.job_id = we.job_id
                    WHERE wj.chat_id = ? AND n.state = 'delivered' AND n.updated_at >= ?
                    """,
                    (str(chat_id), since_iso),
                ).fetchone()["c"]
            )
            completed_analyses = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM analysis_requests
                    WHERE chat_id = ? AND status = 'completed' AND updated_at >= ?
                    """,
                    (str(chat_id), since_iso),
                ).fetchone()["c"]
            )
            rows = conn.execute(
                """
                SELECT run_id, symbol, summary
                FROM analysis_reports
                WHERE chat_id = ? AND updated_at >= ?
                ORDER BY updated_at DESC
                LIMIT 3
                """,
                (str(chat_id), since_iso),
            ).fetchall()
        latest_reports = [
            {
                "run_id": str(row["run_id"]),
                "symbol": str(row["symbol"]),
                "summary": str(row["summary"]),
            }
            for row in rows
        ]
        return {
            "chat_id": str(chat_id),
            "window_start": since_iso,
            "window_end": _isoformat(current),
            "active_jobs": active_jobs,
            "alerts_triggered": alerts_triggered,
            "delivered_notifications": delivered,
            "completed_analyses": completed_analyses,
            "latest_reports": latest_reports,
        }

    def verification_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            processed_updates = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM bot_updates
                    WHERE status IN ('processed', 'retried')
                    """
                ).fetchone()["c"]
            )
            distinct_updates = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT update_id) AS c
                    FROM bot_updates
                    """
                ).fetchone()["c"]
            )
            duplicate_running_or_completed = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM (
                        SELECT request_id, COUNT(*) AS n
                        FROM analysis_requests
                        WHERE status IN ('running', 'completed')
                        GROUP BY request_id
                        HAVING n > 1
                    ) t
                    """
                ).fetchone()["c"]
            )
            dedupe_suppressed_count = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c
                    FROM metric_events
                    WHERE metric_name = 'dedupe_suppressed_count' AND metric_value > 0
                    """
                ).fetchone()["c"]
            )
        return {
            "processed_updates": processed_updates,
            "distinct_updates": distinct_updates,
            "duplicate_running_or_completed": duplicate_running_or_completed,
            "dedupe_suppressed_count": dedupe_suppressed_count,
            "retry_queue_depth": self.count_retry_queue_depth(),
            "dlq_count": self.count_dlq(),
            "notification_state_transition_total": self.count_notification_state_transitions(),
        }

    @staticmethod
    def _p95(values: list[float]) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        idx = max(0, int(round(0.95 * (len(ordered) - 1))))
        return float(ordered[idx])

    def build_phase_c_run_report(self) -> dict[str, float | int]:
        command_total = self.count_metric_events(metric_name="command_total")
        command_success = self.count_metric_events(metric_name="command_success")
        monitor_triggered = self.count_metric_events(metric_name="monitor_trigger")
        monitor_executed = self.count_metric_events(metric_name="monitor_executed")
        push_attempts = self.count_metric_events(metric_name="push_attempt")
        push_success = self.count_metric_events(metric_name="push_success")

        command_success_rate = (command_success / command_total) if command_total else 1.0
        monitor_trigger_rate = (monitor_triggered / monitor_executed) if monitor_executed else 0.0
        push_success_rate = (push_success / push_attempts) if push_attempts else 1.0

        p95_command_latency = self._p95(self.metric_values(metric_name="command_latency_ms"))
        p95_analysis_latency = self._p95(self.metric_values(metric_name="analysis_latency_ms"))
        llm_parse_latency_p95 = self._p95(self.metric_values(metric_name="llm_parse_latency_ms"))

        llm_parse_total = self.count_metric_events(metric_name="llm_parse_total")
        llm_parse_failed = self.count_metric_events(metric_name="llm_parse_failed")
        llm_parse_fail_rate = (llm_parse_failed / llm_parse_total) if llm_parse_total else 0.0

        nl_intent_total = self.count_metric_events(metric_name="nl_intent_total")
        nl_intent_success = self.count_metric_events(metric_name="nl_intent_success")
        nl_intent_reject = self.count_metric_events(metric_name="nl_intent_reject")
        nl_intent_fallback_help = self.count_metric_events(metric_name="nl_intent_fallback_help")
        nl_confirm_timeout_count = self.count_metric_events(metric_name="nl_confirm_timeout_count")
        nl_dedupe_suppressed_count = self.count_metric_events(metric_name="nl_dedupe_suppressed_count")
        nl_clarify_asked_total = self.count_metric_events(metric_name="nl_clarify_asked_total")
        nl_clarify_resolved_total = self.count_metric_events(metric_name="nl_clarify_resolved_total")
        nl_clarify_resolved_rate = (
            nl_clarify_resolved_total / nl_clarify_asked_total if nl_clarify_asked_total else 1.0
        )

        chart_attempt_total = self.count_metric_events(metric_name="chart_render_attempt_total")
        chart_fail_total = self.count_metric_events(metric_name="chart_render_fail_total")
        chart_render_fail_rate = (chart_fail_total / chart_attempt_total) if chart_attempt_total else 0.0
        chart_payload_bytes_p95 = self._p95(self.metric_values(metric_name="chart_payload_bytes"))

        duplicate_update_dropped = self.count_metric_events(metric_name="duplicate_update_dropped")
        dedupe_suppressed = int(sum(self.metric_values(metric_name="dedupe_suppressed_count")))

        return {
            "command_success_rate": round(command_success_rate, 4),
            "monitor_trigger_rate": round(monitor_trigger_rate, 4),
            "push_success_rate": round(push_success_rate, 4),
            "p95_command_latency": round(p95_command_latency, 3),
            "p95_analysis_latency": round(p95_analysis_latency, 3),
            "duplicate_update_dropped": duplicate_update_dropped,
            "dedupe_suppressed_count": dedupe_suppressed,
            "nl_intent_total": nl_intent_total,
            "nl_intent_success": nl_intent_success,
            "nl_intent_reject": nl_intent_reject,
            "nl_intent_fallback_help": nl_intent_fallback_help,
            "nl_confirm_timeout_count": nl_confirm_timeout_count,
            "llm_parse_latency_p95": round(llm_parse_latency_p95, 3),
            "llm_parse_fail_rate": round(llm_parse_fail_rate, 4),
            "nl_dedupe_suppressed_count": nl_dedupe_suppressed_count,
            "nl_clarify_asked_total": nl_clarify_asked_total,
            "nl_clarify_resolved_rate": round(nl_clarify_resolved_rate, 4),
            "chart_render_fail_rate": round(chart_render_fail_rate, 4),
            "chart_payload_bytes_p95": round(chart_payload_bytes_p95, 3),
            "retry_queue_depth": self.count_retry_queue_depth(),
            "dlq_count": self.count_dlq(),
            "notification_state_transition_total": self.count_notification_state_transitions(),
        }

    def _list_audit_metadata(self, *, event_type: str, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT metadata
                FROM audit_events
                WHERE event_type = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (event_type, int(max(1, limit))),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            payload = row["metadata"]
            if not payload:
                continue
            try:
                data = json.loads(str(payload))
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                output.append(data)
        return output

    def list_nl_execution_evidence(self, *, request_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._list_audit_metadata(event_type="nl_execution_evidence", limit=limit)
        if request_id is None:
            return rows
        return [row for row in rows if str(row.get("request_id", "")) == str(request_id)]

    def list_nl_plan_step_events(self, *, request_id: str | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self._list_audit_metadata(event_type="nl_plan_step", limit=limit)
        if request_id is None:
            return rows
        return [row for row in rows if str(row.get("request_id", "")) == str(request_id)]

    def build_phase_d_run_report(self) -> dict[str, float | int]:
        report = dict(self.build_phase_c_run_report())
        report_lookup_success = self.count_metric_events(metric_name="report_lookup_success")
        report_lookup_total = self.count_metric_events(metric_name="report_lookup_total")
        digest_generated = self.count_metric_events(metric_name="digest_generated")
        channel_attempt = self.count_metric_events(metric_name="channel_dispatch_attempt")
        channel_success = self.count_metric_events(metric_name="channel_dispatch_success")
        gray_denied = self.count_audit_events(event_type="gray_release_denied")

        report["report_lookup_success_rate"] = round(
            (report_lookup_success / report_lookup_total) if report_lookup_total else 1.0,
            4,
        )
        report["digest_generated_total"] = digest_generated
        report["channel_dispatch_success_rate"] = round(
            (channel_success / channel_attempt) if channel_attempt else 1.0,
            4,
        )
        report["gray_release_denied_total"] = gray_denied
        report["enabled_notification_routes"] = self._count_enabled_notification_routes()
        report["delivered_notifications_total"] = self.count_delivered_notifications()
        report["suppressed_notifications_total"] = self.count_suppressed_notifications()
        report["enabled_outbound_webhooks"] = self._count_enabled_outbound_webhooks()
        report["watchlist_groups_total"] = self._count_watchlist_groups()
        evidence_rows = self.list_nl_execution_evidence(limit=1000)
        plan_rows = self.list_nl_plan_step_events(limit=2000)
        report["nl_execution_evidence_total"] = len(evidence_rows)
        report["nl_plan_step_total"] = len(plan_rows)
        report["nl_plan_step_failed_total"] = sum(1 for row in plan_rows if str(row.get("status", "")).lower() == "failed")
        report["nl_evidence_mapped_total"] = sum(
            1
            for row in evidence_rows
            if row.get("request_id") and row.get("schema_version") and row.get("action_version")
        )
        asked = self.count_metric_events(metric_name="nl_clarify_asked_total")
        resolved = self.count_metric_events(metric_name="nl_clarify_resolved_total")
        report["clarify_followup_success_rate"] = round((resolved / asked) if asked else 1.0, 4)
        analysis_total = self.count_metric_events(metric_name="analysis_response_total")
        explainable_total = self.count_metric_events(metric_name="analysis_explainable_total")
        report["analysis_explainability_rate"] = round(
            (explainable_total / analysis_total) if analysis_total else 1.0,
            4,
        )
        report["chart_fail_reason_topk"] = self.metric_tag_topk(
            metric_name="chart_render_fail_rate",
            tag_key="reason",
            limit=3,
        )
        carry_hit = self.count_metric_events(metric_name="symbol_carry_over_hit_rate")
        clarify_asked = self.count_metric_events(metric_name="nl_clarify_asked_total")
        report["clarify_avoid_rate"] = round((carry_hit / (carry_hit + clarify_asked)) if (carry_hit + clarify_asked) else 1.0, 4)
        retry_attempted = self.count_metric_events(metric_name="chart_retry_attempted")
        retry_success = self.count_metric_events(metric_name="chart_retry_success")
        report["chart_success_rate_after_retry"] = round((retry_success / retry_attempted) if retry_attempted else 1.0, 4)
        evidence_visible_total = int(sum(self.metric_values(metric_name="evidence_visible_total")))
        analysis_total = self.count_metric_events(metric_name="analysis_response_total")
        report["evidence_visible_rate"] = round((evidence_visible_total / analysis_total) if analysis_total else 1.0, 4)
        return report

    def _count_enabled_notification_routes(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM notification_routes WHERE enabled = 1"
            ).fetchone()
        return int(row["c"])

    def _count_enabled_outbound_webhooks(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM outbound_webhooks WHERE enabled = 1").fetchone()
        return int(row["c"])

    def _count_watchlist_groups(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM watchlist_groups").fetchone()
        return int(row["c"])
