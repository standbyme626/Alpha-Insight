from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


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
    next_run_at: str


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
                CREATE TABLE IF NOT EXISTS feature_flags (
                    flag_key TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_jobs_chat_enabled ON watch_jobs(chat_id, enabled)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_watch_jobs_next_run_at ON watch_jobs(next_run_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_notifications_state_next_retry ON notifications(state, next_retry_at)")

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

    def upsert_telegram_chat(self, *, chat_id: str, user_id: str | None, username: str | None, status: str = "active") -> None:
        now = _utc_now()
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
                (str(chat_id), str(user_id) if user_id else None, username, now, status),
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

    def create_watch_job(
        self,
        *,
        chat_id: str,
        symbol: str,
        interval_sec: int,
        market: str = "auto",
        threshold: float = 0.03,
        mode: str = "anomaly",
        now: datetime | None = None,
    ) -> WatchJobRecord:
        ts = now or _utc_now_dt()
        now_iso = _isoformat(ts)
        next_run_at = _isoformat(ts + timedelta(seconds=interval_sec))
        existing = self.find_enabled_watch_job(chat_id=chat_id, symbol=symbol)
        if existing is not None and existing.interval_sec == interval_sec and existing.mode == mode and existing.market == market:
            return existing

        job_id = f"job-{uuid4().hex[:8]}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watch_jobs(
                    job_id, chat_id, symbol, market, interval_sec, threshold, mode, enabled, next_run_at,
                    created_at, updated_at, last_run_at, last_triggered_at, last_error
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, NULL, NULL, NULL)
                """,
                (
                    job_id,
                    str(chat_id),
                    symbol.upper(),
                    market,
                    int(interval_sec),
                    float(threshold),
                    mode,
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
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, enabled,
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
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, enabled,
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
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, enabled,
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
                SELECT job_id, chat_id, symbol, market, interval_sec, threshold, mode, next_run_at
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

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO watch_events(
                    event_id, job_id, trigger_ts, price, pct_change, reason, rule, bucket_ts,
                    dedupe_key, pushed, run_id, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    event_id,
                    job_id,
                    _isoformat(trigger_ts),
                    float(price),
                    float(pct_change),
                    reason,
                    rule,
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
    ) -> None:
        now = _utc_now()
        notification_id = f"ntf-{uuid4().hex[:10]}"
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notifications(
                    notification_id, event_id, channel, state, retry_count, next_retry_at,
                    last_error, delivered_at, created_at, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id, channel) DO UPDATE SET
                    state = excluded.state,
                    retry_count = excluded.retry_count,
                    next_retry_at = excluded.next_retry_at,
                    last_error = excluded.last_error,
                    delivered_at = excluded.delivered_at,
                    updated_at = excluded.updated_at
                """,
                (
                    notification_id,
                    event_id,
                    channel,
                    state,
                    int(retry_count),
                    next_retry_at,
                    last_error,
                    delivered_at,
                    now,
                    now,
                ),
            )

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
                    FROM (
                        SELECT dedupe_key, COUNT(*) AS n
                        FROM watch_events
                        GROUP BY dedupe_key
                        HAVING n > 1
                    ) t
                    """
                ).fetchone()["c"]
            )
        return {
            "processed_updates": processed_updates,
            "distinct_updates": distinct_updates,
            "duplicate_running_or_completed": duplicate_running_or_completed,
            "dedupe_suppressed_count": dedupe_suppressed_count,
        }
