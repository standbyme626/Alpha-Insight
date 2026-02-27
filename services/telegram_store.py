from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        return {
            "processed_updates": processed_updates,
            "distinct_updates": distinct_updates,
            "duplicate_running_or_completed": duplicate_running_or_completed,
        }

