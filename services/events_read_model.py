from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class EventTimelineItem:
    event_id: str
    event_type: str
    ts: str
    title: str
    summary: str
    details: dict[str, Any]


class EventsReadModel:
    def __init__(self, db_path: str | Path):
        self._db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def _parse_ts(value: str) -> float:
        try:
            return datetime.fromisoformat(value).timestamp()
        except Exception:
            return 0.0

    def _degradation_events(self, *, since: str | None = None, limit: int = 200) -> list[EventTimelineItem]:
        with self._connect() as conn:
            if not self._table_exists(conn, "degradation_events"):
                return []
            query = "SELECT state_key, event_type, reason, created_at FROM degradation_events"
            params: list[Any] = []
            if since:
                query += " WHERE created_at >= ?"
                params.append(str(since))
            query += " ORDER BY created_at DESC LIMIT ?"
            params.append(max(1, int(limit)))
            rows = conn.execute(query, tuple(params)).fetchall()

        out: list[EventTimelineItem] = []
        for row in rows:
            raw_type = str(row["event_type"])
            event_type = "degrade_started" if raw_type == "triggered" else "recover_finished"
            state_key = str(row["state_key"])
            ts = str(row["created_at"])
            summary = str(row["reason"]) if row["reason"] else ""
            out.append(
                EventTimelineItem(
                    event_id=f"degradation:{state_key}:{ts}",
                    event_type=event_type,
                    ts=ts,
                    title=state_key,
                    summary=summary,
                    details={"state_key": state_key, "event_type": raw_type, "reason": summary},
                )
            )
        return out

    def _notification_events(self, *, since: str | None = None, limit: int = 400) -> list[EventTimelineItem]:
        with self._connect() as conn:
            required_tables = ("notifications", "watch_events")
            if not all(self._table_exists(conn, name) for name in required_tables):
                return []

            query = """
                SELECT n.notification_id, n.event_id, n.channel, n.state, n.suppressed_reason, n.last_error, n.updated_at,
                       wj.symbol, we.priority, we.strategy_tier
                FROM notifications n
                JOIN watch_events we ON we.event_id = n.event_id
                JOIN watch_jobs wj ON wj.job_id = we.job_id
            """
            params: list[Any] = []
            if since:
                query += " WHERE n.updated_at >= ?"
                params.append(str(since))
            query += " ORDER BY n.updated_at DESC LIMIT ?"
            params.append(max(1, int(limit)))
            rows = conn.execute(query, tuple(params)).fetchall()

        out: list[EventTimelineItem] = []
        for row in rows:
            state = str(row["state"])
            suppressed_reason = str(row["suppressed_reason"]) if row["suppressed_reason"] else ""
            last_error = str(row["last_error"]) if row["last_error"] else ""
            ts = str(row["updated_at"])
            symbol = str(row["symbol"]) if row["symbol"] else ""
            strategy_tier = str(row["strategy_tier"]) if row["strategy_tier"] else "execution-ready"

            if suppressed_reason.startswith("strategy_tier_guard"):
                event_type = "guard_triggered"
                summary = suppressed_reason
            elif state in {"retry_pending", "retrying", "dlq", "failed"} or last_error:
                event_type = "delivery_failed"
                summary = last_error or state
            elif state == "delivered":
                event_type = "delivery_succeeded"
                summary = "delivered"
            else:
                event_type = f"notification_{state}"
                summary = state

            out.append(
                EventTimelineItem(
                    event_id=f"notification:{row['notification_id']}:{ts}",
                    event_type=event_type,
                    ts=ts,
                    title=f"{symbol} via {row['channel']}",
                    summary=summary,
                    details={
                        "event_id": str(row["event_id"]),
                        "symbol": symbol,
                        "channel": str(row["channel"]),
                        "state": state,
                        "priority": str(row["priority"]) if row["priority"] else "medium",
                        "strategy_tier": strategy_tier,
                        "suppressed_reason": suppressed_reason or None,
                        "last_error": last_error or None,
                    },
                )
            )
        return out

    def list_events(self, *, since: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        events = [*self._degradation_events(since=since, limit=limit), *self._notification_events(since=since, limit=limit * 2)]
        events.sort(key=lambda item: self._parse_ts(item.ts), reverse=True)
        sliced = events[: max(1, int(limit))]
        return [
            {
                "event_id": item.event_id,
                "event_type": item.event_type,
                "ts": item.ts,
                "title": item.title,
                "summary": item.summary,
                "details": item.details,
            }
            for item in sliced
        ]
