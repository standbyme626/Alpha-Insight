from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DEFAULT_DB_CANDIDATES: tuple[Path, ...] = (
    Path("storage/telegram_gateway_live.db"),
    Path("storage/telegram_gateway.db"),
    Path("storage/telegram_gateway_human_sim.db"),
    Path("storage/telegram_gateway_service_path.db"),
    Path("storage/telegram_gateway_e2e.db"),
)


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    if db_path is not None:
        return Path(db_path)
    for key in ("TELEGRAM_GATEWAY_DB", "ALPHA_INSIGHT_STORE_DB"):
        value = os.getenv(key, "").strip()
        if value:
            return Path(value)
    for candidate in DEFAULT_DB_CANDIDATES:
        if candidate.exists():
            return candidate
    return DEFAULT_DB_CANDIDATES[0]


class SQLiteStoreAdapter:
    def __init__(self, db_path: str | Path | None = None):
        self._db_path = resolve_db_path(db_path)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def exists(self) -> bool:
        return self._db_path.exists()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None

    @staticmethod
    def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row["name"]) for row in rows}
