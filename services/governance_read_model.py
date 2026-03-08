from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services.telegram_store import TelegramTaskStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class GovernanceSnapshot:
    active_states: int
    recovered_states: int
    push_attempt_24h: int
    push_success_24h: int
    push_success_rate_24h: float
    states: list[dict[str, Any]]
    generated_at: str


class GovernanceReadModel:
    def __init__(self, store: TelegramTaskStore):
        self._store = store

    def list_states(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._store.list_degradation_states()
        sliced = rows[: max(1, int(limit))]
        return [
            {
                "state_key": row.state_key,
                "status": row.status,
                "reason": row.reason or "",
                "triggered_at": row.triggered_at,
                "recovered_at": row.recovered_at,
                "updated_at": row.updated_at,
            }
            for row in sliced
        ]

    def build_snapshot(self, *, limit: int = 200) -> GovernanceSnapshot:
        states = self.list_states(limit=limit)
        active = sum(1 for row in states if row.get("status") == "active")
        recovered = sum(1 for row in states if row.get("status") == "recovered")
        push_attempt = self._store.count_metric_events(metric_name="push_attempt")
        push_success = self._store.count_metric_events(metric_name="push_success")
        push_success_rate = (push_success / push_attempt) if push_attempt else 1.0
        return GovernanceSnapshot(
            active_states=active,
            recovered_states=recovered,
            push_attempt_24h=push_attempt,
            push_success_24h=push_success,
            push_success_rate_24h=round(push_success_rate, 4),
            states=states,
            generated_at=_utc_now_iso(),
        )
