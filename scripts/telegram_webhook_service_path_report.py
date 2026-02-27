#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from agents.scanner_engine import WatchSignal, build_scan_trigger
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.runtime_controls import RuntimeLimits
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


class _NullSender:
    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:  # noqa: ARG002
        return {"ok": True}


def _load_transitions(db_path: Path) -> list[dict[str, Any]]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT event_id, channel, from_state, to_state, reason, created_at
        FROM notification_state_transitions
        ORDER BY id ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


def _load_notification_rows(db_path: Path) -> list[dict[str, Any]]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT notification_id, event_id, channel, state, retry_count, next_retry_at, last_error, delivered_at
        FROM notifications
        ORDER BY created_at ASC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    store = TelegramTaskStore(db_path)
    store.upsert_telegram_chat(chat_id=args.chat_id, user_id="1", username="service-path")
    store.upsert_outbound_webhook(
        chat_id=args.chat_id,
        url=args.failing_webhook_url,
        secret="phase-d-service-path",
    )

    base_time = datetime.now(timezone.utc).replace(microsecond=0)
    job = store.create_watch_job(
        chat_id=args.chat_id,
        symbol=args.symbol,
        scope="single",
        route_strategy="webhook_only",
        interval_sec=300,
        now=base_time,
    )
    due = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)
    if not due:
        raise RuntimeError("no due watch jobs claimed")
    due_job = due[0]

    async def fake_scan_runner(config: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        signal_time = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_time,
            price=100.0,
            pct_change=0.051,
            rsi=71.0,
            priority="critical",
            reason="price_or_rsi",
            company_name=config.watchlist[0],
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-service-path",
            trigger_type="scheduled",
            trigger_id="trigger-service-path",
            trigger_time=signal_time,
            mode="anomaly",
            signal=AlertSignalSnapshot(
                symbol=signal.symbol,
                company_name=signal.company_name,
                timestamp=signal.timestamp,
                price=signal.price,
                pct_change=signal.pct_change,
                rsi=signal.rsi,
                priority=signal.priority,
                reason=signal.reason,
            ),
            notification_channels=[],
            notification_dispatched=False,
            research_status="skipped",
        )
        return type(
            "RunOut",
            (),
            {
                "trigger": build_scan_trigger(trigger_time=signal_time),
                "signals": [signal],
                "selected_alerts": [signal],
                "snapshots": [snapshot],
                "notifications": [],
                "runtime_metrics": {},
                "failure_events": [],
                "failure_clusters": {},
                "alarms": [],
            },
        )()

    executor = WatchExecutor(
        store=store,
        notifier=_NullSender(),
        scan_runner=fake_scan_runner,
        limits=RuntimeLimits(notification_max_retry=args.max_retry),
    )

    first_result = await executor.execute_job(due_job)
    retry_rounds: list[dict[str, Any]] = []

    for idx in range(args.max_retry + 2):
        due_retries = store.claim_due_notification_retries(now=base_time + timedelta(hours=idx + 1), limit=20)
        if not due_retries:
            break
        for item in due_retries:
            payload = store.get_watch_event(event_id=item.event_id)
            if payload is None:
                continue
            await executor._dispatch_notification(  # noqa: SLF001
                payload,
                retry_count=item.retry_count,
                channel_filter=item.channel,
            )
            retry_rounds.append(
                {
                    "round": idx + 1,
                    "event_id": item.event_id,
                    "channel": item.channel,
                    "retry_count_before_dispatch": item.retry_count,
                }
            )

    transitions = _load_transitions(db_path)
    notifications = _load_notification_rows(db_path)
    transition_states = [f"{row.get('from_state') or 'null'}->{row.get('to_state')}" for row in transitions]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "db_path": str(db_path),
        "job_id": job.job_id,
        "symbol": args.symbol,
        "failing_webhook_url": args.failing_webhook_url,
        "first_execution": {
            "job_id": first_result.job_id,
            "triggered_count": first_result.triggered_count,
            "pushed_count": first_result.pushed_count,
            "dedupe_suppressed_count": first_result.dedupe_suppressed_count,
        },
        "retry_rounds": retry_rounds,
        "retry_queue_depth": store.count_retry_queue_depth(),
        "dlq_count": store.count_dlq(),
        "notification_state_transition_total": store.count_notification_state_transitions(),
        "transition_chain": transition_states,
        "notifications": notifications,
        "transitions": transitions,
        "acceptance": {
            "has_retry_pending": any("->retry_pending" in item for item in transition_states),
            "has_retrying": any("retry_pending->retrying" in item for item in transition_states),
            "has_dlq": any("->dlq" in item for item in transition_states),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Telegram webhook service-path acceptance report")
    parser.add_argument("--db-path", default="storage/telegram_gateway_service_path.db")
    parser.add_argument("--output", default="docs/evidence/telegram_webhook_service_path_report.json")
    parser.add_argument("--chat-id", default="service-path-chat")
    parser.add_argument("--symbol", default="TSLA")
    parser.add_argument("--max-retry", type=int, default=3)
    parser.add_argument("--failing-webhook-url", default="http://127.0.0.1:9/hook")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = asyncio.run(_run(args))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
