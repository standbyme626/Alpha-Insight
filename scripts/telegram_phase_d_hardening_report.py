from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from agents.scanner_engine import WatchSignal, build_scan_trigger
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.notification_channels import MultiChannelNotifier
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


class _ChatSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


class _TargetFailSender:
    async def send_text(self, target: str, text: str) -> dict[str, object]:
        raise RuntimeError(f"forced failure for {target}")


@dataclass
class HardeningEvidence:
    generated_at: str
    cold_start_seconds: float
    cold_start_under_30m: bool
    minimal_loop_ok: bool
    webhook_retry_depth: int
    webhook_dlq_count: int
    webhook_transition_total: int
    webhook_e2e_ok: bool
    notes: list[str]


async def _run_cold_start_minimal_loop(store: TelegramTaskStore) -> tuple[float, bool]:
    sender = _ChatSender()

    async def fake_runner(**kwargs: Any) -> dict[str, Any]:
        return {
            "run_id": "run-cold-start",
            "fused_insights": {"summary": "cold-start-ok"},
            "metrics": {"data_close": 100.0, "technical_rsi_14": 55.0},
            **kwargs,
        }

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    start = time.perf_counter()
    await gateway.process_update(
        {"update_id": 91001, "message": {"chat": {"id": "chat-hard"}, "from": {"id": 1}, "text": "/help"}}
    )
    await gateway.process_update(
        {"update_id": 91002, "message": {"chat": {"id": "chat-hard"}, "from": {"id": 1}, "text": "/monitor TSLA 1h"}}
    )
    elapsed = time.perf_counter() - start
    minimal_ok = any("Monitor created" in text for _, text in sender.messages)
    return elapsed, minimal_ok


async def _run_webhook_dlq_e2e(store: TelegramTaskStore) -> tuple[int, int, int, bool]:
    store.upsert_telegram_chat(chat_id="chat-hard", user_id="1", username="hard")
    store.upsert_outbound_webhook(chat_id="chat-hard", url="https://example.invalid/hook", secret="hard")
    base_time = datetime(2026, 2, 27, 0, 0, tzinfo=timezone.utc)
    job = store.create_watch_job(
        chat_id="chat-hard",
        symbol="AAPL",
        scope="single",
        route_strategy="webhook_only",
        interval_sec=300,
        now=base_time,
    )
    due_job = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)[0]

    async def fake_scan_runner(config: Any, **kwargs: Any) -> Any:
        signal_ts = base_time + timedelta(minutes=10)
        signal = WatchSignal(
            symbol=config.watchlist[0],
            timestamp=signal_ts,
            price=100.0,
            pct_change=0.05,
            rsi=71.0,
            priority="critical",
            reason="price_or_rsi",
            company_name="Apple",
        )
        snapshot = AlertSnapshot(
            snapshot_id="snap-hard",
            trigger_type="scheduled",
            trigger_id="t-hard",
            trigger_time=signal_ts,
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
                "trigger": build_scan_trigger(trigger_time=signal_ts),
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

    multi = MultiChannelNotifier(webhook=_TargetFailSender())
    executor = WatchExecutor(
        store=store,
        notifier=_ChatSender(),
        scan_runner=fake_scan_runner,
        limits=RuntimeLimits(notification_max_retry=3),
        multi_channel_notifier=multi,
    )

    await executor.execute_job(due_job)
    for _ in range(5):
        due = store.claim_due_notification_retries(now=base_time + timedelta(days=1), limit=20)
        if not due:
            break
        for item in due:
            payload = store.get_watch_event(event_id=item.event_id)
            if payload is None:
                continue
            await executor._dispatch_notification(payload, retry_count=item.retry_count, channel_filter=item.channel)  # noqa: SLF001

    retry_depth = store.count_retry_queue_depth()
    dlq_count = store.count_dlq()
    transitions = store.count_notification_state_transitions()
    return retry_depth, dlq_count, transitions, (dlq_count >= 1 and retry_depth == 0)


async def _build_evidence() -> HardeningEvidence:
    with TemporaryDirectory(prefix="tg-phase-d-hardening-") as tmp:
        db_path = Path(tmp) / "telegram.db"
        store = TelegramTaskStore(db_path)

        cold_start_seconds, minimal_loop_ok = await _run_cold_start_minimal_loop(store)
        retry_depth, dlq_count, transitions, webhook_ok = await _run_webhook_dlq_e2e(store)
        notes = [
            "cold-start uses in-process gateway/actions/store minimal loop",
            "webhook e2e uses forced failing webhook sender to verify retry->dlq transitions",
        ]
        return HardeningEvidence(
            generated_at=datetime.now(timezone.utc).isoformat(),
            cold_start_seconds=round(cold_start_seconds, 4),
            cold_start_under_30m=cold_start_seconds <= 1800,
            minimal_loop_ok=minimal_loop_ok,
            webhook_retry_depth=retry_depth,
            webhook_dlq_count=dlq_count,
            webhook_transition_total=transitions,
            webhook_e2e_ok=webhook_ok,
            notes=notes,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Phase-D hardening evidence")
    parser.add_argument(
        "--output",
        default="docs/evidence/telegram_phase_d_hardening_report.json",
        help="output JSON path",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    evidence = asyncio.run(_build_evidence())
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": evidence.generated_at,
        "cold_start_seconds": evidence.cold_start_seconds,
        "cold_start_under_30m": evidence.cold_start_under_30m,
        "minimal_loop_ok": evidence.minimal_loop_ok,
        "webhook_retry_depth": evidence.webhook_retry_depth,
        "webhook_dlq_count": evidence.webhook_dlq_count,
        "webhook_transition_total": evidence.webhook_transition_total,
        "webhook_e2e_ok": evidence.webhook_e2e_ok,
        "notes": evidence.notes,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
