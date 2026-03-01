#!/usr/bin/env python3
"""Generate Upgrade7 P2-B channel adapter matrix evidence."""

from __future__ import annotations

import asyncio
import json
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from agents.scanner_engine import WatchSignal, build_scan_trigger
from core.models import AlertSignalSnapshot, AlertSnapshot
from services.notification_channels import MultiChannelNotifier
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


@dataclass
class _MemoryTargetSender:
    channel: str
    messages: list[tuple[str, str]] = field(default_factory=list)

    async def send_text(self, target: str, text: str) -> dict[str, Any]:
        self.messages.append((target, text))
        return {"ok": True}

    def drain(self) -> list[tuple[str, str]]:
        out = list(self.messages)
        self.messages.clear()
        return out


class _ChatSender:
    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        return {"ok": True, "chat_id": chat_id, "len": len(text)}


async def _fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
    signal_ts = datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc)
    symbol = str(config.watchlist[0])
    signal = WatchSignal(
        symbol=symbol,
        timestamp=signal_ts,
        price=100.0,
        pct_change=0.05,
        rsi=70.0,
        priority="high",
        reason="price_or_rsi",
        company_name="Evidence Corp",
    )
    snapshot = AlertSnapshot(
        snapshot_id=f"snap-{symbol.lower()}",
        trigger_type="scheduled",
        trigger_id="t-p2b",
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
    return type("RunOut", (), {
        "trigger": build_scan_trigger(trigger_time=signal_ts),
        "signals": [signal],
        "selected_alerts": [signal],
        "snapshots": [snapshot],
        "notifications": [],
        "runtime_metrics": {},
        "failure_events": [],
        "failure_clusters": {},
        "alarms": [],
    })()


async def _build_matrix() -> dict[str, Any]:
    async def _run_strategy(*, strategy: str, symbol: str, index: int) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix=f"upgrade7-p2b-{strategy}-") as temp_dir:
            db_path = Path(temp_dir) / "telegram.db"
            store = TelegramTaskStore(db_path)
            chat_id = "chat-p2b"
            store.upsert_telegram_chat(chat_id=chat_id, user_id="evidence", username="p2b")
            store.upsert_notification_route(chat_id=chat_id, channel="email", target="ops@example.com", enabled=True)
            store.upsert_notification_route(chat_id=chat_id, channel="wecom", target="wecom-bot-1", enabled=True)
            webhook = store.upsert_outbound_webhook(chat_id=chat_id, url="https://example.com/hook", secret="s")

            tg_sender = _MemoryTargetSender(channel="telegram")
            email_sender = _MemoryTargetSender(channel="email")
            wecom_sender = _MemoryTargetSender(channel="wecom")
            webhook_sender = _MemoryTargetSender(channel="webhook")
            notifier = MultiChannelNotifier(
                telegram=tg_sender,
                email=email_sender,
                wecom=wecom_sender,
                webhook=webhook_sender,
            )
            executor = WatchExecutor(
                store=store,
                notifier=_ChatSender(),
                scan_runner=_fake_scan_runner,
                multi_channel_notifier=notifier,
            )

            base_time = datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=index * 5)
            job = store.create_watch_job(
                chat_id=chat_id,
                symbol=symbol,
                interval_sec=300,
                route_strategy=strategy,
                now=base_time,
            )
            due = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)
            if not due:
                return {
                    "strategy": strategy,
                    "job_id": job.job_id,
                    "error": "no_due_job",
                    "dispatch_counts": {"telegram": 0, "email": 0, "wecom": 0, "webhook": 0},
                    "targets": {"telegram": [], "email": [], "wecom": [], "webhook": []},
                    "webhook_id_reference": webhook.webhook_id,
                }
            out = await executor.execute_job(due[0])
            tg_msgs = tg_sender.drain()
            email_msgs = email_sender.drain()
            wecom_msgs = wecom_sender.drain()
            webhook_msgs = webhook_sender.drain()
            return {
                "strategy": strategy,
                "job_id": job.job_id,
                "pushed_count": out.pushed_count,
                "dedupe_suppressed_count": out.dedupe_suppressed_count,
                "dispatch_counts": {
                    "telegram": len(tg_msgs),
                    "email": len(email_msgs),
                    "wecom": len(wecom_msgs),
                    "webhook": len(webhook_msgs),
                },
                "targets": {
                    "telegram": [item[0] for item in tg_msgs],
                    "email": [item[0] for item in email_msgs],
                    "wecom": [item[0] for item in wecom_msgs],
                    "webhook": [item[0] for item in webhook_msgs],
                },
                "webhook_id_reference": webhook.webhook_id,
            }

    strategies: list[tuple[str, str]] = [
        ("telegram_only", "AAPL"),
        ("email_only", "MSFT"),
        ("wecom_only", "TSLA"),
        ("webhook_only", "NVDA"),
        ("dual_channel", "META"),
        ("multi_channel", "AMZN"),
    ]
    rows: list[dict[str, Any]] = []
    for idx, (strategy, symbol) in enumerate(strategies, start=1):
        rows.append(await _run_strategy(strategy=strategy, symbol=symbol, index=idx))

    strategy_map = {
        item["strategy"]: item["dispatch_counts"]
        for item in rows
    }
    return {
        "generated_at": _now(),
        "scope": "Upgrade7 P2-B channel adapter route strategy matrix",
        "strategy_matrix": rows,
        "strategies_covered": sorted(strategy_map.keys()),
    }


async def _main() -> None:
    payload = await _build_matrix()
    output = Path("docs/evidence/upgrade7_p2_channel_adapter_matrix.json")
    _write_json(output, payload)
    print(f"[OK] {output}")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
