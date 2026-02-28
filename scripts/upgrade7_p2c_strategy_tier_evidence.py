#!/usr/bin/env python3
"""Generate Upgrade7 P2-C strategy tier matrix evidence."""

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
from core.strategy_tier import ALLOWED_STRATEGY_TIERS
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
        output = list(self.messages)
        self.messages.clear()
        return output


class _ChatSender:
    async def send_text(self, chat_id: str, text: str) -> dict[str, Any]:
        return {"ok": True, "chat_id": chat_id, "text_len": len(text)}


async def _run_tier_case(*, tier: str, index: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix=f"upgrade7-p2c-{tier}-") as temp_dir:
        db_path = Path(temp_dir) / "telegram.db"
        store = TelegramTaskStore(db_path)
        chat_id = "chat-p2c"
        store.upsert_telegram_chat(chat_id=chat_id, user_id="evidence", username="p2c")
        store.upsert_notification_route(chat_id=chat_id, channel="email", target="ops@example.com", enabled=True)
        store.upsert_notification_route(chat_id=chat_id, channel="wecom", target="wecom-bot-1", enabled=True)
        webhook = store.upsert_outbound_webhook(chat_id=chat_id, url="https://example.com/hook", secret="s")

        telegram_sender = _MemoryTargetSender(channel="telegram")
        email_sender = _MemoryTargetSender(channel="email")
        wecom_sender = _MemoryTargetSender(channel="wecom")
        webhook_sender = _MemoryTargetSender(channel="webhook")
        notifier = MultiChannelNotifier(
            telegram=telegram_sender,
            email=email_sender,
            wecom=wecom_sender,
            webhook=webhook_sender,
        )

        runtime_metrics_seen: dict[str, Any] = {}
        scan_kwargs_seen: dict[str, Any] = {}

        async def fake_scan_runner(config, **kwargs):  # noqa: ANN001, ANN003
            scan_kwargs_seen.update(kwargs)
            signal_ts = datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=index)
            symbol = str(config.watchlist[0])
            signal = WatchSignal(
                symbol=symbol,
                timestamp=signal_ts,
                price=100.0 + index,
                pct_change=0.08,
                rsi=75.0,
                priority="critical",
                reason="price_move",
                company_name="Tier Evidence Corp",
            )
            snapshot = AlertSnapshot(
                snapshot_id=f"snap-{tier}-{symbol.lower()}",
                trigger_type="scheduled",
                trigger_id=f"t-p2c-{tier}",
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
                research_status="triggered" if bool(kwargs.get("enable_triggered_research", False)) else "skipped",
                research_run_id=f"run-{tier}" if bool(kwargs.get("enable_triggered_research", False)) else None,
            )
            runtime_metrics = {
                "seed_runtime_metric": True,
                "seed_strategy_tier": kwargs.get("strategy_tier", ""),
                "seed_enable_triggered_research": bool(kwargs.get("enable_triggered_research", False)),
            }
            runtime_metrics_seen.clear()
            runtime_metrics_seen.update(runtime_metrics)
            return type("RunOut", (), {
                "trigger": build_scan_trigger(trigger_time=signal_ts),
                "signals": [signal],
                "selected_alerts": [signal],
                "snapshots": [snapshot],
                "notifications": [],
                "runtime_metrics": runtime_metrics,
                "failure_events": [],
                "failure_clusters": {},
                "alarms": [],
            })()

        executor = WatchExecutor(
            store=store,
            notifier=_ChatSender(),
            scan_runner=fake_scan_runner,
            multi_channel_notifier=notifier,
        )

        base_time = datetime(2026, 2, 28, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=index * 5)
        job = store.create_watch_job(
            chat_id=chat_id,
            symbol="AAPL",
            interval_sec=300,
            route_strategy="multi_channel",
            strategy_tier=tier,
            now=base_time,
        )
        due = store.claim_due_watch_jobs(now=base_time + timedelta(minutes=10), limit=1)
        if not due:
            return {
                "strategy_tier": tier,
                "error": "no_due_job",
                "job_id": job.job_id,
            }

        out = await executor.execute_job(due[0])
        tg_msgs = telegram_sender.drain()
        email_msgs = email_sender.drain()
        wecom_msgs = wecom_sender.drain()
        webhook_msgs = webhook_sender.drain()

        with store._connect() as conn:  # noqa: SLF001
            state_rows = conn.execute(
                """
                SELECT n.state, n.suppressed_reason, COUNT(*) AS c
                FROM notifications n
                GROUP BY n.state, n.suppressed_reason
                ORDER BY c DESC, n.state ASC
                """
            ).fetchall()
            event_row = conn.execute(
                """
                SELECT strategy_tier
                FROM watch_events
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchone()
        return {
            "strategy_tier": tier,
            "job_id": job.job_id,
            "route_strategy": job.route_strategy,
            "watch_event_strategy_tier": str(event_row["strategy_tier"]) if event_row is not None else "",
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
                "webhook_id_reference": webhook.webhook_id,
            },
            "notification_state_matrix": [
                {
                    "state": str(row["state"]),
                    "suppressed_reason": str(row["suppressed_reason"]) if row["suppressed_reason"] else "",
                    "count": int(row["c"]),
                }
                for row in state_rows
            ],
            "audit_counts": {
                "strategy_tier_decision": store.count_audit_events(event_type="strategy_tier_decision"),
                "strategy_tier_guarded": store.count_audit_events(event_type="strategy_tier_guarded"),
                "degrade_skip": store.count_audit_events(event_type="degrade_skip"),
            },
            "runtime_metrics": dict(runtime_metrics_seen),
            "scan_kwargs_seen": {
                "enable_triggered_research": bool(scan_kwargs_seen.get("enable_triggered_research", False)),
                "strategy_tier": str(scan_kwargs_seen.get("strategy_tier", "")),
            },
        }


async def _build_matrix() -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for idx, tier in enumerate(ALLOWED_STRATEGY_TIERS, start=1):
        rows.append(await _run_tier_case(tier=tier, index=idx))
    tier_distribution = {item["strategy_tier"]: 1 for item in rows if "strategy_tier" in item}
    guarded_tier_distribution = {
        item["strategy_tier"]: int(item.get("audit_counts", {}).get("strategy_tier_guarded", 0))
        for item in rows
        if "strategy_tier" in item
    }
    return {
        "generated_at": _now(),
        "scope": "Upgrade7 P2-C strategy tier matrix",
        "strategy_tiers_covered": [item["strategy_tier"] for item in rows if "strategy_tier" in item],
        "tier_matrix": rows,
        "tier_distribution": tier_distribution,
        "guarded_tier_distribution": guarded_tier_distribution,
    }


async def _main() -> None:
    payload = await _build_matrix()
    output = Path("docs/evidence/upgrade7_p2_strategy_tier_matrix.json")
    _write_json(output, payload)
    print(f"[OK] {output}")


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
