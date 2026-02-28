from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import aiohttp

from services.notification_channels import MultiChannelNotifier
from services.market_pulse import MarketPulsePublisher
from services.reliability_governor import GovernorConfig, ReliabilityGovernor
from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
from services.scheduler import TelegramWatchScheduler
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor
from tools.telegram import send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text, reply_markup=reply_markup)


class TelegramTargetSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, target: str, text: str) -> dict[str, object]:
        return await send_text(self._bot_token, target, text)


class WebhookTextSender:
    def __init__(self, webhook_url: str):
        self._webhook_url = webhook_url

    async def send_text(self, target: str, text: str) -> dict[str, object]:
        payload = {"target": target, "text": text}
        async with aiohttp.ClientSession() as session:
            async with session.post(self._webhook_url, json=payload, timeout=20) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    raise RuntimeError(f"webhook send failed status={response.status} payload={data}")
                return {"ok": True, "response": data}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram watch scheduler for Alpha-Insight Phase C")
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=20)
    return parser.parse_args()


async def _main() -> int:
    args = parse_args()
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    limits = RuntimeLimits(
        per_chat_per_minute=int(os.getenv("TELEGRAM_PER_CHAT_PER_MINUTE", "20")),
        max_watch_jobs_per_chat=int(os.getenv("TELEGRAM_MAX_WATCH_JOBS_PER_CHAT", "10")),
        global_concurrency=int(os.getenv("TELEGRAM_GLOBAL_CONCURRENCY", "8")),
        notification_max_retry=int(os.getenv("TELEGRAM_NOTIFICATION_MAX_RETRY", "3")),
    )
    gate = GlobalConcurrencyGate(limits.global_concurrency)

    store = TelegramTaskStore(Path(args.db_path))
    notifier = TelegramChatSender(bot_token)
    email_webhook = os.getenv("TELEGRAM_EMAIL_WEBHOOK_URL", "").strip()
    wecom_webhook = os.getenv("TELEGRAM_WECOM_WEBHOOK_URL", "").strip()
    multi_channel = MultiChannelNotifier(
        telegram=TelegramTargetSender(bot_token),
        email=WebhookTextSender(email_webhook) if email_webhook else None,
        wecom=WebhookTextSender(wecom_webhook) if wecom_webhook else None,
    )
    executor = WatchExecutor(
        store=store,
        notifier=notifier,
        limits=limits,
        global_gate=gate,
        multi_channel_notifier=multi_channel,
    )
    governor = ReliabilityGovernor(
        store=store,
        config=GovernorConfig(
            push_success_threshold=float(os.getenv("TELEGRAM_SLO_PUSH_SUCCESS", "0.99")),
            analysis_p95_threshold_ms=float(os.getenv("TELEGRAM_SLO_ANALYSIS_P95_MS", "90000")),
        ),
    )
    pulse_publisher = MarketPulsePublisher(store=store, sender=notifier)
    scheduler = TelegramWatchScheduler(
        store=store,
        executor=executor,
        governor=governor,
        pulse_publisher=pulse_publisher,
        poll_interval_seconds=args.poll_interval_seconds,
        batch_size=args.batch_size,
    )
    await scheduler.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
