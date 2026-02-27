from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from tools.telegram import send_photo, send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text)

    async def send_photo(self, chat_id: str, image_path: str, caption: str = "") -> dict[str, object]:
        return await send_photo(self._bot_token, chat_id, image_path, caption)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram long polling gateway for Alpha-Insight Phase C")
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--poll-timeout-seconds", type=int, default=20)
    parser.add_argument("--idle-sleep-seconds", type=float, default=0.5)
    return parser.parse_args()


def _parse_csv_set(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


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
    actions = TelegramActions(store=store, notifier=notifier, limits=limits, global_gate=gate)
    gateway = TelegramGateway(
        store=store,
        actions=actions,
        limits=limits,
        access_mode=os.getenv("TELEGRAM_ACCESS_MODE", "blacklist"),
        allowed_chat_ids=_parse_csv_set(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "")),
        blocked_chat_ids=_parse_csv_set(os.getenv("TELEGRAM_BLOCKED_CHAT_IDS", "")),
        allowed_commands=_parse_csv_set(
            os.getenv(
                "TELEGRAM_ALLOWED_COMMANDS",
                "help,analyze,monitor,list,stop,report,digest,alerts,bulk,webhook,pref",
            )
        ),
        gray_release_enabled=os.getenv("TELEGRAM_GRAY_RELEASE_ENABLED", "false").strip().lower() in {"1", "true", "yes"},
    )
    await gateway.run_long_polling(
        bot_token=bot_token,
        poll_timeout_seconds=args.poll_timeout_seconds,
        idle_sleep_seconds=args.idle_sleep_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
