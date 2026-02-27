from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from tools.telegram import send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram long polling gateway for Alpha-Insight Phase A")
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--poll-timeout-seconds", type=int, default=20)
    parser.add_argument("--idle-sleep-seconds", type=float, default=0.5)
    return parser.parse_args()


async def _main() -> int:
    args = parse_args()
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = TelegramTaskStore(Path(args.db_path))
    notifier = TelegramChatSender(bot_token)
    actions = TelegramActions(store=store, notifier=notifier)
    gateway = TelegramGateway(store=store, actions=actions)
    await gateway.run_long_polling(
        bot_token=bot_token,
        poll_timeout_seconds=args.poll_timeout_seconds,
        idle_sleep_seconds=args.idle_sleep_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
