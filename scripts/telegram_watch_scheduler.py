from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from services.scheduler import TelegramWatchScheduler
from services.telegram_store import TelegramTaskStore
from services.watch_executor import WatchExecutor
from tools.telegram import send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram watch scheduler for Alpha-Insight Phase B")
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--poll-interval-seconds", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=20)
    return parser.parse_args()


async def _main() -> int:
    args = parse_args()
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = TelegramTaskStore(Path(args.db_path))
    notifier = TelegramChatSender(bot_token)
    executor = WatchExecutor(store=store, notifier=notifier)
    scheduler = TelegramWatchScheduler(
        store=store,
        executor=executor,
        poll_interval_seconds=args.poll_interval_seconds,
        batch_size=args.batch_size,
    )
    await scheduler.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
