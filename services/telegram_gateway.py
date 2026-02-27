from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

from agents.telegram_command_router import CommandError, parse_telegram_command
from services.telegram_actions import TelegramActions
from services.telegram_store import TelegramTaskStore


class TelegramGateway:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        actions: TelegramActions,
    ):
        self._store = store
        self._actions = actions
        self._offset = 0

    async def process_update(self, update: dict[str, Any]) -> bool:
        update_id = int(update.get("update_id", 0))
        if update_id <= 0:
            return False

        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        text = str(message.get("text", "")).strip()

        inserted = self._store.insert_bot_update_if_new(
            update_id=update_id,
            chat_id=chat_id,
            payload=update,
        )
        if not inserted:
            return False

        try:
            parsed = parse_telegram_command(text)
            if isinstance(parsed, CommandError):
                await self._actions.send_error_message(chat_id=chat_id, text=parsed.message)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="invalid_command",
                    error=parsed.message,
                )
                return True

            if parsed.name == "help":
                result = await self._actions.handle_help(chat_id=chat_id)
            elif parsed.name == "analyze":
                result = await self._actions.handle_analyze(
                    update_id=update_id,
                    chat_id=chat_id,
                    symbol=parsed.args["symbol"],
                )
            else:
                raise ValueError(f"Unsupported route: {parsed.name}")

            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command=result.command,
                request_id=result.request_id,
                error=None,
            )
            return True
        except Exception as exc:
            self._store.update_bot_update_status(
                update_id=update_id,
                status="failed",
                command=None,
                request_id=None,
                error=str(exc),
            )
            raise

    async def process_updates(self, updates: list[dict[str, Any]]) -> int:
        handled = 0
        for update in updates:
            if await self.process_update(update):
                handled += 1
            update_id = int(update.get("update_id", 0))
            if update_id > 0:
                self._offset = max(self._offset, update_id + 1)
        return handled

    async def run_long_polling(
        self,
        *,
        bot_token: str,
        poll_timeout_seconds: int = 20,
        idle_sleep_seconds: float = 0.5,
    ) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
        async with aiohttp.ClientSession() as session:
            while True:
                payload = {
                    "timeout": poll_timeout_seconds,
                    "offset": self._offset,
                    "allowed_updates": ["message"],
                }
                async with session.post(url, json=payload, timeout=poll_timeout_seconds + 5) as response:
                    data = await response.json(content_type=None)
                    if response.status >= 400 or not data.get("ok"):
                        await asyncio.sleep(idle_sleep_seconds)
                        continue
                    updates = data.get("result", [])
                    if not isinstance(updates, list):
                        await asyncio.sleep(idle_sleep_seconds)
                        continue
                    await self.process_updates([item for item in updates if isinstance(item, dict)])
                await asyncio.sleep(idle_sleep_seconds)
