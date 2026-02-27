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
        self._offset = max(0, self._store.get_latest_update_id() + 1)

    async def enqueue_update(self, update: dict[str, Any]) -> int | None:
        update_id = int(update.get("update_id", 0))
        if update_id <= 0:
            return None

        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        inserted = self._store.insert_bot_update_if_new(
            update_id=update_id,
            chat_id=chat_id,
            payload=update,
        )
        if not inserted:
            return None
        return update_id

    async def process_update(self, update: dict[str, Any]) -> bool:
        update_id = await self.enqueue_update(update)
        if update_id is None:
            return False
        return await self.process_enqueued_update(update_id=update_id)

    async def process_enqueued_update(self, *, update_id: int) -> bool:
        payload = self._store.get_bot_update_payload(update_id=update_id)
        if payload is None:
            return False

        message = payload.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        from_user = message.get("from") or {}
        user_id = str(from_user.get("id", "")) if from_user.get("id") is not None else None
        username = str(from_user.get("username", "")).strip() or None
        text = str(message.get("text", "")).strip()

        try:
            self._store.upsert_telegram_chat(chat_id=chat_id, user_id=user_id, username=username)
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
            elif parsed.name == "monitor":
                result = await self._actions.handle_monitor(
                    chat_id=chat_id,
                    symbol=parsed.args["symbol"],
                    interval_sec=int(parsed.args["interval_sec"]),
                )
            elif parsed.name == "list":
                result = await self._actions.handle_list(chat_id=chat_id)
            elif parsed.name == "stop":
                result = await self._actions.handle_stop(
                    chat_id=chat_id,
                    target=parsed.args["target"],
                    target_type=parsed.args["target_type"],
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

    async def process_pending_updates(self, *, limit: int = 100) -> int:
        pending_update_ids = self._store.list_pending_bot_update_ids(limit=limit)
        handled = 0
        for update_id in pending_update_ids:
            if await self.process_enqueued_update(update_id=update_id):
                handled += 1
        return handled

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
                        await self.process_pending_updates(limit=100)
                        await asyncio.sleep(idle_sleep_seconds)
                        continue
                    updates = data.get("result", [])
                    if not isinstance(updates, list):
                        await self.process_pending_updates(limit=100)
                        await asyncio.sleep(idle_sleep_seconds)
                        continue
                    await self.process_updates([item for item in updates if isinstance(item, dict)])
                    await self.process_pending_updates(limit=100)
                await asyncio.sleep(idle_sleep_seconds)
