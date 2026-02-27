from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

from agents.telegram_command_router import CommandError, parse_telegram_command
from services.runtime_controls import RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_store import TelegramTaskStore


def _mask_chat_id(chat_id: str) -> str:
    if len(chat_id) <= 4:
        return "***"
    return f"{chat_id[:2]}***{chat_id[-2:]}"


class TelegramGateway:
    def __init__(
        self,
        *,
        store: TelegramTaskStore,
        actions: TelegramActions,
        limits: RuntimeLimits | None = None,
        allowed_chat_ids: set[str] | None = None,
        allowed_commands: set[str] | None = None,
        gray_release_enabled: bool = False,
    ):
        self._store = store
        self._actions = actions
        self._limits = limits or RuntimeLimits()
        self._allowed_chat_ids = allowed_chat_ids or set()
        self._allowed_commands = allowed_commands or {"help", "analyze", "monitor", "list", "stop", "report", "digest"}
        self._gray_release_enabled = gray_release_enabled
        self._offset = max(0, self._store.get_latest_update_id() + 1)

    @staticmethod
    def _audit_payload(update: dict[str, Any]) -> dict[str, Any]:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        text = str(message.get("text", "")).strip()
        return {
            "update_id": int(update.get("update_id", 0)),
            "message": {
                "chat": {"id": str(chat.get("id", ""))},
                "text": text[:256],
            },
        }

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
            payload=self._audit_payload(update),
        )
        if not inserted:
            self._store.record_metric(metric_name="duplicate_update_dropped", metric_value=1.0)
            return None
        return update_id

    async def process_update(self, update: dict[str, Any]) -> bool:
        update_id = await self.enqueue_update(update)
        if update_id is None:
            return False
        return await self.process_enqueued_update(update_id=update_id)

    async def process_enqueued_update(self, *, update_id: int) -> bool:
        started_at = time.perf_counter()
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

        print(f"[gateway] handling update={update_id} chat={_mask_chat_id(chat_id)}")
        self._store.record_metric(metric_name="command_total", metric_value=1.0, tags={"chat_id": chat_id})

        try:
            if self._allowed_chat_ids and chat_id not in self._allowed_chat_ids:
                denied = "chat is not allowlisted"
                await self._actions.send_error_message(chat_id=chat_id, text=f"Permission denied: {denied}")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="source_denied",
                    error=denied,
                )
                self._store.add_audit_event(
                    event_type="source_denied",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason=denied,
                )
                return True

            if self._gray_release_enabled and (not self._store.is_chat_allowlisted(chat_id=chat_id)):
                denied = "gray release active: chat not allowlisted"
                await self._actions.send_error_message(chat_id=chat_id, text=f"Permission denied: {denied}")
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="gray_release_denied",
                    error=denied,
                )
                self._store.add_audit_event(
                    event_type="gray_release_denied",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason=denied,
                )
                return True

            self._store.upsert_telegram_chat(chat_id=chat_id, user_id=user_id, username=username)

            within_limit, _ = self._store.check_and_increment_command_rate_limit(
                chat_id=chat_id,
                max_per_minute=self._limits.per_chat_per_minute,
            )
            if not within_limit:
                msg = f"Rate limit exceeded: max {self._limits.per_chat_per_minute} commands/minute."
                await self._actions.send_error_message(chat_id=chat_id, text=msg)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="rate_limited",
                    error=msg,
                )
                self._store.add_audit_event(
                    event_type="rate_limited",
                    chat_id=chat_id,
                    update_id=update_id,
                    action="reject",
                    reason="per_chat_per_minute",
                )
                return True

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

            if parsed.name not in self._allowed_commands:
                error = f"Command not allowlisted: {parsed.name}"
                await self._actions.send_error_message(chat_id=chat_id, text=error)
                self._store.update_bot_update_status(
                    update_id=update_id,
                    status="failed",
                    command="command_denied",
                    error=error,
                )
                self._store.add_audit_event(
                    event_type="command_denied",
                    chat_id=chat_id,
                    update_id=update_id,
                    action=parsed.name,
                    reason="command_not_allowlisted",
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
                if self._store.is_degradation_active(state_key="no_monitor_push"):
                    reason = "monitor push paused due to SLO degradation"
                    await self._actions.send_error_message(chat_id=chat_id, text=reason)
                    self._store.add_audit_event(
                        event_type="degrade_skip",
                        chat_id=chat_id,
                        update_id=update_id,
                        action="monitor",
                        reason=reason,
                    )
                    result = type("ActionResult", (), {"command": "monitor_skipped", "request_id": None})()
                else:
                    result = await self._actions.handle_monitor(
                        chat_id=chat_id,
                        symbol=parsed.args["symbol"],
                        interval_sec=int(parsed.args["interval_sec"]),
                        mode=str(parsed.args.get("mode", "anomaly")),
                        threshold=float(parsed.args.get("threshold", "0.03")),
                        template=str(parsed.args.get("template", "volatility")),
                    )
            elif parsed.name == "list":
                result = await self._actions.handle_list(chat_id=chat_id)
            elif parsed.name == "stop":
                result = await self._actions.handle_stop(
                    chat_id=chat_id,
                    target=parsed.args["target"],
                    target_type=parsed.args["target_type"],
                )
            elif parsed.name == "report":
                result = await self._actions.handle_report(chat_id=chat_id, target_id=parsed.args["target_id"])
            elif parsed.name == "digest":
                result = await self._actions.handle_digest(chat_id=chat_id, period=parsed.args["period"])
            else:
                raise ValueError(f"Unsupported route: {parsed.name}")

            self._store.update_bot_update_status(
                update_id=update_id,
                status="processed",
                command=result.command,
                request_id=result.request_id,
                error=None,
            )
            self._store.record_metric(metric_name="command_success", metric_value=1.0, tags={"command": parsed.name})
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
        finally:
            latency_ms = (time.perf_counter() - started_at) * 1000
            self._store.record_metric(metric_name="command_latency_ms", metric_value=latency_ms)

    async def process_pending_updates(self, *, limit: int = 100) -> int:
        pending_update_ids = self._store.list_pending_bot_update_ids(limit=limit)
        handled = 0
        for update_id in pending_update_ids:
            if await self.process_enqueued_update(update_id=update_id):
                handled += 1
        handled += await self._actions.process_due_analysis_recovery(limit=limit)
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
