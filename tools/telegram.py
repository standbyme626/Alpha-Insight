"""Telegram Bot API helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import aiohttp


class TelegramError(RuntimeError):
    """Raised when Telegram API returns a non-success response."""


@dataclass
class NotificationMessage:
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)


class NotificationChannel(Protocol):
    channel_name: str

    async def send_text(self, text: str) -> dict[str, Any]:
        ...


async def send_text(
    bot_token: str,
    chat_id: str,
    text: str,
    reply_markup: dict[str, Any] | None = None,
) -> dict:
    print("[DEBUG] QuantNode send_text Start")
    base_url = os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org").rstrip("/")
    url = f"{base_url}/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as response:
            data = await response.json(content_type=None)
            if response.status >= 400 or not data.get("ok"):
                raise TelegramError(f"Telegram sendMessage failed: {data}")
            return data


async def send_photo(bot_token: str, chat_id: str, image_path: str, caption: str = "") -> dict:
    print("[DEBUG] QuantNode send_photo Start")
    file_path = Path(image_path)
    if not file_path.exists():
        raise TelegramError(f"Image not found: {image_path}")

    base_url = os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org").rstrip("/")
    url = f"{base_url}/bot{bot_token}/sendPhoto"
    form = aiohttp.FormData()
    form.add_field("chat_id", chat_id)
    if caption:
        form.add_field("caption", caption)
    form.add_field("photo", file_path.read_bytes(), filename=file_path.name)

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form, timeout=60) as response:
            data = await response.json(content_type=None)
            if response.status >= 400 or not data.get("ok"):
                raise TelegramError(f"Telegram sendPhoto failed: {data}")
            return data


async def send_chat_action(bot_token: str, chat_id: str, action: str = "typing") -> dict:
    base_url = os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org").rstrip("/")
    url = f"{base_url}/bot{bot_token}/sendChatAction"
    payload = {"chat_id": chat_id, "action": action}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as response:
            data = await response.json(content_type=None)
            if response.status >= 400 or not data.get("ok"):
                raise TelegramError(f"Telegram sendChatAction failed: {data}")
            return data


class TelegramNotifier:
    channel_name = "telegram"

    def __init__(self, bot_token: str, chat_id: str):
        self._bot_token = bot_token
        self._chat_id = chat_id

    async def send_text(self, text: str) -> dict[str, Any]:
        return await send_text(self._bot_token, self._chat_id, text)

    async def send_chat_action(self, action: str = "typing") -> dict[str, Any]:
        return await send_chat_action(self._bot_token, self._chat_id, action)


async def dispatch_notifications(
    messages: list[NotificationMessage],
    *,
    notifier: NotificationChannel,
) -> list[dict[str, Any]]:
    responses: list[dict[str, Any]] = []
    for message in messages:
        payload = await notifier.send_text(message.text)
        responses.append(
            {
                "channel": notifier.channel_name,
                "metadata": message.metadata,
                "response": payload,
            }
        )
    return responses
