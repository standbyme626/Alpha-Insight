"""Telegram Bot API helpers."""

from __future__ import annotations

from pathlib import Path

import aiohttp


class TelegramError(RuntimeError):
    """Raised when Telegram API returns a non-success response."""


async def send_text(bot_token: str, chat_id: str, text: str) -> dict:
    print("[DEBUG] QuantNode send_text Start")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
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

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
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
