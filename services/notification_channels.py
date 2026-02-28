from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol


class TargetedSender(Protocol):
    async def send_text(self, target: str, text: str) -> dict[str, Any]:
        ...


class ChatSender(Protocol):
    async def send_text(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ...


@dataclass
class ChannelDispatchResult:
    delivered: bool
    payload: dict[str, Any] | None = None
    error: str | None = None


class TelegramChannelAdapter:
    """Adapter for Telegram-like sender objects used by gateway/actions."""

    _FORBIDDEN_USER_TOKENS = (
        "traceback",
        "schema_version",
        "action_version",
        "raw_error",
        "_plan_steps",
    )
    _MAX_TEXT_CHARS = 1200

    def __init__(self, sender: ChatSender):
        self._sender = sender

    @classmethod
    def _apply_user_response_contract(cls, text: str) -> str:
        sanitized = str(text or "")
        for token in cls._FORBIDDEN_USER_TOKENS:
            sanitized = re.sub(re.escape(token), "内部细节", sanitized, flags=re.IGNORECASE)
        if len(sanitized) > cls._MAX_TEXT_CHARS:
            sanitized = f"{sanitized[: cls._MAX_TEXT_CHARS - 1]}…"
        return sanitized

    async def send_text(
        self,
        *,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> ChannelDispatchResult:
        try:
            payload = await self._sender.send_text(
                chat_id,
                self._apply_user_response_contract(text),
                reply_markup=reply_markup,
            )
            return ChannelDispatchResult(delivered=True, payload=payload)
        except Exception as exc:  # pragma: no cover
            return ChannelDispatchResult(delivered=False, error=str(exc))

    async def send_photo(
        self,
        *,
        chat_id: str,
        image_path: str,
        caption: str = "",
    ) -> ChannelDispatchResult:
        sender = getattr(self._sender, "send_photo", None)
        if not callable(sender):
            return ChannelDispatchResult(delivered=False, error="send_photo_not_supported")
        try:
            payload = await sender(chat_id, image_path, caption)
            return ChannelDispatchResult(delivered=True, payload=payload)
        except Exception as exc:  # pragma: no cover
            return ChannelDispatchResult(delivered=False, error=str(exc))

    async def send_progress(
        self,
        *,
        chat_id: str,
        text: str,
        message_id: int | None = None,
        reply_markup: dict[str, Any] | None = None,
    ) -> ChannelDispatchResult:
        sanitized = self._apply_user_response_contract(text)
        if message_id is not None:
            editor = getattr(self._sender, "edit_message_text", None)
            if callable(editor):
                try:
                    payload = await editor(chat_id, int(message_id), sanitized, reply_markup=reply_markup)
                    return ChannelDispatchResult(delivered=True, payload=payload)
                except Exception as exc:  # pragma: no cover
                    return ChannelDispatchResult(delivered=False, error=str(exc))
        return await self.send_text(chat_id=chat_id, text=sanitized, reply_markup=reply_markup)

    async def send_chat_action(
        self,
        *,
        chat_id: str,
        action: str = "typing",
    ) -> ChannelDispatchResult:
        sender = getattr(self._sender, "send_chat_action", None)
        if not callable(sender):
            return ChannelDispatchResult(delivered=False, error="send_chat_action_not_supported")
        try:
            payload = await sender(chat_id, action)
            return ChannelDispatchResult(delivered=True, payload=payload)
        except Exception as exc:  # pragma: no cover
            return ChannelDispatchResult(delivered=False, error=str(exc))


@dataclass
class DispatchResult:
    channel: str
    target: str
    delivered: bool
    error: str | None = None


class MultiChannelNotifier:
    def __init__(
        self,
        *,
        telegram: TargetedSender | None = None,
        email: TargetedSender | None = None,
        wecom: TargetedSender | None = None,
        webhook: TargetedSender | None = None,
    ) -> None:
        self._senders: dict[str, TargetedSender] = {}
        if telegram is not None:
            self._senders["telegram"] = telegram
        if email is not None:
            self._senders["email"] = email
        if wecom is not None:
            self._senders["wecom"] = wecom
        if webhook is not None:
            self._senders["webhook"] = webhook

    async def dispatch(self, *, channel: str, target: str, text: str) -> DispatchResult:
        sender = self._senders.get(channel)
        if sender is None:
            return DispatchResult(channel=channel, target=target, delivered=False, error="channel_not_configured")
        payload_text = text
        if channel == "telegram":
            payload_text = TelegramChannelAdapter._apply_user_response_contract(text)
        try:
            await sender.send_text(target, payload_text)
            return DispatchResult(channel=channel, target=target, delivered=True, error=None)
        except Exception as exc:  # pragma: no cover
            return DispatchResult(channel=channel, target=target, delivered=False, error=str(exc))
