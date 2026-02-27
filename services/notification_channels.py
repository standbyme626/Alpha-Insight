from __future__ import annotations

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


@dataclass
class ChannelDispatchResult:
    delivered: bool
    payload: dict[str, Any] | None = None
    error: str | None = None


class TelegramChannelAdapter:
    """Adapter for Telegram-like sender objects used by gateway/actions."""

    def __init__(self, sender: ChatSender):
        self._sender = sender

    async def send_text(
        self,
        *,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> ChannelDispatchResult:
        try:
            payload = await self._sender.send_text(chat_id, text, reply_markup=reply_markup)
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
        reply_markup: dict[str, Any] | None = None,
    ) -> ChannelDispatchResult:
        return await self.send_text(chat_id=chat_id, text=text, reply_markup=reply_markup)

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
        try:
            await sender.send_text(target, text)
            return DispatchResult(channel=channel, target=target, delivered=True, error=None)
        except Exception as exc:  # pragma: no cover
            return DispatchResult(channel=channel, target=target, delivered=False, error=str(exc))
