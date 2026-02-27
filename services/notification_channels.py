from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class TargetedSender(Protocol):
    async def send_text(self, target: str, text: str) -> dict[str, Any]:
        ...


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
