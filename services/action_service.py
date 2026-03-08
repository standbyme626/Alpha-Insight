from __future__ import annotations

from itertools import count

from core.strategy_tier import DEFAULT_STRATEGY_TIER, normalize_strategy_tier
from services.telegram_actions import ActionResult, TelegramActions


class ActionService:
    """Unified write entry for web callers reusing Telegram action semantics."""

    def __init__(self, *, actions: TelegramActions, update_id_seed: int = 1_000_000) -> None:
        self._actions = actions
        self._update_id_counter = count(start=max(1, int(update_id_seed)))

    async def handle_analyze(
        self,
        *,
        chat_id: str,
        symbol: str,
        update_id: int | None = None,
        request_id: str | None = None,
    ) -> ActionResult:
        resolved_update_id = int(update_id) if update_id is not None else next(self._update_id_counter)
        return await self._actions.handle_analyze(
            update_id=resolved_update_id,
            chat_id=chat_id,
            symbol=symbol,
            request_id=request_id,
        )

    async def handle_monitor(
        self,
        *,
        chat_id: str,
        symbol: str,
        symbols: list[str] | None = None,
        interval_sec: int,
        mode: str = "anomaly",
        threshold: float = 0.03,
        template: str = "volatility",
        route_strategy: str = "dual_channel",
        strategy_tier: str = DEFAULT_STRATEGY_TIER,
    ) -> ActionResult:
        return await self._actions.handle_monitor(
            chat_id=chat_id,
            symbol=symbol,
            symbols=symbols,
            interval_sec=interval_sec,
            mode=mode,
            threshold=threshold,
            template=template,
            route_strategy=route_strategy,
            strategy_tier=normalize_strategy_tier(strategy_tier),
        )

    async def handle_route(
        self,
        *,
        chat_id: str,
        action: str,
        channel: str = "",
        target: str = "",
    ) -> ActionResult:
        return await self._actions.handle_route(
            chat_id=chat_id,
            action=action,
            channel=channel,
            target=target,
        )

    async def handle_pref(
        self,
        *,
        chat_id: str,
        setting: str,
        value: str,
    ) -> ActionResult:
        return await self._actions.handle_pref(
            chat_id=chat_id,
            setting=setting,
            value=value,
        )
