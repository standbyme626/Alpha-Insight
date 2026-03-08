from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

from services.action_service import ActionService
from services.telegram_actions import TelegramActions
from services.telegram_store import TelegramTaskStore


class _FakeSender:
    def __init__(self) -> None:
        self._message_id = 0
        self.sent: list[dict[str, Any]] = []

    async def send_text(  # noqa: ANN204
        self,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ):
        self._message_id += 1
        self.sent.append({"chat_id": chat_id, "text": text, "reply_markup": reply_markup})
        return {"ok": True, "result": {"message_id": self._message_id}}


async def _noop_run_analysis_request(**_: Any) -> None:
    return None


def _build_actions(db_path: Path) -> tuple[TelegramTaskStore, TelegramActions]:
    store = TelegramTaskStore(db_path)
    store.upsert_telegram_chat(chat_id="chat-u11", user_id="u11", username="upgrade11")
    sender = _FakeSender()
    actions = TelegramActions(store=store, notifier=sender, analysis_timeout_seconds=5)
    actions._run_analysis_request = _noop_run_analysis_request  # type: ignore[attr-defined]
    return store, actions


def _job_semantics(job: Any) -> dict[str, Any]:
    return {
        "chat_id": job.chat_id,
        "symbol": job.symbol,
        "interval_sec": job.interval_sec,
        "threshold": job.threshold,
        "mode": job.mode,
        "scope": job.scope,
        "route_strategy": job.route_strategy,
        "strategy_tier": job.strategy_tier,
        "enabled": job.enabled,
    }


@pytest.mark.asyncio
async def test_upgrade11_analyze_action_semantics_consistent(tmp_path: Path) -> None:
    tg_store, tg_actions = _build_actions(tmp_path / "tg.db")
    web_store, web_actions = _build_actions(tmp_path / "web.db")
    web_service = ActionService(actions=web_actions)

    tg_result = await tg_actions.handle_analyze(update_id=11, chat_id="chat-u11", symbol="AAPL", request_id="req-u11")
    web_result = await web_service.handle_analyze(update_id=11, chat_id="chat-u11", symbol="AAPL", request_id="req-u11")

    assert tg_result.command == web_result.command == "analyze"
    assert tg_result.request_id == web_result.request_id == "req-u11"

    tg_request = tg_store.get_analysis_request("req-u11")
    web_request = web_store.get_analysis_request("req-u11")
    assert tg_request is not None and web_request is not None
    assert tg_request.update_id == web_request.update_id == 11
    assert tg_request.chat_id == web_request.chat_id == "chat-u11"
    assert tg_request.payload == web_request.payload == {"symbol": "AAPL"}
    assert tg_request.status == web_request.status == "queued"


@pytest.mark.asyncio
async def test_upgrade11_monitor_action_semantics_consistent(tmp_path: Path) -> None:
    tg_store, tg_actions = _build_actions(tmp_path / "tg.db")
    web_store, web_actions = _build_actions(tmp_path / "web.db")
    web_service = ActionService(actions=web_actions)

    await tg_actions.handle_monitor(
        chat_id="chat-u11",
        symbol="AAPL",
        interval_sec=1800,
        mode="anomaly",
        threshold=0.04,
        template="volatility",
        route_strategy="dual_channel",
        strategy_tier="research-only",
    )
    await web_service.handle_monitor(
        chat_id="chat-u11",
        symbol="AAPL",
        interval_sec=1800,
        mode="anomaly",
        threshold=0.04,
        template="volatility",
        route_strategy="dual_channel",
        strategy_tier="research-only",
    )

    tg_jobs = tg_store.list_watch_jobs(chat_id="chat-u11", include_disabled=True)
    web_jobs = web_store.list_watch_jobs(chat_id="chat-u11", include_disabled=True)
    assert len(tg_jobs) == len(web_jobs) == 1
    assert _job_semantics(tg_jobs[0]) == _job_semantics(web_jobs[0])


@pytest.mark.asyncio
async def test_upgrade11_route_action_semantics_consistent(tmp_path: Path) -> None:
    tg_store, tg_actions = _build_actions(tmp_path / "tg.db")
    web_store, web_actions = _build_actions(tmp_path / "web.db")
    web_service = ActionService(actions=web_actions)

    await tg_actions.handle_route(chat_id="chat-u11", action="set", channel="email", target="ops@example.com")
    await web_service.handle_route(chat_id="chat-u11", action="set", channel="email", target="ops@example.com")

    tg_routes = [asdict(item) for item in tg_store.list_notification_routes(chat_id="chat-u11", enabled_only=False)]
    web_routes = [asdict(item) for item in web_store.list_notification_routes(chat_id="chat-u11", enabled_only=False)]
    assert len(tg_routes) == len(web_routes)
    assert {(item["channel"], item["target"], item["enabled"]) for item in tg_routes} == {
        (item["channel"], item["target"], item["enabled"]) for item in web_routes
    }


@pytest.mark.asyncio
async def test_upgrade11_pref_action_semantics_consistent(tmp_path: Path) -> None:
    tg_store, tg_actions = _build_actions(tmp_path / "tg.db")
    web_store, web_actions = _build_actions(tmp_path / "web.db")
    web_service = ActionService(actions=web_actions)

    await tg_actions.handle_pref(chat_id="chat-u11", setting="summary", value="short")
    await web_service.handle_pref(chat_id="chat-u11", setting="summary", value="short")

    tg_pref = tg_store.get_chat_preferences(chat_id="chat-u11")
    web_pref = web_store.get_chat_preferences(chat_id="chat-u11")
    assert tg_pref.summary_mode == web_pref.summary_mode == "short"
    assert tg_pref.min_priority == web_pref.min_priority
    assert tg_pref.quiet_hours == web_pref.quiet_hours
    assert tg_pref.digest_schedule == web_pref.digest_schedule
