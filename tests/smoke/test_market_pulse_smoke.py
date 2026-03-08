from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.market_pulse import MarketPulsePublisher
from services.telegram_store import TelegramTaskStore


class _FakePulseSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


@pytest.mark.asyncio
async def test_market_pulse_smoke_publish_due_is_idempotent_per_window(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "market_pulse.db")
    sender = _FakePulseSender()
    publisher = MarketPulsePublisher(store=store, sender=sender)
    chat_id = "chat-pulse-smoke"
    now = datetime(2026, 3, 8, 13, 35, 0, tzinfo=timezone.utc)

    store.upsert_telegram_chat(chat_id=chat_id, user_id="u-smoke", username="smoke")
    store.upsert_chat_preferences(chat_id=chat_id, digest_schedule="pulse:1h")
    store.set_degradation_state(state_key="no_monitor_push", status="active", reason="push quality low")
    job = store.create_watch_job(chat_id=chat_id, symbol="TSLA", interval_sec=3600, now=now - timedelta(hours=2))
    store.record_watch_event_if_new(
        job_id=job.job_id,
        symbol="TSLA",
        trigger_ts=now.replace(hour=12, minute=30, second=0, microsecond=0),
        price=210.0,
        pct_change=0.041,
        reason="price_or_rsi",
        rule="price_or_rsi",
        priority="high",
        run_id=None,
    )

    sent = await publisher.publish_due(now=now)
    assert sent == 1
    assert sender.messages
    msg = sender.messages[-1][1]
    assert "市场脉冲（1h）" in msg
    assert "Top movers" in msg
    assert "新闻主题变化" in msg
    assert "风险提示" in msg
    assert "TSLA" in msg

    sent_again = await publisher.publish_due(now=now)
    assert sent_again == 0
