from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from services.market_pulse import MarketPulsePublisher
from services.telegram_store import TelegramTaskStore


class FakePulseSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


@pytest.mark.asyncio
async def test_market_pulse_publish_due_sends_once_per_window_and_contains_sections(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakePulseSender()
    publisher = MarketPulsePublisher(store=store, sender=sender)
    chat_id = "chat-pulse-1"
    now = datetime.now(timezone.utc).replace(minute=35, second=0, microsecond=0)

    store.upsert_telegram_chat(chat_id=chat_id, user_id="u-pulse", username="pulse")
    store.upsert_chat_preferences(chat_id=chat_id, digest_schedule="pulse:1h")
    job = store.create_watch_job(chat_id=chat_id, symbol="TSLA", interval_sec=3600, now=now - timedelta(hours=2))
    store.record_watch_event_if_new(
        job_id=job.job_id,
        symbol="TSLA",
        trigger_ts=now.replace(minute=30, second=0, microsecond=0) - timedelta(hours=1),
        price=200.0,
        pct_change=0.045,
        reason="price_or_rsi",
        rule="price_or_rsi",
        priority="high",
        run_id=None,
    )
    store.upsert_analysis_report(
        run_id="run-pulse-cur",
        request_id="req-pulse-cur",
        chat_id=chat_id,
        symbol="TSLA",
        summary="pulse cur",
        key_metrics={
            "news_digest": {
                "top_themes": [
                    {"category": "财报", "count": 2},
                    {"category": "监管", "count": 1},
                ]
            }
        },
    )
    store.upsert_analysis_report(
        run_id="run-pulse-prev",
        request_id="req-pulse-prev",
        chat_id=chat_id,
        symbol="TSLA",
        summary="pulse prev",
        key_metrics={
            "news_digest": {
                "top_themes": [
                    {"category": "财报", "count": 1},
                ]
            }
        },
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
    assert "/pref pulse off" in msg

    sent_again = await publisher.publish_due(now=now)
    assert sent_again == 0
