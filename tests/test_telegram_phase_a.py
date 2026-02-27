from __future__ import annotations

import pytest

from agents.telegram_command_router import CommandError, parse_telegram_command
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore


class FakeSender:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        self.messages.append((chat_id, text))
        return {"ok": True}


def test_parse_help_command() -> None:
    parsed = parse_telegram_command("/help")
    assert not isinstance(parsed, CommandError)
    assert parsed.name == "help"


def test_parse_analyze_command() -> None:
    parsed = parse_telegram_command("/analyze aapl")
    assert not isinstance(parsed, CommandError)
    assert parsed.name == "analyze"
    assert parsed.args["symbol"] == "AAPL"


def test_parse_invalid_analyze_param() -> None:
    parsed = parse_telegram_command("/analyze ???")
    assert isinstance(parsed, CommandError)
    assert "Invalid symbol" in parsed.message


@pytest.mark.asyncio
async def test_update_id_idempotency_does_not_repeat_execution(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {"run_id": "run-dup-test", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)
    gateway = TelegramGateway(store=store, actions=actions)

    update = {
        "update_id": 1001,
        "message": {
            "chat": {"id": "chat-1"},
            "text": "/analyze AAPL",
        },
    }
    first = await gateway.process_update(update)
    second = await gateway.process_update(update)

    assert first is True
    assert second is False
    assert calls["n"] == 1
    stats = store.verification_counts()
    assert stats["processed_updates"] == 1
    assert stats["distinct_updates"] == 1


@pytest.mark.asyncio
async def test_request_id_idempotency_does_not_rerun_research(tmp_path) -> None:  # noqa: ANN001
    store = TelegramTaskStore(tmp_path / "telegram.db")
    sender = FakeSender()
    calls = {"n": 0}

    async def fake_runner(**kwargs):  # noqa: ANN003
        calls["n"] += 1
        return {"run_id": "run-idem-test", **kwargs}

    actions = TelegramActions(store=store, notifier=sender, research_runner=fake_runner, analysis_timeout_seconds=5)

    await actions.handle_analyze(update_id=2001, chat_id="chat-2", symbol="TSLA", request_id="req-fixed")
    await actions.handle_analyze(update_id=2002, chat_id="chat-2", symbol="TSLA", request_id="req-fixed")

    record = store.get_analysis_request("req-fixed")
    assert record is not None
    assert record.status == "completed"
    assert record.run_id == "run-idem-test"
    assert calls["n"] == 1
    stats = store.verification_counts()
    assert stats["duplicate_running_or_completed"] == 0

