from __future__ import annotations

import asyncio

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from scripts import telegram_webhook_gateway


class _FakeGateway:
    def __init__(self) -> None:
        self.updates: list[dict[str, object]] = []
        self.processed: list[int] = []

    async def enqueue_update(self, payload: dict[str, object]) -> int | None:
        self.updates.append(payload)
        return len(self.updates)

    async def process_enqueued_update(self, update_id: int) -> None:
        self.processed.append(update_id)


def _build_app(fake_gateway: _FakeGateway, *, secret_token: str = "", allowed_ips: set[str] | None = None) -> web.Application:
    app = web.Application()
    app["gateway"] = fake_gateway
    app["secret_token"] = secret_token
    app["allowed_ips"] = allowed_ips or set()
    app["tasks"] = set()
    app.add_routes([web.post("/telegram/webhook", telegram_webhook_gateway._handle_webhook)])
    return app


@pytest.mark.asyncio
async def test_webhook_smoke_accepts_valid_payload_and_dispatches_async_task() -> None:
    gateway = _FakeGateway()
    app = _build_app(gateway)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.post("/telegram/webhook", json={"update_id": 1, "message": {"text": "/help"}})
            payload = await resp.json()
            assert resp.status == 200
            assert payload["ok"] is True
            assert payload["accepted"] is True
            assert payload["update_id"] == 1
            await asyncio.sleep(0)
            assert gateway.processed == [1]


@pytest.mark.asyncio
async def test_webhook_smoke_rejects_invalid_secret_token() -> None:
    gateway = _FakeGateway()
    app = _build_app(gateway, secret_token="expected")
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.post(
                "/telegram/webhook",
                json={"update_id": 1},
                headers={"X-Telegram-Bot-Api-Secret-Token": "wrong"},
            )
            payload = await resp.json()
            assert resp.status == 401
            assert payload["ok"] is False
            assert payload["error"] == "invalid secret token"
