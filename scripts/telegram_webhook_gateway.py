from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
from pathlib import Path

from aiohttp import web

from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from tools.telegram import send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram webhook gateway for Alpha-Insight Phase B")
    parser.add_argument("--db-path", default=os.getenv("TELEGRAM_GATEWAY_DB", "storage/telegram_gateway.db"))
    parser.add_argument("--host", default=os.getenv("TELEGRAM_WEBHOOK_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("TELEGRAM_WEBHOOK_PORT", "8081")))
    parser.add_argument("--path", default=os.getenv("TELEGRAM_WEBHOOK_PATH", "/telegram/webhook"))
    parser.add_argument(
        "--pending-poll-seconds",
        type=float,
        default=float(os.getenv("TELEGRAM_PENDING_POLL_SECONDS", "0.5")),
    )
    return parser.parse_args()


async def _pending_worker(gateway: TelegramGateway, interval_seconds: float) -> None:
    while True:
        await gateway.process_pending_updates(limit=100)
        await asyncio.sleep(max(0.1, interval_seconds))


async def _handle_webhook(request: web.Request) -> web.Response:
    gateway: TelegramGateway = request.app["gateway"]
    secret_token = str(request.app.get("secret_token") or "").strip()
    if secret_token:
        provided = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if provided != secret_token:
            return web.json_response({"ok": False, "error": "invalid secret token"}, status=401)

    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if not isinstance(payload, dict):
        return web.json_response({"ok": False, "error": "payload must be object"}, status=400)

    update_id = await gateway.enqueue_update(payload)
    if update_id is None:
        return web.json_response({"ok": True, "accepted": False})

    # Fast ACK after verification + durable insert; processing is async.
    task = asyncio.create_task(gateway.process_enqueued_update(update_id=update_id))
    request.app["tasks"].add(task)
    task.add_done_callback(lambda t: request.app["tasks"].discard(t))
    return web.json_response({"ok": True, "accepted": True, "update_id": update_id})


async def _build_app(args: argparse.Namespace) -> web.Application:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    store = TelegramTaskStore(Path(args.db_path))
    notifier = TelegramChatSender(bot_token)
    actions = TelegramActions(store=store, notifier=notifier)
    gateway = TelegramGateway(store=store, actions=actions)

    app = web.Application()
    app["gateway"] = gateway
    app["secret_token"] = os.getenv("TELEGRAM_WEBHOOK_SECRET_TOKEN", "").strip()
    app["tasks"] = set()

    async def _on_startup(_: web.Application) -> None:
        await gateway.process_pending_updates(limit=100)
        worker = asyncio.create_task(_pending_worker(gateway, args.pending_poll_seconds))
        app["worker"] = worker

    async def _on_cleanup(_: web.Application) -> None:
        worker = app.get("worker")
        if worker is not None:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
        running_tasks = list(app["tasks"])
        for task in running_tasks:
            task.cancel()
        for task in running_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app.add_routes([web.post(args.path, _handle_webhook)])
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


def main() -> int:
    args = parse_args()
    app = asyncio.run(_build_app(args))
    web.run_app(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
