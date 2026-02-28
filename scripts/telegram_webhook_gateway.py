from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
from pathlib import Path

from aiohttp import web

from services.runtime_controls import GlobalConcurrencyGate, RuntimeLimits
from services.telegram_actions import TelegramActions
from services.telegram_gateway import TelegramGateway
from services.telegram_store import TelegramTaskStore
from tools.telegram import edit_message_text, send_chat_action, send_photo, send_text


class TelegramChatSender:
    def __init__(self, bot_token: str):
        self._bot_token = bot_token

    async def send_text(self, chat_id: str, text: str, reply_markup: dict[str, object] | None = None) -> dict[str, object]:
        return await send_text(self._bot_token, chat_id, text, reply_markup=reply_markup)

    async def send_photo(self, chat_id: str, image_path: str, caption: str = "") -> dict[str, object]:
        return await send_photo(self._bot_token, chat_id, image_path, caption)

    async def send_chat_action(self, chat_id: str, action: str = "typing") -> dict[str, object]:
        return await send_chat_action(self._bot_token, chat_id, action)

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        reply_markup: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return await edit_message_text(
            self._bot_token,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram webhook gateway for Alpha-Insight Phase C")
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


def _parse_csv_set(raw: str) -> set[str]:
    return {item.strip() for item in raw.split(",") if item.strip()}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _resolve_client_ip(request: web.Request) -> str:
    fwd = str(request.headers.get("X-Forwarded-For", "")).strip()
    if fwd:
        return fwd.split(",")[0].strip()
    peer = request.transport.get_extra_info("peername") if request.transport else None
    if not peer:
        return ""
    return str(peer[0])


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

    allowed_ips: set[str] = request.app.get("allowed_ips", set())
    if allowed_ips:
        source_ip = _resolve_client_ip(request)
        if source_ip not in allowed_ips:
            return web.json_response({"ok": False, "error": "source ip denied"}, status=403)

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

    limits = RuntimeLimits(
        per_chat_per_minute=int(os.getenv("TELEGRAM_PER_CHAT_PER_MINUTE", "20")),
        max_watch_jobs_per_chat=int(os.getenv("TELEGRAM_MAX_WATCH_JOBS_PER_CHAT", "10")),
        global_concurrency=int(os.getenv("TELEGRAM_GLOBAL_CONCURRENCY", "8")),
        notification_max_retry=int(os.getenv("TELEGRAM_NOTIFICATION_MAX_RETRY", "3")),
        analysis_command_timeout_seconds=float(os.getenv("TELEGRAM_ANALYSIS_COMMAND_TIMEOUT_SECONDS", "90")),
        analysis_snapshot_timeout_seconds=float(os.getenv("TELEGRAM_ANALYSIS_SNAPSHOT_TIMEOUT_SECONDS", "90")),
        analysis_recovery_timeout_seconds=float(os.getenv("TELEGRAM_ANALYSIS_RECOVERY_TIMEOUT_SECONDS", "180")),
        photo_send_timeout_seconds=float(os.getenv("TELEGRAM_PHOTO_SEND_TIMEOUT_SECONDS", "20")),
        typing_heartbeat_seconds=float(os.getenv("TELEGRAM_TYPING_HEARTBEAT_SECONDS", "4")),
        session_singleflight_ttl_seconds=int(os.getenv("TELEGRAM_SINGLEFLIGHT_TTL_SECONDS", "120")),
        send_progress_updates=_env_bool("TELEGRAM_SEND_PROGRESS_UPDATES", True),
    )
    gate = GlobalConcurrencyGate(limits.global_concurrency)

    store = TelegramTaskStore(Path(args.db_path))
    notifier = TelegramChatSender(bot_token)
    actions = TelegramActions(store=store, notifier=notifier, limits=limits, global_gate=gate)
    allowed_chat_ids_raw = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").strip() or os.getenv("TELEGRAM_CHAT_IDS", "").strip()
    access_mode = os.getenv("TELEGRAM_ACCESS_MODE", "").strip().lower() or ("allowlist" if allowed_chat_ids_raw else "blacklist")
    gateway = TelegramGateway(
        store=store,
        actions=actions,
        limits=limits,
        access_mode=access_mode,
        allowed_chat_ids=_parse_csv_set(allowed_chat_ids_raw),
        blocked_chat_ids=_parse_csv_set(os.getenv("TELEGRAM_BLOCKED_CHAT_IDS", "")),
        allowed_commands=_parse_csv_set(
            os.getenv(
                "TELEGRAM_ALLOWED_COMMANDS",
                "help,analyze,monitor,list,stop,report,digest,alerts,bulk,webhook,route,pref",
            )
        ),
        gray_release_enabled=os.getenv("TELEGRAM_GRAY_RELEASE_ENABLED", "false").strip().lower() in {"1", "true", "yes"},
    )

    app = web.Application()
    app["gateway"] = gateway
    app["secret_token"] = os.getenv("TELEGRAM_WEBHOOK_SECRET_TOKEN", "").strip()
    app["allowed_ips"] = _parse_csv_set(os.getenv("TELEGRAM_ALLOWED_SOURCE_IPS", ""))
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
