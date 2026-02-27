#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local mock for Telegram Bot API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18081)
    parser.add_argument("--log", default="storage/telegram_api_mock.log")
    return parser.parse_args()


async def _handle_send_message(request: web.Request) -> web.Response:
    payload = await request.json()
    app_log: Path = request.app["log_path"]
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "path": request.path,
        "payload": payload,
    }
    with app_log.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    return web.json_response({"ok": True, "result": {"message_id": 1}})


def main() -> int:
    args = parse_args()
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    app = web.Application()
    app["log_path"] = log_path
    app.add_routes(
        [
            web.post(r"/bot{token}/sendMessage", _handle_send_message),
            web.post(r"/bot{token}/sendPhoto", _handle_send_message),
        ]
    )
    web.run_app(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
