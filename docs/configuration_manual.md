# Alpha-Insight Configuration Manual

This document is the single reference for runtime configuration used by:

- `scripts/telegram_webhook_gateway.py`
- `scripts/telegram_watch_scheduler.py`
- `services/resource_api.py`
- `web_console/*` resource clients

## 1. Resolution Order

Configuration precedence is:

1. CLI flags (`--host`, `--port`, `--db-path`, etc.)
2. Environment variables
3. In-code defaults

## 2. Telegram Webhook Gateway

Source: `scripts/telegram_webhook_gateway.py`

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `TELEGRAM_BOT_TOKEN` | yes | none | Bot token for Telegram API calls. |
| `TELEGRAM_GATEWAY_DB` | no | `storage/telegram_gateway.db` | SQLite path for gateway/scheduler state. |
| `TELEGRAM_WEBHOOK_HOST` | no | `0.0.0.0` | Bind host. |
| `TELEGRAM_WEBHOOK_PORT` | no | `8081` | Bind port. |
| `TELEGRAM_WEBHOOK_PATH` | no | `/telegram/webhook` | Webhook route path. |
| `TELEGRAM_WEBHOOK_SECRET_TOKEN` | no | empty | If set, validates `X-Telegram-Bot-Api-Secret-Token`. |
| `TELEGRAM_ALLOWED_SOURCE_IPS` | no | empty | CSV allowlist for source IP check. |
| `TELEGRAM_PENDING_POLL_SECONDS` | no | `0.5` | Interval for pending update recovery loop. |
| `TELEGRAM_ACCESS_MODE` | no | auto | `allowlist` or `blacklist` mode. |
| `TELEGRAM_ALLOWED_CHAT_IDS` | no | empty | CSV allowlist chat IDs. |
| `TELEGRAM_CHAT_IDS` | no | empty | Backward-compatible allowlist alias. |
| `TELEGRAM_BLOCKED_CHAT_IDS` | no | empty | CSV denylist chat IDs. |
| `TELEGRAM_ALLOWED_COMMANDS` | no | built-in list | CSV command allowlist. |
| `TELEGRAM_GRAY_RELEASE_ENABLED` | no | `false` | Enables allowlist-gated rollout behavior. |
| `TELEGRAM_PER_CHAT_PER_MINUTE` | no | `20` | Runtime throttling. |
| `TELEGRAM_MAX_WATCH_JOBS_PER_CHAT` | no | `10` | Per-chat monitor cap. |
| `TELEGRAM_GLOBAL_CONCURRENCY` | no | `8` | Global runtime concurrency gate. |
| `TELEGRAM_NOTIFICATION_MAX_RETRY` | no | `3` | Notification retry cap. |
| `TELEGRAM_ANALYSIS_COMMAND_TIMEOUT_SECONDS` | no | `90` | Analyze command timeout. |
| `TELEGRAM_ANALYSIS_SNAPSHOT_TIMEOUT_SECONDS` | no | `90` | Snapshot timeout. |
| `TELEGRAM_ANALYSIS_RECOVERY_TIMEOUT_SECONDS` | no | `180` | Recovery timeout. |
| `TELEGRAM_PHOTO_SEND_TIMEOUT_SECONDS` | no | `20` | Telegram photo send timeout. |
| `TELEGRAM_TYPING_HEARTBEAT_SECONDS` | no | `4` | Typing heartbeat cadence. |
| `TELEGRAM_SINGLEFLIGHT_TTL_SECONDS` | no | `120` | Duplicate in-flight request suppression TTL. |
| `TELEGRAM_SEND_PROGRESS_UPDATES` | no | `true` | Progress message enable flag. |

## 3. Watch Scheduler

Source: `scripts/telegram_watch_scheduler.py`

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `TELEGRAM_BOT_TOKEN` | yes | none | Required for Telegram push. |
| `TELEGRAM_GATEWAY_DB` | no | `storage/telegram_gateway.db` | Must match gateway DB path. |
| `TELEGRAM_EMAIL_WEBHOOK_URL` | no | empty | Optional outbound email adapter endpoint. |
| `TELEGRAM_WECOM_WEBHOOK_URL` | no | empty | Optional outbound WeCom adapter endpoint. |
| `TELEGRAM_PER_CHAT_PER_MINUTE` | no | `20` | Runtime throttling. |
| `TELEGRAM_MAX_WATCH_JOBS_PER_CHAT` | no | `10` | Per-chat monitor cap. |
| `TELEGRAM_GLOBAL_CONCURRENCY` | no | `8` | Global runtime concurrency gate. |
| `TELEGRAM_NOTIFICATION_MAX_RETRY` | no | `3` | Retry cap for failed notifications. |
| `TELEGRAM_SLO_PUSH_SUCCESS` | no | `0.99` | Reliability governor push SLO threshold. |
| `TELEGRAM_SLO_ANALYSIS_P95_MS` | no | `90000` | Reliability governor p95 latency threshold. |

CLI flags:

- `--poll-interval-seconds` (default `1.0`)
- `--batch-size` (default `20`)

## 4. Resource API

Source: `services/resource_api.py`

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `RESOURCE_API_HOST` | no | `0.0.0.0` | Bind host. |
| `RESOURCE_API_PORT` | no | `8765` | Bind port. |
| `TELEGRAM_GATEWAY_DB` | no | `storage/telegram_gateway.db` | Source DB for realtime resources. |
| `RESOURCE_API_EVIDENCE_DIR` | no | `docs/evidence` | Evidence index path. |

## 5. Web Console Resource Routing

Source: `web_console/lib/client.ts`, `web_console/lib/resources.ts`

| Key | Required | Default | Notes |
| --- | --- | --- | --- |
| `UPGRADE7_CONSOLE_BASE_URL` | no | empty | Browser/client base URL for console API routes. |
| `RESOURCE_API_BASE_URL` | no | empty | Preferred direct Resource API base URL. |
| `UPGRADE10_RESOURCE_API_BASE_URL` | no | empty | Upgrade10 compatibility alias. |
| `UPGRADE7_RESOURCE_API_BASE_URL` | no | empty | Legacy compatibility alias. |

## 6. Operational Baseline

- Keep one canonical env file for production (for example `/etc/alpha-insight/alpha-insight.env`).
- Gateway, scheduler, and resource API must point to the same `TELEGRAM_GATEWAY_DB`.
- Validate settings before rollout using:
  - `pytest -q`
  - `python -m py_compile scripts/*.py services/*.py`
