# Alpha-Insight Webhook Contract

This document defines inbound and outbound webhook behavior used in the Upgrade10 runtime.

## 1. Inbound Telegram Webhook

Source: `scripts/telegram_webhook_gateway.py`

- Method: `POST`
- Path: configurable by `TELEGRAM_WEBHOOK_PATH` (default `/telegram/webhook`)
- Content-Type: `application/json`
- Body: Telegram update object (must be a JSON object)

### 1.1 Optional Security Checks

- Secret token:
  - Config: `TELEGRAM_WEBHOOK_SECRET_TOKEN`
  - Header required when configured: `X-Telegram-Bot-Api-Secret-Token`
  - Mismatch response: `401 {"ok": false, "error": "invalid secret token"}`
- Source IP allowlist:
  - Config: `TELEGRAM_ALLOWED_SOURCE_IPS` (CSV)
  - Source from `X-Forwarded-For` first hop, fallback to peer address
  - Denied response: `403 {"ok": false, "error": "source ip denied"}`

### 1.2 Response Contract

- Invalid JSON: `400 {"ok": false, "error": "invalid json"}`
- Non-object JSON: `400 {"ok": false, "error": "payload must be object"}`
- Duplicate/ignored update: `200 {"ok": true, "accepted": false}`
- Accepted update: `200 {"ok": true, "accepted": true, "update_id": <int>}`

Processing is async after durable enqueue.

## 2. Outbound Alert Webhook (Multi-channel Route)

Source: `services/watch_executor.py` (`_build_webhook_payload`, `_WebhookTargetedSender`)

Delivery target is resolved from `outbound_webhooks` table (`webhook_id` selected by routing strategy).

### 2.1 Request

- Method: `POST`
- URL: `outbound_webhooks.url`
- Header:
  - `content-type: application/json`
  - `x-alpha-insight-signature: <hex hmac sha256>`
- Signature algorithm:
  - `HMAC_SHA256(secret=outbound_webhooks.secret, message=<raw request body>)`
- Timeout:
  - `max(0.5s, outbound_webhooks.timeout_ms / 1000)`

### 2.2 Body Schema

Raw JSON object:

```json
{
  "event_id": "evt-xxx",
  "dedupe_key": "optional-string",
  "run_id": "optional-string",
  "priority": "medium|high|critical|...",
  "strategy_tier": "execution-ready|research-only|alert-only",
  "symbol": "AAPL",
  "job_id": "job-xxx",
  "trigger_ts": "ISO-8601",
  "metrics": {
    "price": 123.45,
    "pct_change": 0.032,
    "reason": "price_or_rsi",
    "rule": "price_or_rsi"
  }
}
```

### 2.3 Receiver Expectations

- Any HTTP status `>= 400` is treated as failure and pushed into retry/error flow.
- Receiver should:
  - verify `x-alpha-insight-signature`
  - respond with `2xx` for accepted messages
  - return compact error body for diagnostics when rejecting

## 3. Scheduler External Adapter Webhook

Source: `scripts/telegram_watch_scheduler.py` (`WebhookTextSender`)

Used when `TELEGRAM_EMAIL_WEBHOOK_URL` or `TELEGRAM_WECOM_WEBHOOK_URL` is configured.

- Method: `POST`
- Body:

```json
{
  "target": "<route target>",
  "text": "<message>"
}
```

- Timeout: 20 seconds
- On HTTP `>= 400`, scheduler raises runtime error with status and response payload snippet.

## 4. Versioning and Compatibility

- Current contract baseline: Upgrade10 T8
- Non-breaking changes:
  - adding optional fields
  - adding new event priorities
- Breaking changes require:
  - contract doc update
  - regression test update in `tests/smoke/test_webhook_smoke.py`
