# Alpha-Insight Compliance Boundary

This document records the operational and product compliance boundary for Alpha-Insight Upgrade10.

## 1. Product Boundary

- The Telegram workflow is research and alerting only.
- The system does not place trades and does not provide automated execution.
- User-facing command/help copy must keep this boundary explicit.

## 2. Data Collection and Source Usage

- Market/news ingestion must respect source terms and rate limits.
- Web scraping/RSS connectors must be configured to avoid abusive frequency.
- Collected evidence artifacts are operational records, not redistribution payloads.

Controls in code:

- `core/guardrails.py` sandbox policy checks
- `services/news_digest.py` user-visible payload redaction
- `agents/telegram_nlu_planner.py` prompt-injection risk checks

## 3. Security Controls

- Secrets must be provided via environment variables (not hardcoded).
- Webhook delivery uses per-webhook HMAC signature (`x-alpha-insight-signature`).
- Optional inbound webhook secret and source IP allowlist are supported.

See also:

- `docs/configuration_manual.md`
- `docs/webhook_contract.md`

## 4. Operational Governance

- Use `docs/runbook.md` for incident and recovery SOP.
- Use `deploy/systemd/README.md` for process supervision baseline.
- Reliability signals (`metric_events`, degradation states, retry/dlq depth) are part of runtime governance.

## 5. Logging and Audit Scope

- Audit-relevant tables include:
  - `watch_events`
  - `notifications`
  - `degradation_events`
  - `market_pulse_dispatches`
- Evidence files in `docs/evidence/` are used for traceability and regression review.

## 6. Remaining Verify Items

- Confirm production retention period policy for DB and `docs/evidence/*` artifacts.
- Confirm final production network allowlist for Telegram source IPs and outbound webhook targets.
