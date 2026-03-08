# systemd Operations Runbook (Upgrade12)

This folder contains systemd units and operational procedures for running Alpha-Insight services in long-running production environments.

## 1. Units Included

- `alpha-insight-webhook-gateway.service`
- `alpha-insight-watch-scheduler.service`
- `alpha-insight-resource-api.service`

## 2. Prerequisites

- Linux host with systemd
- Project path: `/home/kkk/Project/Alpha-Insight`
- Python virtualenv at `/home/kkk/Project/Alpha-Insight/.venv`
- Runtime env file:
  - `/etc/alpha-insight/alpha-insight.env`

Create env directory:

```bash
sudo mkdir -p /etc/alpha-insight
sudo cp /home/kkk/Project/Alpha-Insight/.env /etc/alpha-insight/alpha-insight.env
sudo chmod 600 /etc/alpha-insight/alpha-insight.env
```

## 3. Install / Enable

```bash
cd /home/kkk/Project/Alpha-Insight
sudo cp deploy/systemd/alpha-insight-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now alpha-insight-webhook-gateway.service
sudo systemctl enable --now alpha-insight-watch-scheduler.service
sudo systemctl enable --now alpha-insight-resource-api.service
```

## 4. Verify

```bash
systemctl status alpha-insight-webhook-gateway.service --no-pager
systemctl status alpha-insight-watch-scheduler.service --no-pager
systemctl status alpha-insight-resource-api.service --no-pager
journalctl -u alpha-insight-webhook-gateway.service -f
journalctl -u alpha-insight-watch-scheduler.service -f
journalctl -u alpha-insight-resource-api.service -f
```

Health checks:

```bash
curl -sSf http://127.0.0.1:8765/healthz
curl -sSf http://127.0.0.1:8600/runs > /dev/null
```

## 5. Day-2 Operations

### 5.1 Restart Procedures

- Full restart:
  - `sudo systemctl restart alpha-insight-webhook-gateway.service alpha-insight-watch-scheduler.service alpha-insight-resource-api.service`
- Single service restart:
  - `sudo systemctl restart <service-name>`

### 5.2 Log Rotation

- systemd journal retention should be configured via `/etc/systemd/journald.conf`:
  - `SystemMaxUse=2G`
  - `MaxFileSec=7day`
- Apply:
  - `sudo systemctl restart systemd-journald`

### 5.3 Backup and Restore

- Backup:
  - DB file (`TELEGRAM_GATEWAY_DB`, default `storage/telegram_gateway.db`)
  - `.env` runtime file at `/etc/alpha-insight/alpha-insight.env`
  - evidence directory (`docs/evidence/`) if needed for audit
- Restore:
  1. Stop three services.
  2. Restore DB/env/evidence snapshot.
  3. Start three services and run health checks.

### 5.4 Staging vs Production

- Staging:
  - can use reduced watchlist and shorter retention
  - allowlist can include test webhook endpoints
- Production:
  - strict Telegram source IP allowlist
  - strict outbound webhook allowlist
  - retention and archive policy must follow `docs/compliance.md`

## 6. Upgrade and Rollback

- Upgrade:
  1. Deploy code and dependencies.
  2. Restart all three services.
  3. Run health checks and smoke tests.
- Rollback:
  1. Restore previous release and DB snapshot if needed.
  2. Restart all three services.
  3. Validate `/healthz`, `/runs`, and alert flow.

## 7. Notes

- Unit files run processes in foreground and rely on systemd restart policy.
- Keep gateway and scheduler pointed to the same `TELEGRAM_GATEWAY_DB`.
- For hardened hosts, adjust `ReadWritePaths` if storage/evidence directories differ.
