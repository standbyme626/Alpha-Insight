# systemd Deployment Baseline

This folder contains baseline unit files for running Upgrade10 services with systemd.

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

## 3. Install

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
```

## 5. Upgrade and Rollback

- Upgrade:
  1. Deploy code and dependencies.
  2. `sudo systemctl restart alpha-insight-webhook-gateway.service alpha-insight-watch-scheduler.service alpha-insight-resource-api.service`
- Rollback:
  1. Restore previous release.
  2. Restart the same three units.

## 6. Notes

- Unit files run processes in foreground and rely on systemd restart policy.
- Keep gateway and scheduler pointed to the same `TELEGRAM_GATEWAY_DB`.
- For hardened hosts, adjust `ReadWritePaths` if storage/evidence directories differ.
