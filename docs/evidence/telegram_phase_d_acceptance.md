# Telegram Phase D Acceptance Evidence

Date: 2026-02-27

## Scope
- D1 命令体验升级：`/report`、`/digest daily`、`/help` 合规提示、`/monitor` 阈值模板。
- D2 通道抽象：Telegram/Email/企业微信 多通道路由基础能力。
- D3 运维交付：容器化模板、灰度开关、Runbook。
- D4 Alert Hub：支持 `triggered/failed/suppressed` 视图与 watch_jobs 批量操作（启停/改频率/改阈值）。
- D5 Watchlist + Webhook：支持 group 级规则与 `telegram_only|webhook_only|dual_channel` 路由策略。
- D6 移动优先策略：短摘要默认、quiet hours、用户级通知优先级偏好。

## Test Evidence
- Command: `pytest -q`
- Result: `79 passed in 16.43s`
- 阶段 D 测试文件：`tests/test_telegram_phase_d.py`（11 passed，含 D4~D6）

## Run Report
- JSON: `docs/evidence/telegram_phase_d_run_report.json`
- 生成命令:
  - `PYTHONPATH=/home/kkk/Project/Alpha-Insight python scripts/telegram_phase_d_report.py --db-path storage/telegram_gateway.db --output docs/evidence/telegram_phase_d_run_report.json`
- 本轮真实服务路径 run_report:
  - `docs/evidence/telegram_phase_d_run_report_e2e.json`
  - 生成命令:
    - `docker compose --env-file /tmp/alpha_telegram_e2e.env -f docker-compose.telegram.yml -f /tmp/docker-compose.telegram.e2e.override.yml exec -T telegram-gateway python scripts/telegram_phase_d_report.py --db-path /workspace/storage/telegram_gateway_e2e.db --output /workspace/docs/evidence/telegram_phase_d_run_report_e2e.json`

## Hardening Evidence (D 验收缺口补齐)
- JSON: `docs/evidence/telegram_phase_d_hardening_report.json`
- 生成命令:
  - `PYTHONPATH=/home/kkk/Project/Alpha-Insight python scripts/telegram_phase_d_hardening_report.py --output docs/evidence/telegram_phase_d_hardening_report.json`
- 结果摘要（2026-02-27）:
  - `cold_start_seconds=0.0613`，`cold_start_under_30m=true`，`minimal_loop_ok=true`
  - `webhook_retry_depth=0`，`webhook_dlq_count=1`，`webhook_transition_total=5`，`webhook_e2e_ok=true`

## Real Service-Path Acceptance (A/B)
- A. 30 分钟冷启动 E2E（docker-compose + webhook + worker + db）:
  - `docs/evidence/telegram_real_30m_cold_start_e2e_report.json`
  - 闭环命令: `/help`、`/monitor TSLA 1h`、`/list`（均为 `processed`）
  - 通知状态变化: `notification_state_transitions: null->delivered`
  - `duration_minutes=1.1064`，`satisfy_under_30_minutes=true`
- B. webhook 失败链路（真实服务路径，非纯单测）:
  - `docs/evidence/telegram_webhook_service_path_report.json`
  - 转态链: `retry_pending -> retrying -> dlq`
  - 验收字段: `acceptance.has_retry_pending=true`、`acceptance.has_retrying=true`、`acceptance.has_dlq=true`
- 本轮新增脚本:
  - `scripts/telegram_webhook_service_path_report.py`
  - `scripts/telegram_api_mock.py`

## Environment Gap
- 真实 Telegram 外部接入仍受配置阻塞（未在仓库内注入密钥）:
  - `TELEGRAM_BOT_TOKEN` 缺失
  - `TELEGRAM_WEBHOOK_SECRET_TOKEN` 缺失
- 本轮 A 验收使用了本地 Telegram Bot API mock（服务路径真实、外部 Telegram 非真实）。

## D3 Artifacts
- 容器化模板：`docker-compose.telegram.yml`
- 运维文档更新：`docs/runbook.md`（新增 Telegram Phase D 章节）
- 灰度开关：`TELEGRAM_GRAY_RELEASE_ENABLED`

## D4~D6 Artifacts
- Alert Hub + 批量能力：`services/telegram_actions.py`, `services/telegram_store.py`
- watchlist group / webhook 分发：`services/watch_executor.py`, `services/notification_channels.py`
- 用户偏好与移动优先回包：`agents/telegram_command_router.py`, `services/telegram_gateway.py`
- 报告字段扩展：`docs/evidence/telegram_phase_d_run_report.json`
