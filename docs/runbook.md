# Alpha-Insight Ops Runbook

本 Runbook 面向值班/交接场景，覆盖冷启动、重建、端口、依赖、权限与数据回退策略。

## 1. Cold Start SOP（冷启动）

1. 拉取最新代码并进入项目根目录：
   - `git pull --rebase`
   - `cd /home/kkk/Project/Alpha-Insight`
2. 准备环境变量：
   - `cp .env.example .env`（首次）
   - 检查 `OPENAI_*`、`ENABLE_LOCAL_FALLBACK`、`TELEGRAM_*`
3. 安装依赖（本地）：
   - `python3 -m venv .venv_local`
   - `.venv_local/bin/pip install -r requirements-dev.txt`
4. 快速健康检查：
   - `.venv_local/bin/python -m pytest -q`
   - `python -m py_compile agents/*.py core/*.py tools/*.py scripts/*.py ui/*.py`

## 2. Rebuild / Restart

### Docker 路径

- 测试：
  - `docker compose --env-file .env run --rm test`
- 单次扫描：
  - `docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/hourly_watchlist_scan.py --once --watchlist 'AAPL,MSFT,TSLA' --market us --granularity hour --mode anomaly"`
- 驾驶舱：
  - `docker compose --env-file .env run --rm -d --name alpha-insight-ui-cockpit -p 8501:8501 dev bash -lc "export PYTHONPATH=/workspace && streamlit run ui/streamlit_dashboard.py --server.address 0.0.0.0 --server.port 8501"`
- LLM 前端：
  - `docker compose --env-file .env run --rm -d --name alpha-insight-ui-llm -p 8502:8501 dev bash -lc "scripts/run_llm_frontend.sh"`

### 本地路径

- `export PYTHONPATH=/home/kkk/Project/Alpha-Insight`
- `streamlit run ui/streamlit_dashboard.py --server.port 8501`

## 3. Ports / Process

- `8501`: Realtime Cockpit
- `8502`: Planner + Full Analysis
- 查看容器：
  - `docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"`
- 查看日志：
  - `docker logs -f alpha-insight-ui-cockpit`
  - `docker logs -f alpha-insight-ui-llm`

## 4. Dependencies / Runtime Prerequisites

- Python: `3.11+`
- 必装包来源：`requirements.txt` + `requirements-dev.txt`
- 可选外部依赖：
  - Docker（推荐沙箱隔离）
  - Telegram Bot（告警发送）
  - 远程 LLM Provider（OpenAI 兼容）

## 5. Permissions / Security Notes

- `docker.sock permission denied` 时，系统会自动降级本地进程执行（`local-process-fallback`）。
- Guardrails 默认启用，阻断网络访问、危险命令与动态安装。
- 若必须恢复容器沙箱，优先修复 Docker 权限，不要关闭 Guardrails。

## 6. Data Fallback Strategy（数据回退）

1. `market_data` API 失败：
   - Week1/Week2 流程路由到 scraper 或本地 fallback planner。
2. 扫描失败聚类：
   - 统一落在 `runtime_failure_clusters`（如 `data/network/sandbox/fallback`）。
3. 触发式研究失败：
   - 在 `AlertSnapshot.research_status=failed` 与 `research_error` 记录；扫描流程不中断。

## 7. Threshold Alarms（阈值告警）

扫描器支持三类阈值告警：

- `fallback_spike`: 回退占比异常
- `failure_spike`: 失败数激增
- `latency_anomaly`: 周期延迟异常

可通过 CLI 配置：

- `--fallback-spike-rate`（默认 `0.25`）
- `--failure-spike-count`（默认 `3`）
- `--latency-anomaly-ms`（默认 `2500`）

告警会输出在：

- `scripts/hourly_watchlist_scan.py` 日志
- Streamlit 驾驶舱 `Overview` 与 `Artifacts & Alerts` 面板

## 8. Incident Checklist（故障处置）

1. 先看 `runtime_metrics` 与 `alarms`，确认是回退、失败还是延迟。
2. 若 `failure_spike`：
   - 检查行情源可用性、符号格式、网络连通性。
3. 若 `fallback_spike`：
   - 检查 Docker 沙箱权限、远程模型可用性。
4. 若 `latency_anomaly`：
   - 降低 watchlist 数量，缩短周期，或拆分调度批次。
5. 保留 `artifacts/alerts/watchlist_alert_snapshots.jsonl` 作为审计记录。

## 9. Telegram Phase D（Gateway + Worker + Multi-Channel）

### 9.1 启动模板（容器化）

- 使用 `docker-compose.telegram.yml`：
  - `docker compose -f docker-compose.telegram.yml --env-file .env up -d telegram-db telegram-gateway telegram-worker`
- 检查服务：
  - `docker compose -f docker-compose.telegram.yml ps`

### 9.2 灰度发布（白名单 chat）

1. 开启灰度：
   - `TELEGRAM_GRAY_RELEASE_ENABLED=true`
2. 仅放行白名单 chat（写入 `allowlist_chats`）：
   - 对目标 chat 执行 `set_allowlist_chat(chat_id, can_monitor=1)`（可通过运维脚本或临时 Python shell）。
3. 验证：
   - 白名单 chat 命令可执行；
   - 非白名单 chat 返回 `gray release active: chat not allowlisted`。

### 9.3 故障切换与回滚

- 网关异常：
  1. `docker compose -f docker-compose.telegram.yml restart telegram-gateway`
  2. 观察 `bot_updates` 中 `processing` 是否持续下降。
- Worker 异常：
  1. `docker compose -f docker-compose.telegram.yml restart telegram-worker`
  2. 检查 `notifications` 的 `retry_pending/dlq` 是否持续增加。
- 快速回滚：
  1. 关闭灰度：`TELEGRAM_GRAY_RELEASE_ENABLED=false`
  2. 回滚镜像或代码后重启 gateway/worker。

### 9.4 数据修复

- 卡住的 update 恢复：
  - 将 `bot_updates.status='processing'` 的记录交由 `process_pending_updates` 重新消费。
- 重推失败修复：
  - 将 `notifications.state='retry_pending'` 且 `next_retry_at<=now`，由 worker 自动重试。
- 报告查询修复：
  - 如 `analysis_requests.status='completed'` 但 `analysis_reports` 缺失，可依据 `request_id/run_id` 回填摘要。
