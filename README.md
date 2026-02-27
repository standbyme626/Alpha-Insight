# Alpha-Insight

Alpha-Insight 是一个基于 LangGraph 的多 Agent 量化投研系统，支持沙箱执行、行情抓取、自动纠错、研报输出与 Telegram 推送。

## 已完成范围（第 1-4 周）

- 第 1 周：沙箱管理、行情数据工具、爬虫回退、Telegram 基础推送
- 第 2 周：`Planner -> Coder -> Executor -> Debugger` 自修复闭环
- 第 3 周：MACD/RSI、向量化回测、多模态研报、HITL 人工确认
- 第 4 周：实时异动扫描、分级告警、Streamlit 驾驶舱、安全 Guardrails、观测封装

## 当前可用功能（在历史能力基础上的增量）

- 三市场监控：A 股 / 港股 / 美股，支持 Top100 监控池
- 公司名展示：前端与告警统一展示公司名，支持 `代码(公司名)` 与 `代码 | 公司名`
- Telegram 告警：分级告警文案中英双语，字段统一（价格/涨跌幅/RSI/原因/时间）
- Telegram 命令增强（Phase D）：`/report <run_id|request_id>`、`/digest daily`、`/monitor <symbol> <interval> [volatility|price|rsi]`
- 双前端页面：
  - `8501` 实时驾驶舱（扫描、信号、流水线、告警）
  - `8502` Planner 控制台（规划）+ Full Analysis（完整分析产物）
- Full Analysis 模式：可展示沙箱代码、stdout/stderr、traceback、重试次数
- 沙箱容灾：Docker 沙箱不可用（如 `docker.sock permission denied`）时，自动回退本地进程执行（仍受 guardrails）

## 目录结构

- `agents/`：工作流与 Agent 逻辑（`planner_engine.py`, `workflow_engine.py`, `report_workflow.py`, `scanner_engine.py`）
- `core/`：沙箱、模型、安全策略、观测模块
- `tools/`：行情、Telegram、产物提取
- `scripts/`：集成脚本、定时任务脚本、真实 LLM 测试脚本
- `ui/`：前端页面（Streamlit，含运行态可观测面板）
- `tests/`：Week1-Week4 的 pytest 测试
- `models/`：模型目录（本项目当前主要使用远程 API 模型，见 `models/MODELS.md`）

## 环境准备

1. 创建并填写 `.env`（已提供 `.env.example`）：

```bash
cp .env.example .env
```

2. 关键变量：

- `OPENAI_API_KEY`
- `OPENAI_API_BASE`（例如 DashScope OpenAI 兼容地址）
- `OPENAI_MODEL_NAME`（例如 `qwen3-32b`）
- `TEMPERATURE`
- `ENABLE_LOCAL_FALLBACK`
- `TELEGRAM_BOT_TOKEN`（可选）
- `TELEGRAM_CHAT_ID`（可选）

3. 推荐本地 Python 环境（与当前测试一致）：

```bash
python3 -m venv .venv_local
.venv_local/bin/pip install -r requirements-dev.txt
```

## 测试

```bash
docker compose --env-file .env run --rm test
```

或本地：

```bash
.venv_local/bin/python -m pytest -q
```

## 硬口径验收证据（Run Report + 离线20次）

已提供固定证据脚本：`scripts/hard_acceptance_evidence.py`，默认将产物写入 `docs/evidence/`。

1. 生成最新一次 run_report（样例）：

```bash
PYTHONPATH=/home/kkk/Project/Alpha-Insight python scripts/hard_acceptance_evidence.py generate --runs 1 --offline --output-json docs/evidence/run_report_latest.json --output-md docs/evidence/run_report_latest.md --title "Run Report (Latest Full Analysis)"
```

2. 生成离线 Docker Full Analysis 20 次统计：

```bash
PYTHONPATH=/home/kkk/Project/Alpha-Insight python scripts/hard_acceptance_evidence.py generate --runs 20 --offline --output-json docs/evidence/offline_docker_full_analysis_20.json --output-md docs/evidence/offline_docker_full_analysis_20.md --title "Offline Docker Full Analysis Benchmark (20 Runs)"
```

3. 记录 pytest 门禁结果：

```bash
PYTHONPATH=/home/kkk/Project/Alpha-Insight pytest -q | tee docs/evidence/pytest_gate_latest.txt
```

字段覆盖：`success/fallback/retry/latency/backend/failure_type`，用于硬口径复盘与回归对比。

## 后端启动

### 1) 实时异动扫描（可用于 Cron）

单次执行（推荐给 cron 调度）：

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/hourly_watchlist_scan.py --once --watchlist 'AAPL,MSFT,TSLA' --market us --granularity hour --mode anomaly"
```

常驻循环（默认每小时一次）：

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/hourly_watchlist_scan.py --market cn --top100 --granularity day"
```

可选市场：`us | hk | cn | auto`

阈值告警参数（D'）：

- `--fallback-spike-rate`：回退占比阈值（默认 `0.25`）
- `--failure-spike-count`：失败数阈值（默认 `3`）
- `--latency-anomaly-ms`：延迟阈值（默认 `2500`）

### 2) 真实 LLM 连通性测试

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/real_llm_smoke_test.py"
```

## 前端启动

当前有两个 Streamlit 页面，可并行启动：

### A. 实时驾驶舱（8501）

```bash
docker compose --env-file .env run --rm -d --name alpha-insight-ui-cockpit -p 8501:8501 dev bash -lc "export PYTHONPATH=/workspace && streamlit run ui/streamlit_dashboard.py --server.address 0.0.0.0 --server.port 8501"
```

打开：`http://localhost:8501`

能力说明：

- 启动即展示 Top100 成分（代码 + 公司名）
- 三市场切换、粒度切换（日/时/分）
- 信号图与信号表显示 `代码(公司名)`
- Telegram 预览与发送
- Runtime Log 中英双语

### B. Planner 控制台 + Full Analysis（8502）

```bash
docker compose --env-file .env run --rm -d --name alpha-insight-ui-llm -p 8502:8501 dev bash -lc "scripts/run_llm_frontend.sh"
```

打开：`http://localhost:8502`

能力说明：

- `Run Planner`：仅做规划（steps/data_source/reason）
- `Run Full Analysis`：执行 Week2 全流程并显示完整产物
  - sandbox code
  - sandbox stdout / stderr
  - traceback
  - retry count / success

查看运行状态：

```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

查看日志：

```bash
docker logs -f alpha-insight-ui-cockpit
docker logs -f alpha-insight-ui-llm
```

停止前端：

```bash
docker stop alpha-insight-ui-cockpit alpha-insight-ui-llm
```

## 安全与观测

- `core/guardrails.py`：限制危险导入、网络调用、危险执行函数与越界路径
- `core/observability.py`：span 计时、token 事件、success/fallback/retry/latency 指标、失败聚类与阈值告警规则

## 运维 Runbook

- 详细冷启动与应急流程见：[docs/runbook.md](docs/runbook.md)
- Telegram 网关/Worker 容器化模板见：`docker-compose.telegram.yml`

## 常见问题

1. `ModuleNotFoundError: agents`
需要在容器内设置：`export PYTHONPATH=/workspace`，或确保从项目根目录启动。

2. 真实 LLM 报错且 `ENABLE_LOCAL_FALLBACK=false`
表示远程调用失败时不会回退本地策略。可先排查 key、base URL、model，或临时改为 `true`。

3. Planner 显示 `provider=fallback`
说明远程模型调用失败或环境变量未生效。优先检查：
- `OPENAI_API_KEY`
- `OPENAI_API_BASE`
- `OPENAI_MODEL_NAME`

4. Full Analysis 报 Docker 权限错误
当前版本已支持自动回退到本地进程执行。若需强隔离执行，需修复 Docker 权限或镜像环境。
