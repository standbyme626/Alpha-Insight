# Alpha-Insight

Alpha-Insight 是一个基于 LangGraph 的多 Agent 量化投研系统，支持沙箱执行、行情抓取、自动纠错、研报输出与 Telegram 推送。

## 已完成范围（第 1-4 周）

- 第 1 周：沙箱管理、行情数据工具、爬虫回退、Telegram 基础推送
- 第 2 周：`Planner -> Coder -> Executor -> Debugger` 自修复闭环
- 第 3 周：MACD/RSI、向量化回测、多模态研报、HITL 人工确认
- 第 4 周：实时异动扫描、分级告警、Streamlit 驾驶舱、安全 Guardrails、观测封装

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

2. 关键变量说明：

- `OPENAI_API_KEY`
- `OPENAI_API_BASE`（例如 DashScope OpenAI 兼容地址）
- `OPENAI_MODEL_NAME`（例如 `qwen3-32b`）
- `TEMPERATURE`
- `ENABLE_LOCAL_FALLBACK`
- `TELEGRAM_BOT_TOKEN`（可选）
- `TELEGRAM_CHAT_ID`（可选）

## 一键测试

```bash
docker compose --env-file .env run --rm test
```

## 后端怎么启动

### 1) 实时异动扫描后端（可用于 Cron）

单次执行（推荐给 cron 调度）：

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/hourly_watchlist_scan.py --once --watchlist 'AAPL,MSFT,TSLA' --market us --granularity hour --mode anomaly"
```

常驻循环（默认每小时一次）：

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/hourly_watchlist_scan.py --market cn --cn-top100 --granularity day"
```

### 2) 真实 LLM 连通性后端测试

```bash
docker compose --env-file .env run --rm dev bash -lc "export PYTHONPATH=/workspace && python scripts/real_llm_smoke_test.py"
```

## 前端怎么启动

当前有两个前端页面，都是 Streamlit，支持并行启动（不同端口）：

### A. 实时驾驶舱（Run 状态 + Watchlist + Pipeline）

```bash
docker compose --env-file .env run --rm -d --name alpha-insight-ui-cockpit -p 8501:8501 dev bash -lc "export PYTHONPATH=/workspace && streamlit run ui/streamlit_dashboard.py --server.address 0.0.0.0 --server.port 8501"
```

打开：`http://localhost:8501`

### B. Planner 控制台（中美市场请求模板）

```bash
docker compose --env-file .env run --rm -d --name alpha-insight-ui-llm -p 8502:8501 dev bash -lc "scripts/run_llm_frontend.sh"
```

打开：`http://localhost:8502`

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
- `core/observability.py`：span 计时、token 事件记录、Phoenix 对接预留

## 常见问题

1. `ModuleNotFoundError: agents`
需要在容器内设置：`export PYTHONPATH=/workspace`。

2. 真实 LLM 报错且 `ENABLE_LOCAL_FALLBACK=false`
表示远程调用失败时不会回退本地策略。可先排查 key、base URL、model，或临时改为 `true`。
