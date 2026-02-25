# 📈 Alpha-Insight 量化 Agent 30天实战进度表

> **当前目标**: 构建从数据感知到代码执行，最后到移动端推送的全链路闭环。

## 📅 第一周：基础设施、沙箱与感知地基 (Eyes & Hands)
- [x] **Day 1: 环境初始化**
    - [x] 配置项目骨架，接入 E2B SDK。
    - [x] 实现异步沙箱管理器 `core/sandbox_manager.py`。
- [x] **Day 2-3: 结构化数据工具集**
    - [x] 封装 `tools/market_data.py` (yfinance 适配器)。
    - [x] 实现基础指令：获取股价并存入沙箱 DataFrame。
- [x] **Day 4-5: 动态爬虫 Agent (Scraper)**
    - [x] 集成 `Crawl4AI`，实现对非 API 网页的深度抓取。
    - [x] 编写逻辑：当 API 缺失数据时，Planner 自动路由至 Scraper。
- [x] **Day 6-7: 通讯终端集成**
    - [x] 接入 Telegram Bot，实现“分析完成通知”与“图片转发”功能。

## 📅 第二周：编排大脑与代码纠错循环 (The Brain)
- [x] **Day 8-9: LangGraph 状态拓扑构建**
    - [x] 定义 `AgentState` Pydantic 模型。
    - [x] 搭建 `Planner -> Coder -> Executor` 基础循环。
- [x] **Day 10-12: 深度规划逻辑 (Planner & R1)**
    - [x] 接入 DeepSeek-R1，编写 CoT 系统提示词。
    - [x] 实现任务拆解：Data Fetch -> Logic Calc -> Plotting。
- [x] **Day 13-14: 自修复循环 (Self-Correction Loop)**
    - [x] **核心**: 实现 Debugger 节点，解析沙箱 Traceback。
    - [x] 验证：故意编写错误 Python 语法，观察 Agent 是否能自主修复。
- [x] **Day 15: 逻辑回顾与基准测试**
    - [x] 确保所有计算 100% 发生在沙箱中，禁止 LLM 直接输出数字。

## 📅 第三周：专业量化与多模态输出 (Intelligence)
- [x] **Day 16-17: 交互式绘图集成**
    - [x] 在沙箱中封装 Plotly 生成 HTML 报告的逻辑。
    - [x] 实现前端提取，将沙箱内的 HTML/PNG 转发至用户界面。
- [x] **Day 18-19: 技术指标与回测引擎**
    - [x] 接入 TA-Lib，使 Coder 具备编写 MACD/RSI 策略的能力。
    - [x] 实现简单的策略回测模板（Pandas Vectorized Backtesting）。
- [x] **Day 20-21: 多模态融合研报**
    - [x] 实现文字情绪（Scraper 提供）与技术面数据（API 提供）的综合评分。
    - [x] 生成 PDF/HTML 格式的深度投研报告。
- [x] **Day 22-23: 人类介入 (HITL)**
    - [x] 实现 LangGraph 的中断机制：在发送买入信号前，等待 Telegram 确认。

## 📅 第四周：驾驶舱、安全与上线交付 (Voice)
- [x] **Day 24-25: 实时异动引擎 (Cron Job)**
    - [x] 实现每小时自动扫描关注列表 (Watchlist)。
    - [x] 触发异动报警：当涨跌幅或指标超限时，主动推送到 Telegram。
- [x] **Day 26-27: Streamlit 交互大屏**
    - [x] 开发 Web 端对话界面，实时可视化 Agent 思考过程 (Log Streaming)。
    - [x] 嵌入 Plotly 交互式行情看板。
- [x] **Day 28: 安全 Guardrails 与观测性**
    - [x] 限制沙箱库权限，接入 Arize Phoenix 进行全链路追踪。
- [x] **Day 29-30: 最终复盘与项目落地**
    - [x] 完善 README，录制“自修复代码”视频演示，完成最终部署。

---
*注：每天完成后请在 [ ] 中填入 x。*
