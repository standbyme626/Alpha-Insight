# Alpha-Insight 面试速记卡（新文件）

> 用途：面试前 30-60 分钟快速过稿。  
> 来源：基于当前项目代码与详细版题库压缩。  
> 结构：50 题，每题三行：`主答` / `追问补刀` / `证据路径`。

---

## 0) 开场话术（可直接背）

### 30 秒版本
我做的是 LLM 应用工程落地，不是只调模型 API。我在 Alpha-Insight 里把“请求理解、数据抓取、指标计算、告警分发、降级治理”做成了可追踪工作流，重点解决稳定性、可解释性和可运维问题。

### 60 秒版本
Alpha-Insight 是一个投研自动化系统：Telegram 侧支持命令和自然语言，后端统一走 workflow；分析输出固定 Card A/B/C/D，保证可解释；监控侧有 route strategy 和 strategy tier；运行侧有重试、DLQ、降级、恢复和状态汇总。我的核心工作是把不确定的 LLM 能力收敛成稳定可验收链路。

### 反问面试官（结尾可用）
你们现在最痛的是模型效果不稳、系统稳定性、还是上线治理（评估/风控/审计）？我可以按你们最痛点展开我的项目经验。

---

## A. 通用能力（1-15）

### Q1
主答：LLM 工程和后端的差别是“管理不确定性”，必须加回退、评估、审计。  
追问补刀：只会 prompt 不等于可上线系统。  
证据路径：`agents/workflow_engine.py`, `services/reliability_governor.py`

### Q2
主答：生产默认低温（0~0.2），创意场景才升温。  
追问补刀：参数必须可运行时调整并可持久化。  
证据路径：`ui/llm_frontend.py` Runtime Config + persist `.env`

### Q3
主答：结构化输出让下游可验证、可统计、可回归。  
追问补刀：纯文本会把系统变成正则猜测。  
证据路径：`services/telegram_actions.py::_build_analysis_contract`

### Q4
主答：有副作用用 function/tool calling，无副作用展示用 schema JSON。  
追问补刀：两者都要后置校验。  
证据路径：`services/telegram_actions.py` 动作执行层

### Q5
主答：Prompt Injection 要分层防护，不靠一句 system prompt。  
追问补刀：输入检测漏掉也要靠权限层兜底。  
证据路径：`agents/telegram_nlu_planner.py::detect_prompt_injection_risk`

### Q6
主答：降幻觉三步：先取证据、再生成、缺证据就降级。  
追问补刀：不输出伪确定结论。  
证据路径：Card C 证据三件套（`telegram_actions.py`）

### Q7
主答：RAG 常见瓶颈在检索，不在生成。  
追问补刀：先优化召回/排序再谈话术。  
证据路径：`tools/news_data.py`, `services/news_digest.py`

### Q8
主答：评估看离线准确 + 在线行为 + 成本延迟三角。  
追问补刀：指标要绑定业务任务。  
证据路径：`metric_events`（`services/telegram_store.py`）

### Q9
主答：可预测场景用 workflow，探索场景再用 agent。  
追问补刀：生产常见是“workflow 主干 + agent 局部”。  
证据路径：`agents/workflow_engine.py`

### Q10
主答：Eval 必须进 CI，且覆盖对抗样本。  
追问补刀：只做一次离线评测不够。  
证据路径：`docs/evidence/*`, `tests/*`

### Q11
主答：成本延迟优化靠路由、缓存、裁剪上下文、异步化。  
追问补刀：先定义质量下限，再谈降本。  
证据路径：`services/runtime_controls.py`

### Q12
主答：提示词只能提概率，不能给确定性保障。  
追问补刀：保障来自状态机、重试、幂等、降级。  
证据路径：`services/telegram_store.py`

### Q13
主答：模型输出当不可信输入，必须清洗和限制执行边界。  
追问补刀：尤其 URL/HTML/命令片段。  
证据路径：`services/news_digest.py::redact_user_visible_payload`

### Q14
主答：故障先看 SLI（成功率、p95、retry、DLQ）。  
追问补刀：先止血再定位。  
证据路径：`services/telegram_actions.py::handle_status`

### Q15
主答：可解释输出=结论+证据+口径+动作。  
追问补刀：只有结论没有证据不可上线。  
证据路径：Card A/B/C/D 设计

---

## B. 系统设计与可靠性（16-30）

### Q16
主答：网关和调度器解耦，接入流量与执行负载隔离。  
追问补刀：否则高峰时会互相拖垮。  
证据路径：`telegram_long_polling_gateway.py`, `telegram_watch_scheduler.py`

### Q17
主答：开发优先 long polling，生产优先 webhook。  
追问补刀：webhook 可叠加 secret token/IP 控制。  
证据路径：`scripts/telegram_webhook_gateway.py`

### Q18
主答：幂等核心是 claim + mark，不依赖平台不重放。  
追问补刀：进度和最终消息要分开去重。  
证据路径：`request_progress_messages`, `final_message_dispatches`

### Q19
主答：可重试与不可重试错误要分流。  
追问补刀：超限入 DLQ，防无限重试。  
证据路径：`services/watch_executor.py`

### Q20
主答：降级由 SLO 触发，不靠人工拍脑袋。  
追问补刀：恢复也要自动化并写事件。  
证据路径：`services/reliability_governor.py`

### Q21
主答：route strategy 管“发到哪”，adapter 管“怎么发”。  
追问补刀：新增通道不改策略引擎。  
证据路径：`_resolve_routes`, `notification_channels.py`

### Q22
主答：strategy tier 是风险边界，不是业务装饰字段。  
追问补刀：research-only/alert-only/execution-ready 分层要前置。  
证据路径：`core/strategy_tier.py`

### Q23
主答：多表建模是为了状态机、审计、恢复，不是过度设计。  
追问补刀：单表很快会失控。  
证据路径：`services/telegram_store.py` 建表段

### Q24
主答：超时请求进恢复队列，避免用户感知“黑洞失败”。  
追问补刀：恢复后仍走幂等发送。  
证据路径：`analysis_recovery_queue`, `process_due_analysis_recovery`

### Q25
主答：进度消息 edit 可降噪、防刷屏、降风控风险。  
追问补刀：失败要有降级回退。  
证据路径：`send_analysis_progress`

### Q26
主答：配置分层 base->env->runtime，保留 source trace。  
追问补刀：定位配置漂移会快很多。  
证据路径：`core/runtime_config.py`

### Q27
主答：审计事件是 LLM 系统的“黑匣子”。  
追问补刀：没有审计就很难 RCA。  
证据路径：`audit_events` 表

### Q28
主答：限流至少两层：会话级 + 全局并发级。  
追问补刀：高成本动作要单独限。  
证据路径：`RuntimeLimits`, `GlobalConcurrencyGate`

### Q29
主答：Market Pulse 是运营摘要，不是普通告警。  
追问补刀：看的是“系统+市场”双状态。  
证据路径：`services/market_pulse.py`

### Q30
主答：SQLite 适合快速落地，Postgres 适合高并发与多实例。  
追问补刀：迁移优先做双写校验与幂等验证。  
证据路径：`TelegramTaskStore` + `docker-compose.telegram.yml`

---

## C. 项目深挖（31-50）

### Q31
主答：分析链路是“网关入库 -> 动作触发 -> 研究执行 -> 卡片生成 -> 幂等发送”。  
追问补刀：每步都有状态落库可追踪。  
证据路径：`telegram_gateway.py`, `telegram_actions.py`

### Q32
主答：“Planner fallback”先查进程环境变量，再查远程调用，再看 fallback 开关。  
追问补刀：排障时可暂时关 fallback 暴露真实错误。  
证据路径：`agents/planner_engine.py`

### Q33
主答：Card A 数据不足核心是 OHLC 序列缺失或映射不一致。  
追问补刀：要给清晰降级文案，不给伪结论。  
证据路径：`_compute_window_metrics_from_records`

### Q34
主答：技术一句话必须含 MA/支撑/压力/触发条件。  
追问补刀：模板话术对交易无帮助。  
证据路径：`_technical_sentence_with_levels`

### Q35
主答：新闻主题必须可核验（标题+媒体+时间+链接）。  
追问补刀：情绪分必须附 N 和方法。  
证据路径：`services/news_digest.py`

### Q36
主答：N<5 不输出情绪分，避免误导。  
追问补刀：这是风险控制，不是保守。  
证据路径：`_news_theme_lines`

### Q37
主答：任务、事件、通知拆表是为了可恢复和可重放。  
追问补刀：通知状态机独立非常关键。  
证据路径：`watch_jobs/watch_events/notifications`

### Q38
主答：route_strategy 与 strategy_tier 正交，分别控路由与权限。  
追问补刀：能做更细粒度治理。  
证据路径：`watch_jobs` 字段 + `watch_executor`

### Q39
主答：降级恢复必须事件化，否则无法做可靠复盘。  
追问补刀：触发和恢复都要有 reason。  
证据路径：`degradation_events`

### Q40
主答：fast lane immediate retry 能减少关键告警漏发。  
追问补刀：要配预算，防流量放大。  
证据路径：`watch_executor.py`

### Q41
主答：`/status` 最关键看成功率、retry depth、DLQ、活跃降级。  
追问补刀：这是值班决策入口。  
证据路径：`handle_status`

### Q42
主答：群聊误操作防护靠 allowlist/blacklist + command allowlist + 限流。  
追问补刀：入口拦截成本最低。  
证据路径：`TelegramGateway` 初始化参数

### Q43
主答：NLU 低置信时先澄清槽位，不直接执行。  
追问补刀：先确认再动作。  
证据路径：`extract_clarify_slots`, `clarify_pending`

### Q44
主答：双前端并存是角色分工，不是重复建设。  
追问补刀：Streamlit 运维快，Next 资源化强。  
证据路径：`ui/*`, `web_console/*`

### Q45
主答：Next 当前读快照 JSON，展示稳定但实时性有限。  
追问补刀：下一步应升级实时 typed API。  
证据路径：`web_console/lib/resources.ts`

### Q46
主答：前端 Runtime Config 写入 `.env` 降低了手工运维错误。  
追问补刀：参数改动从“SSH操作”变成“界面操作”。  
证据路径：`ui/llm_frontend.py` persist 逻辑

### Q47
主答：沙箱常见故障是 Docker 不可用，必须自动 fallback。  
追问补刀：可用性优先于理想执行环境。  
证据路径：`core/sandbox_manager.py`

### Q48
主答：可验收交付靠证据链，不靠口头“我测过了”。  
追问补刀：证据要可复现。  
证据路径：`docs/evidence/*`, `tests/*`

### Q49
主答：生产化优先三件：实时 API、Eval CI、存储迁移。  
追问补刀：直接提升实时性、稳定性、扩展性。  
证据路径：`web_console/lib/*`, `tests/*`, `telegram_store.py`

### Q50
主答：是否可上生产是风险管理结论，不是功能演示结论。  
追问补刀：必须给 SLO、降级、回滚、证据四件套。  
证据路径：`/status`, `degradation_states`, `docs/evidence/`

---

## 面试当天建议
1. 先讲“系统稳定性设计”再讲“模型效果”，更像资深工程师。  
2. 每答一个概念，立刻补一句项目证据路径。  
3. 遇到不会的问题，用“我会怎么验证/落地”的工程方法回答。  
4. 结尾反问：你们当前最痛点在效果、稳定性、还是治理，我可按这个展开。

## 相关文档
- 详细版 50 题：`docs/interview/alpha_insight_ai_llm_interview_50_qa_detailed_v2.md`
- 上一版：`docs/interview/alpha_insight_ai_llm_interview_50_qa.md`
