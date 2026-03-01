# Alpha-Insight 面试题库（AI工程师 / LLM应用工程师）

> 版本：2026-03-01  
> 生成方式：基于项目代码遍历 + 联网检索行业面试题方向后定制。  
> 适用岗位：AI Engineer、LLM Application Engineer、Agent/Workflow Engineer。

## 使用说明
- 每题包含：`问题`、`参考回答`、`项目落点`。
- `参考回答`按面试口语化表达写，可直接复述。
- 项目落点优先引用本仓库实际实现，避免“空谈概念”。

---

## A. 通用 AI/LLM 能力（1-15）

### Q1. 什么是 LLM 应用工程，和传统后端工程有什么不同？
**参考回答：** LLM 应用工程不是只调 API，而是把模型能力变成稳定业务流程。核心差异在于不确定性管理：要做提示词约束、结构化输出、失败回退、评估闭环和安全防护。传统后端更关注确定性逻辑，LLM 工程还要处理概率输出与对齐风险。
**项目落点：** `agents/workflow_engine.py`（流程编排）、`agents/planner_engine.py`（远程失败回退）、`services/reliability_governor.py`（降级治理）。

### Q2. temperature 在生产里怎么设？
**参考回答：** 面向事实抽取、规划、告警这类任务，我会优先 `temperature=0~0.2`，提高可复现性；只有创意文案类才升高。并且把温度作为可配置项而不是写死。
**项目落点：** `.env` + `ui/llm_frontend.py` 的 Runtime Config（可改温度并持久化）。

### Q3. 你如何设计“结构化输出”而不是纯文本输出？
**参考回答：** 我会把模型输出约束为 schema，再做程序级验证。这样能把下游处理从“正则猜测”变成“字段驱动”，减少线上解析失败。
**项目落点：** 项目在业务层做了结构化卡片契约：`services/telegram_actions.py::_build_analysis_contract`，并把关键指标写入 `analysis_reports`。

### Q4. Function calling 和纯 JSON 响应你怎么选？
**参考回答：** 需要触发系统动作（查库、发消息、调用外部接口）时用 function calling；只需要结构化展示给用户时用 JSON schema 输出。判断标准是“是否要驱动系统副作用”。
**项目落点：** 当前系统偏“工作流 + 工具函数”模式，动作层在 `TelegramActions` 中执行。

### Q5. 什么是 Prompt Injection？你会怎么防？
**参考回答：** Prompt Injection 是把恶意指令伪装成用户输入或上下文，诱导模型越权。防护要分层：输入检测、最小权限工具、输出校验、审计与限速，不能只靠一句 system prompt。
**项目落点：** `agents/telegram_nlu_planner.py::detect_prompt_injection_risk`，以及网关审计逻辑 `services/telegram_gateway.py`。

### Q6. 你如何降低幻觉（hallucination）？
**参考回答：** 三步：先检索/取数，再回答；回答里绑定证据字段；最后做“无证据降级”而不是硬答。对关键场景还要加评测集持续回归。
**项目落点：** Card C 证据三件套、新闻来源覆盖、行情源和指标口径均在 `telegram_actions` 中显式输出。

### Q7. RAG 的核心瓶颈通常在哪？
**参考回答：** 大多数问题不在生成，而在检索：召回不准、排序不稳、chunk 切分不合理。必须先把 retrieval 指标打稳，再谈生成质量。
**项目落点：** 本项目新闻聚合 + 主题化在 `tools/news_data.py` 与 `services/news_digest.py`，重点做了去重、主题分类和样本门槛。

### Q8. 检索系统里你会看哪些指标？
**参考回答：** 离线看 recall/precision/NDCG，在线看点击、复用率和人工满意度；同时看延迟和成本。指标一定要和业务任务绑定。
**项目落点：** 可对齐到 `metric_events`（`services/telegram_store.py`）做线上统计沉淀。

### Q9. 什么时候用 Agent，什么时候用 Workflow？
**参考回答：** 可预测、高频、强约束场景优先 Workflow；开放探索场景才引入 Agent。生产里一般是“Workflow 主干 + Agent 局部”。
**项目落点：** 本项目主路径是 workflow 编排（planner -> data -> coder -> executor），不是全自动自由代理。

### Q10. 你如何做 LLM 应用评估（Eval）？
**参考回答：** 先定义任务成功标准，再用真实流量样本构建测试集，自动指标 + 人工标注联合评估，最后进 CI 做回归门禁。
**项目落点：** `docs/evidence/` 中已有多类验收证据，可继续扩展为标准化 eval 套件。

### Q11. 成本和延迟如何同时优化？
**参考回答：** 典型组合是：路由小模型优先、缓存高频请求、减少上下文长度、失败快速降级、关键链路异步化。
**项目落点：** 调度与降级链路已具备：`runtime_controls`、`watch_executor`、`reliability_governor`。

### Q12. 为什么“只靠提示词”不够？
**参考回答：** 因为提示词只能提高概率，不能提供确定性保证。生产里必须加 schema 验证、状态机、权限边界和重试策略。
**项目落点：** `TelegramTaskStore` 的状态表 + 去重机制就是把不确定输出收敛到确定流程。

### Q13. 你怎么做输出安全（Insecure Output Handling）？
**参考回答：** 模型输出视为不可信输入，必须在下游执行前做校验和转义，禁止直接拼接到命令执行或高权限动作。
**项目落点：** `services/news_digest.py::redact_user_visible_payload` + 动作层用户文案清洗。

### Q14. 线上故障时先看什么？
**参考回答：** 先看关键 SLI：成功率、延迟、重试队列、DLQ，再看最近发布变更和外部依赖状态，最后再深挖具体链路。
**项目落点：** `/status` 汇总 push success、retry depth、DLQ、degrade states（`handle_status`）。

### Q15. 你怎么做“人类可解释输出”？
**参考回答：** 结果必须包含“结论 + 证据 + 口径 + 下一步动作”。只有结论没有证据，在投研/风控场景是不可用的。
**项目落点：** Card A/B/C/D 的设计就是可解释交付。

---

## B. LLM 应用系统设计（16-30）

### Q16. 你会如何设计 Telegram Bot 的高可用架构？
**参考回答：** 网关层和调度层解耦：网关负责接入与入队，调度器负责执行与重试。这样接入流量波动不会直接压垮执行链路。
**项目落点：** `scripts/telegram_long_polling_gateway.py` + `scripts/telegram_watch_scheduler.py`。

### Q17. Long polling 和 webhook 怎么选？
**参考回答：** 内网/开发环境用 long polling 快速落地；生产有公网和网关治理时优先 webhook，延迟更低、可控性更好。
**项目落点：** 两套入口都已实现：`telegram_long_polling_gateway.py`、`telegram_webhook_gateway.py`。

### Q18. 如何实现幂等与去重，避免 Telegram 重复发消息？
**参考回答：** 核心是请求级唯一键 + 最终发送声明表。发送前 claim，发送后 mark，失败可重试但不重复投递。
**项目落点：** `final_message_dispatches` 与 `request_progress_messages`（`telegram_store.py`）。

### Q19. 你如何设计重试策略？
**参考回答：** 区分可重试和不可重试错误；指数退避 + 最大重试次数；超限入 DLQ，避免无限重试拖垮系统。
**项目落点：** `watch_executor.py` 的 retry/backoff + `dlq` 状态。

### Q20. 什么情况下触发系统降级？
**参考回答：** 当关键 SLO 持续恶化（成功率、p95、DLQ 趋势）就进入降级，优先保住核心链路，再逐步恢复。
**项目落点：** `services/reliability_governor.py` 维护 `no_monitor_push`、`summary_mode` 等状态。

### Q21. 多通道通知如何做路由治理？
**参考回答：** 将路由策略显式化（telegram/email/wecom/webhook），执行前按策略与偏好计算目标通道，失败通道单独重试。
**项目落点：** `watch_executor.py::_resolve_routes` + `notification_channels.py`。

### Q22. 你如何设计策略分层（strategy tier）？
**参考回答：** 把策略分成 research-only / alert-only / execution-ready，按风险与权限决定可执行动作，防止高风险动作误触发。
**项目落点：** `core/strategy_tier.py` + `watch_executor.py` 的 tier guard。

### Q23. 存储层为什么用这么多表？
**参考回答：** 因为要支持审计、恢复、追踪和回放。单表无法满足多状态机场景，必须把请求、事件、通知、降级分开建模。
**项目落点：** `telegram_store.py` 中 `analysis_requests/watch_jobs/notifications/degradation_states` 等。

### Q24. 你怎么处理“异步任务超时但用户要结果”场景？
**参考回答：** 请求超时不等于任务失败，进入恢复队列异步补跑，完成后再投递结果或摘要。
**项目落点：** `analysis_recovery_queue` + `process_due_analysis_recovery`。

### Q25. 为什么要把“进度消息”做成 edit 而不是多条发送？
**参考回答：** 多条进度会刷屏、降低可读性。进度编辑让用户只关注最终结果，同时降低消息量和风控风险。
**项目落点：** `send_analysis_progress` 使用 `send_progress/edit_message_text`。

### Q26. 你如何设计统一配置层（config layering）？
**参考回答：** base -> env override -> runtime flags，保留 source trace 和 diff summary，便于定位“到底哪个层覆盖了配置”。
**项目落点：** `core/runtime_config.py::resolve_runtime_config`。

### Q27. 为什么要做审计事件（audit events）？
**参考回答：** LLM 系统不是纯函数，存在外部副作用。审计事件是排障、合规和复盘的基础。
**项目落点：** `telegram_store.py` 的 `audit_events` 表及记录逻辑。

### Q28. 你会如何做限流？
**参考回答：** 至少两层：会话级限流（防滥用）+ 全局并发门控（防雪崩）。
**项目落点：** `RuntimeLimits` + `GlobalConcurrencyGate`。

### Q29. 为什么要有 Market Pulse？
**参考回答：** 它是“监控运营层”的摘要输出，不是单次告警。能把系统健康、市场异动、队列风险做周期化播报。
**项目落点：** `services/market_pulse.py::MarketPulsePublisher.publish_due`。

### Q30. 你如何看待 SQLite 与 Postgres 取舍？
**参考回答：** SQLite 快速、轻部署，适合单机与中小规模；高并发、多实例、复杂查询与HA场景应迁到 Postgres。
**项目落点：** 当前主实现在 `TelegramTaskStore`（SQLite），但 compose 里已出现 Postgres 组件，说明存在迁移方向。

---

## C. Alpha-Insight 项目深挖（31-50）

### Q31. 这个项目的一次“分析请求”从入口到输出如何流转？
**参考回答：** Telegram 网关接收命令/NL并入库，动作层触发统一研究，生成 Card A/B/C/D，写报告与指标，最后发送结果并记录去重状态。
**项目落点：** `telegram_gateway.py`、`telegram_actions.py`、`workflow_engine.py`、`telegram_store.py`。

### Q32. 出现“Planner is using local fallback”你怎么排查？
**参考回答：** 先查当前进程环境变量是否生效（key/base/model），再看远程请求是否失败，最后确认 `ENABLE_LOCAL_FALLBACK` 是否开启。
**项目落点：** `agents/planner_engine.py::_load_config/plan_tasks`。

### Q33. Card A 为什么会出现数据不足？
**参考回答：** 通常是只拿到 latest close 但缺少近30日 OHLC 序列，或者字段映射不一致导致窗口为空。
**项目落点：** `_extract_ohlc_records_from_result` 与 `_compute_window_metrics_from_records`。

### Q34. 你如何保证技术一句话“可操作”？
**参考回答：** 必须包含可量化价位：MA10/MA20、支撑、压力、触发条件，不说纯形容词。
**项目落点：** `_technical_sentence_with_levels`。

### Q35. 新闻主题化为什么要强制“标题+媒体+时间+链接”？
**参考回答：** 这是可核验最小闭环，避免“只给情绪分”造成不可追溯结论。
**项目落点：** `news_digest.py` 的 `TopNewsItem/ThemeDigestItem` 与格式化函数。

### Q36. 为什么 N<5 时不输出情绪分？
**参考回答：** 小样本情绪分波动大，业务上会误导决策。应明确标注样本不足而不是硬给数值。
**项目落点：** `TelegramActions._news_theme_lines`。

### Q37. 监控任务是怎么建模的？
**参考回答：** 任务（watch_jobs）和事件（watch_events）分离，通知（notifications）独立状态机，便于回放与重试。
**项目落点：** `telegram_store.py` 的表结构与索引。

### Q38. 为什么要有 `route_strategy` 和 `strategy_tier` 两个维度？
**参考回答：** 一个解决“发到哪里”，一个解决“允许做到哪一步”，分别控制分发与权限边界。
**项目落点：** `watch_jobs` 字段 + `watch_executor.py` 执行路径。

### Q39. 降级状态如何恢复？
**参考回答：** 降级不应人工拍脑袋恢复，要由指标趋势自动恢复并写入恢复事件，保证一致性。
**项目落点：** `ReliabilityGovernor.reconcile` + `degradation_events`。

### Q40. 为什么要做 fast lane immediate retry？
**参考回答：** 高优先级告警允许短窗口立即重试，可显著降低瞬时网络抖动导致的漏报。
**项目落点：** `watch_executor.py` 中 fast lane 重试计数逻辑。

### Q41. `/status` 输出应该看哪些字段最关键？
**参考回答：** 24h 投递成功率、retry depth、DLQ、活跃降级状态、最近恢复时间。它们能快速判断系统是“可用但降级”还是“投递失效”。
**项目落点：** `TelegramActions.handle_status`。

### Q42. 你如何防止用户在群聊里误操作高风险命令？
**参考回答：** 做访问控制（allowlist/blacklist）、命令白名单、灰度开关、会话限流。
**项目落点：** `TelegramGateway` 初始化参数和拦截逻辑。

### Q43. NLU 误判时怎么兜底？
**参考回答：** 采用“可澄清槽位”机制，不确定时先追问再执行，避免错误动作直接落地。
**项目落点：** `extract_clarify_slots` 与 pending selection 存储表。

### Q44. 你如何理解这个项目的“前后端双栈”？
**参考回答：** Streamlit 适合快速运维可视化；Next.js 适合资源化页面与扩展。两者服务不同角色，不是二选一。
**项目落点：** `ui/*` 与 `web_console/*`。

### Q45. Next.js 控制台当前的数据读取模式是什么？
**参考回答：** 不是直连数据库，而是读取导出的资源快照 JSON，适合验收展示但实时性有限。
**项目落点：** `web_console/lib/resources.ts` + `scripts/upgrade7_frontend_resources_export.py`。

### Q46. 你最近在前端做了什么可运维改动？
**参考回答：** Runtime Config 增加“写入 .env”能力，前端输入后可同时更新当前进程和持久配置，减少手工改服务器文件。
**项目落点：** `ui/llm_frontend.py`（Persist to .env）。

### Q47. 沙箱相关最常见线上问题是什么？
**参考回答：** Docker 不可用（如二进制缺失/权限不足）。策略应自动切到本地执行并上报降级，而不是直接中断业务。
**项目落点：** `core/sandbox_manager.py` 回退判定（含 errno 2 / no such file）。

### Q48. 这个项目如何支持“可验收交付”？
**参考回答：** 不是只看控制台，而是固化 evidence JSON、测试门禁和状态报表，形成可审计证据链。
**项目落点：** `docs/evidence/*` + `tests/*`。

### Q49. 如果让你做下一步生产化，你会先改什么？
**参考回答：** 我会先做三件事：1) 把资源快照改为实时 typed API；2) 把 eval 套件接入 CI；3) 逐步迁移存储到 Postgres 并补 HA。
**项目落点：** `web_console/lib/*`、`tests/*`、`telegram_store.py`。

### Q50. 你如何向业务方解释“这个系统现在是否可上生产”？
**参考回答：** 不会只给“能跑”结论，我会给 SLO 面板、失败模式、降级策略、回滚策略和验收证据。能否上生产取决于风险可接受度，而不是 demo 成功。
**项目落点：** `/status`、`degradation_states`、`docs/evidence/`、回归测试。

---

## 联网检索来源（用于题目方向与答案校准）
> 说明：以下来源用于提炼“岗位常问维度”和“行业最佳实践”；其中将这些知识映射到 Alpha-Insight 代码，是本项目内推断。

1. OpenAI Structured Outputs：
   - https://openai.com/index/introducing-structured-outputs-in-the-api/
2. OpenAI Structured Outputs Guide：
   - https://platform.openai.com/docs/guides/structured-outputs/function-calling-vs-response-format
3. OpenAI Prompt Engineering Best Practices：
   - https://help.openai.com/en/articles/6654000-best-practices-for-prompt-engineering-with-the-openai-api%255E.pdf
4. OpenAI Evaluation Best Practices：
   - https://platform.openai.com/docs/guides/evaluation-best-practices
5. OWASP Top 10 for LLM Applications：
   - https://owasp.org/www-project-top-10-for-large-language-model-applications/
6. Anthropic Prompt Injection Mitigation：
   - https://docs.anthropic.com/en/docs/test-and-evaluate/strengthen-guardrails/mitigate-jailbreaks
7. Telegram Bot API（getUpdates / editMessageText / sendChatAction）：
   - https://core.telegram.org/bots/api
8. FastAPI Deployment / Workers：
   - https://fastapi.tiangolo.com/deployment/server-workers/
9. FastAPI Background Tasks：
   - https://fastapi.tiangolo.com/tutorial/background-tasks/
10. Streamlit Session State：
    - https://docs.streamlit.io/develop/api-reference/caching-and-state/st.session_state
11. Pydantic Models & Validation：
    - https://docs.pydantic.dev/latest/concepts/models/
12. 公开 LLM 面试题库（题目分布参考）：
    - https://github.com/llmgenai/LLMInterviewQuestions
    - https://github.com/Devinterview-io/llms-interview-questions

## 项目内主要依据文件
- `agents/workflow_engine.py`
- `agents/planner_engine.py`
- `agents/telegram_command_router.py`
- `agents/telegram_nlu_planner.py`
- `services/telegram_gateway.py`
- `services/telegram_actions.py`
- `services/telegram_store.py`
- `services/scheduler.py`
- `services/watch_executor.py`
- `services/reliability_governor.py`
- `services/market_pulse.py`
- `ui/llm_frontend.py`
- `web_console/lib/resources.ts`
- `README.md`
