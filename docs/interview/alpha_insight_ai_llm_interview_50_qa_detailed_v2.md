# Alpha-Insight 面试题库（详细版，50题）

> 版本：2026-03-01（Detailed v2）  
> 目标岗位：AI Engineer / LLM 应用工程师 / Agent Workflow Engineer  
> 生成方法：
> 1) 遍历本项目关键代码路径（网关、动作层、调度、治理、存储、前端）
> 2) 联网检索主流 AI/LLM 工程面试方向（安全、评估、系统设计、Agent）
> 3) 将通用问题映射到 Alpha-Insight 的“可举证实现”

## 使用方式
- 每题包含：`问题`、`详细回答`、`追问延展`、`项目举证点`。
- 你可以先背“详细回答”的主线，再根据“追问延展”做现场扩展。
- “项目举证点”是你在面试里最有说服力的部分：不空谈，直接落到代码和表结构。

---

## A. AI/LLM 基础与工程方法（1-15）

### Q1. LLM 应用工程和传统后端工程最大的差异是什么？
**详细回答：** 传统后端主要处理确定性逻辑，输入相同输出基本一致；LLM 应用工程的核心是管理不确定性。我们不只是“调模型 API”，而是把模型放进可控工作流，增加输出约束、回退策略、状态机、评估与审计。一个成熟的 LLM 系统必须回答三个问题：结果是否可解释、失败是否可恢复、风险是否可治理。没有这三点，系统只能算 demo。
**追问延展：** 为什么要把模型能力“流程化”？因为生产目标是稳定交付，而不是单次漂亮回答。
**项目举证点：** `agents/workflow_engine.py`（编排）、`agents/planner_engine.py`（远程失败回退）、`services/reliability_governor.py`（降级治理）。

### Q2. 你在生产中怎么设置 temperature / top_p 这类采样参数？
**详细回答：** 面向规划、告警、结构化摘要这类任务，我会默认低温（0~0.2）来提高可复现性；创意文本才提高温度。参数必须是可配置而非写死，并且要支持运行时调整和持久化。这样出现质量波动时可以快速回滚参数，而不用发版。
**追问延展：** 如果老板要求“更灵活更像人”，你会怎么做？答：局部场景提高温度，不动核心风险链路参数。
**项目举证点：** `.env` + `ui/llm_frontend.py` Runtime Config（支持应用并写入 `.env`）。

### Q3. 为什么你强调结构化输出，而不是纯自然语言回答？
**详细回答：** 因为纯文本在下游不可编程：难校验、难统计、难回归。结构化输出可以让系统以字段为单位做校验和展示，比如涨跌幅、样本数、新闻来源、降级状态，这些都可以被测试、监控和审计。结构化不是为了“好看”，而是为了稳定工程化。
**追问延展：** 结构化字段变化怎么办？答：版本化 schema + 向后兼容解析。
**项目举证点：** `services/telegram_actions.py::_build_analysis_contract`，`analysis_reports.key_metrics` 存档。

### Q4. function calling 和 JSON schema 输出怎么选？
**详细回答：** 判断标准是是否触发副作用。如果模型要驱动动作（发消息、改任务、下游 API），优先 function/tool calling；如果只用于展示或报告，JSON schema 更简单稳定。两者都需要后置校验，不能盲信模型。
**追问延展：** 什么时候两者混用？答：先 function 调数据，再 schema 输出展示对象。
**项目举证点：** 本项目偏“工作流动作层”模式，业务动作集中在 `TelegramActions`。

### Q5. 什么是 Prompt Injection，你如何做分层防护？
**详细回答：** Prompt Injection 是通过恶意输入篡改模型行为，本质是越权控制。防护不能只靠系统提示词，必须分层：输入检测、动作白名单、最小权限、输出校验、审计追踪。即使检测漏掉，也要由权限层兜底，保证坏输出无法直接执行高风险动作。
**追问延展：** 你如何验证防护有效？答：构造红队样本做持续回归。
**项目举证点：** `agents/telegram_nlu_planner.py::detect_prompt_injection_risk` + `services/telegram_gateway.py` 审计流程。

### Q6. 你如何降低 hallucination？
**详细回答：** 我用“证据先行”策略：先取结构化数据（行情/新闻），再生成结论；结论必须带证据字段；缺证据时明确降级而不是猜测。并且把“可解释输出”作为产品契约，而不是可选项。
**追问延展：** 如果用户坚持要结论？答：给置信区间和缺失原因，不给伪确定答案。
**项目举证点：** Card C 三件套（行情源、新闻覆盖、指标口径）在 `telegram_actions.py`。

### Q7. RAG 系统最容易被忽视的瓶颈是什么？
**详细回答：** 多数场景瓶颈在 retrieval，不在 generation。召回不准、排序不稳、切块策略不合理，会让上层模型“无米下锅”。所以我会先把检索指标打稳，再优化模型回答风格。
**追问延展：** 检索和生成谁先优化？答：先检索后生成。
**项目举证点：** `tools/news_data.py`（多源拉取/去重）+ `services/news_digest.py`（主题归并）。

### Q8. 评估（Eval）怎么落地，不流于“感觉不错”？
**详细回答：** 做 eval-driven development：先定义成功标准，再沉淀数据集，再自动跑评分并结合人工抽样。评估要覆盖正常样本、边界样本、对抗样本。上线后持续追加失败样本，避免“老题高分，新题翻车”。
**追问延展：** 自动评分和人工冲突怎么办？答：以业务风险为准，校准评分器。
**项目举证点：** `docs/evidence/*` + `tests/*` 可作为基线评测资产。

### Q9. 什么时候该用 Agent，什么时候不用？
**详细回答：** 任务可预测、流程固定、审计要求高时，优先 workflow；探索性高、步骤不固定时再考虑 agent。生产里常见最佳实践是“workflow 主干 + agent 局部增强”。
**追问延展：** 你如何避免 agent 失控？答：动作白名单、预算上限、人工确认点。
**项目举证点：** 主链路明确是 workflow（planner -> data -> coder -> executor）。

### Q10. 你怎么平衡“回答质量、延迟、成本”？
**详细回答：** 先定义分层目标：核心链路要稳（低延迟、可回退），增强链路可降级（例如图表、扩展新闻）。通过小模型优先、缓存、上下文裁剪、异步执行和失败快退来控制成本。关键是“质量预算”和“错误预算”要可观测。
**追问延展：** 什么时候宁可贵一点？答：高风险决策节点。
**项目举证点：** `runtime_controls.py`、`watch_executor.py`、`reliability_governor.py`。

### Q11. 为什么“提示词优化”不能替代工程治理？
**详细回答：** 提示词只能提升概率，无法提供系统性保证。生产稳定性来自状态机、重试、去重、降级、审计，而不是“更聪明的 prompt”。好的 prompt 是必要条件，不是充分条件。
**追问延展：** 什么时候 prompt 仍然关键？答：输出风格、约束表达、工具选择策略。
**项目举证点：** `telegram_store.py` 的多表状态机 + `final_message_dispatches` 去重。

### Q12. 输出安全（Insecure Output Handling）你怎么做？
**详细回答：** 把模型输出当不可信输入处理：必须校验、清洗、限制执行范围，禁止直连高权限后端动作。尤其是带 URL、命令、HTML 片段的输出，必须经过白名单和转义。
**追问延展：** 为什么这不是“前端问题”？答：后端执行链同样有风险。
**项目举证点：** `services/news_digest.py::redact_user_visible_payload`，动作层文案净化。

### Q13. 线上故障定位的第一步是什么？
**详细回答：** 看 SLI 而不是先看代码。优先检查成功率、延迟、重试队列、DLQ、活跃降级状态，再定位是外部依赖问题还是内部逻辑问题。这样能快速判断“是否需要先止血再修复”。
**追问延展：** 什么叫“止血”？答：触发 summary/degrade 保核心可用。
**项目举证点：** `/status` 输出来自 `TelegramActions.handle_status`。

### Q14. 你如何定义“可解释输出”？
**详细回答：** 至少要有结论、证据、口径、动作建议四层。没有证据的结论在投研场景不可接受。可解释不是“解释得长”，而是“可以被复核和追踪”。
**追问延展：** 用户只要结论怎么办？答：默认简版，但保留证据入口。
**项目举证点：** Card A/B/C/D 结构契约。

### Q15. 你怎么做“失败可恢复”设计？
**详细回答：** 失败分可重试与不可重试；可重试走退避队列，不可重试明确入 DLQ；用户侧要可见状态。并且把超时请求放入恢复队列，避免用户以为“彻底失败”。
**追问延展：** 恢复后如何避免重复通知？答：claim+mark 去重。
**项目举证点：** `analysis_recovery_queue`、`process_due_analysis_recovery`、`final_message_dispatches`。

---

## B. LLM 应用系统设计与可靠性（16-30）

### Q16. Telegram 机器人为什么要网关和调度器解耦？
**详细回答：** 网关负责接入和持久化，调度器负责执行与重试。解耦后可以把“流量尖峰”和“执行负载”隔离，避免接入层被耗时任务阻塞。这样扩展时也能独立扩容网关和 worker。
**追问延展：** 不解耦会怎样？答：延迟飙升、超时、重复消费。
**项目举证点：** `telegram_long_polling_gateway.py` / `telegram_webhook_gateway.py` + `telegram_watch_scheduler.py`。

### Q17. long polling 和 webhook 在你项目里怎么定位？
**详细回答：** long polling 适合开发和受限网络环境，部署简单；webhook 更适合生产，延迟更低，且可接入网关安全策略（secret token、IP 白名单）。两者并存可以提高交付灵活性。
**追问延展：** 切换成本如何控制？答：统一动作层接口，不改业务逻辑。
**项目举证点：** 双入口脚本都已存在。

### Q18. 幂等与去重的关键实现是什么？
**详细回答：** 不依赖“消息平台不会重复”，而是应用侧强制幂等：发送前 claim，发送后 mark；重复请求直接抑制。进度消息和最终消息分开去重，避免互相干扰。
**追问延展：** 为什么要两张表？答：生命周期不同、冲突策略不同。
**项目举证点：** `request_progress_messages` + `final_message_dispatches`。

### Q19. 重试策略如何区分“该重试”和“不该重试”？
**详细回答：** 网络抖动、限流类错误可重试；参数错误、权限错误通常不可重试。重试要有上限和退避，超限后进入 DLQ，防止无限循环消耗资源。
**追问延展：** 快速重试何时开启？答：高优先级 fast lane。
**项目举证点：** `watch_executor.py` 的 retry/backoff/DLQ 与 immediate retry 逻辑。

### Q20. 降级策略的触发条件如何定义？
**详细回答：** 不能拍脑袋，应由 SLO 指标触发：成功率阈值、p95 延迟阈值、DLQ 趋势。触发后保留核心能力，关闭或降级次要能力；恢复也要自动化并写事件。
**追问延展：** 为什么要“趋势”而不是瞬时值？答：避免抖动误触发。
**项目举证点：** `reliability_governor.py::_dlq_is_rising` + `_reconcile_state`。

### Q21. route strategy 与 channel adapter 是如何解耦的？
**详细回答：** route strategy 决定“走哪些通道”，adapter 负责“怎么发”。这样新增通道不需要重写策略引擎，只需要新增适配器。
**追问延展：** 多通道一致性如何保证？答：统一 payload 合约 + 每通道状态记录。
**项目举证点：** `_resolve_routes` + `notification_channels.py`。

### Q22. strategy tier 的业务意义是什么？
**详细回答：** 它把“能力边界”显式化：research-only 只研判，alert-only 可告警，execution-ready 可进入执行级动作。这个分层是把风控前置到系统设计里，而不是事后补救。
**追问延展：** 为什么不是一个 bool？答：三档更贴合风险梯度。
**项目举证点：** `core/strategy_tier.py` + `watch_executor` tier guard。

### Q23. 为什么要保留 audit_events？
**详细回答：** LLM 系统涉及大量动态决策，没有审计就无法复盘“为什么这样做”。审计是排障、合规、和跨团队沟通的共同语言。
**追问延展：** 审计会不会太重？答：关键节点审计，非关键抽样。
**项目举证点：** `telegram_store.py` 的 `audit_events` 表。

### Q24. 你如何做限流与并发控制？
**详细回答：** 双层控制：会话级速率限制防滥用，全局并发门防资源雪崩。对高成本动作（分析、图表）额外设超时和并发上限。
**追问延展：** 限流拒绝时用户体验怎么做？答：返回可操作提示和重试建议。
**项目举证点：** `RuntimeLimits` + `GlobalConcurrencyGate`。

### Q25. 为什么进度消息要 edit 而不是连发多条？
**详细回答：** 连发会造成信息噪音和消息轰炸，且增加平台风控压力。edit 进度能减少消息量，用户只关注最后结果。
**追问延展：** edit 失败怎么办？答：回退到发送新消息并记录失败指标。
**项目举证点：** `send_analysis_progress` + `tools/telegram.py::edit_message_text`。

### Q26. 你如何设计配置层分层与可追溯？
**详细回答：** 使用 base、env override、runtime flags 三层合并，并输出 source trace 与 diff summary。这样线上“配置为什么变了”可定位到具体层和具体键。
**追问延展：** 怎么防止脏配置？答：schema 校验 + forbid extra。
**项目举证点：** `core/runtime_config.py`。

### Q27. 对于“外部依赖波动”，你的工程策略是什么？
**详细回答：** 先隔离影响范围，再降级核心链路，保住最低可用，再做恢复。外部调用全部做超时、重试和降级分支，不能让依赖抖动拖垮主流程。
**追问延展：** 怎么验证策略有效？答：故障注入 + 回归门禁。
**项目举证点：** `connectors.py`、`reliability_governor.py`、`docs/evidence/*`。

### Q28. 你会如何定义“可运营化的 LLM 系统”？
**详细回答：** 可运营化意味着：有状态观测、有失败分类、有恢复路径、有指标看板、有审计闭环。没有这些，系统只能算“可演示”，不能算“可运维”。
**追问延展：** 你会先补哪块？答：SLI 和告警阈值。
**项目举证点：** `metric_events`、`degradation_states`、`/status` 汇总。

### Q29. Market Pulse 的作用是什么？
**详细回答：** 它是面向运营的摘要层，把市场变化与系统健康合并播报，帮助管理者快速判断“市场是否异常 + 系统是否可靠”。
**追问延展：** 它和告警有什么区别？答：告警是事件，pulse 是周期摘要。
**项目举证点：** `services/market_pulse.py::publish_due`。

### Q30. SQLite 迁移到 Postgres 你会怎么做？
**详细回答：** 先抽象存储接口，再做双写/回放验证，最后切读并保留回滚开关。迁移期间重点验证幂等键、索引和时序字段的一致性。
**追问延展：** 最大风险点？答：状态机并发冲突与唯一约束差异。
**项目举证点：** 当前 `TelegramTaskStore`（SQLite）+ `docker-compose.telegram.yml`（Postgres 方向）。

---

## C. Alpha-Insight 深挖与实战追问（31-50）

### Q31. 你能完整讲一遍“分析请求”链路吗？
**详细回答：** 用户输入命令或 NL 后，网关做访问控制、命令/NLU 解析、入库；动作层发送进度并触发统一研究；研究结果进入卡片契约生成，再写报告与状态，最后发送最终结果并做幂等标记。这个链路关键点是“全程可追踪 + 结果可去重”。
**追问延展：** 哪一步最易故障？答：外部数据和通知分发。
**项目举证点：** `telegram_gateway.py`、`telegram_actions.py`、`workflow_engine.py`。

### Q32. “Planner is using local fallback”出现时你怎么定位？
**详细回答：** 先确认当前进程是否拿到 `OPENAI_API_KEY/API_BASE/MODEL_NAME`；再验证远程调用返回是否非 2xx 或解析失败；最后确认 fallback 是否开启。必要时暂时关 fallback，让错误显性化。
**追问延展：** 为什么不一直关 fallback？答：生产可用性会下降。
**项目举证点：** `planner_engine.py::_load_config/_call_remote_planner/plan_tasks`。

### Q33. Card A 出现 N/A 的根因通常有哪些？
**详细回答：** 三类：只有 close 无 OHLC 序列；字段映射对不上；窗口对齐后样本不足。正确处理是“清晰降级说明 + 自动重试”，而不是输出伪结论。
**追问延展：** 如何监控该问题？答：记录 card_a_ready 比例。
**项目举证点：** `_compute_window_metrics_from_records`。

### Q34. 技术一句话如何避免“模板话术”？
**详细回答：** 必须强制包含可执行价位：MA10/MA20、支撑/压力、触发条件；并根据样本量附置信度提示。这样用户能直接据此观察，不是情绪化描述。
**追问延展：** 指标冲突时如何说？答：明确短中期分歧与触发条件。
**项目举证点：** `_technical_sentence_with_levels`。

### Q35. 新闻主题化里“可核验”怎么落地？
**详细回答：** 每个主题必须给代表新闻，且包含标题、媒体、时间、链接；情绪评分必须给样本数和方法。这样避免“神秘评分”不可复查。
**追问延展：** 媒体字段缺失怎么办？答：至少回落域名。
**项目举证点：** `TopNewsItem/ThemeDigestItem` 与 `_news_theme_lines`。

### Q36. 为什么要设置情绪样本门槛（N<5 不给分）？
**详细回答：** 小样本下情绪分方差很大，给分会制造伪精确。业务上应明确“样本不足，不计算”。
**追问延展：** 如果业务强制要分？答：给低置信提示并单独标注。
**项目举证点：** `TelegramActions._news_theme_lines`。

### Q37. 监控任务与通知状态为什么要拆表？
**详细回答：** 任务生命周期和通知生命周期不同，拆表可独立扩展重试、DLQ、抑制策略。单表会导致状态耦合、查询复杂、审计困难。
**追问延展：** 你会加哪些索引？答：next_run_at、state+next_retry_at。
**项目举证点：** `telegram_store.py` 建表与索引。

### Q38. route_strategy + strategy_tier 的组合价值是什么？
**详细回答：** route_strategy 解决“通知路由”，strategy_tier 解决“行为权限”。两者正交，组合后可实现细粒度风控和运营策略。
**追问延展：** 典型组合示例？答：research-only + telegram_only。
**项目举证点：** `watch_jobs` 字段与执行路径。

### Q39. 降级状态恢复为什么一定要事件化？
**详细回答：** 因为恢复不是“看起来好了”，而是可追踪事实。事件化能支持 RCA、SLA 复盘、和自动告警静默窗口管理。
**追问延展：** 你会记录哪些字段？答：triggered_at、recovered_at、reason。
**项目举证点：** `degradation_states/degradation_events`。

### Q40. fast lane immediate retry 的业务收益是什么？
**详细回答：** 对关键告警，立即重试能显著降低瞬态失败带来的漏告警概率，尤其在网络抖动场景。它是“低成本高收益”的可靠性优化。
**追问延展：** 风险是什么？答：短时放大流量，要有预算上限。
**项目举证点：** `watch_executor.py` fast lane retry 计数。

### Q41. `/status` 你会怎么向面试官解释？
**详细回答：** 它是运维入口，不是展示页。核心看板包含成功率、retry 队列、DLQ、活跃降级和恢复事件，用来快速判断系统健康与投递质量。
**追问延展：** 为什么把这些做成命令？答：提升值班效率。
**项目举证点：** `handle_status` 文本构成。

### Q42. 群聊安全你做了哪些控制？
**详细回答：** 访问控制（allowlist/blacklist）、命令白名单、灰度开关、会话级限流。目标是让误操作和滥用都在入口被拦截，而不是下游补救。
**追问延展：** 为什么入口拦截优先？答：成本最低、影响最小。
**项目举证点：** `TelegramGateway` 初始化参数。

### Q43. NLU 误判怎么避免“错误执行”？
**详细回答：** 对低置信请求先澄清槽位，不直接执行。系统要有 pending 状态和候选选择机制，保证“先确认，再动作”。
**追问延展：** 哪些槽位最关键？答：symbol、interval、template。
**项目举证点：** `extract_clarify_slots` + `clarify_pending`/`pending_candidate_selection`。

### Q44. 你如何解释“前后端双栈并存”？
**详细回答：** Streamlit 服务运维和快速迭代，Next.js 服务资源化页面与可扩展治理。双栈不是重复建设，而是满足不同使用者角色。
**追问延展：** 将来会不会统一？答：可逐步统一到 typed API 后评估。
**项目举证点：** `ui/*` + `web_console/*`。

### Q45. Next 控制台为什么当前读 JSON 快照？
**详细回答：** 快照模式适合验收与演示，能快速提供稳定视图。代价是实时性不足，因此下一步应升级为实时 typed API。
**追问延展：** 迁移优先级如何排？答：先 runs/alerts，再 governance/evidence。
**项目举证点：** `web_console/lib/resources.ts`。

### Q46. 你最近做的“可运维改动”有什么价值？
**详细回答：** 前端 Runtime Config 增加“写入 `.env`”，让运行参数可通过界面落盘，减少 SSH 手工改配置导致的错误和漂移。
**追问延展：** 安全上要注意什么？答：密钥展示脱敏，写入键白名单。
**项目举证点：** `ui/llm_frontend.py` 新增持久化逻辑。

### Q47. 沙箱故障的经典问题你怎么答？
**详细回答：** 最常见是 Docker 不可用（binary 缺失或权限问题）。好的系统应自动回退本地执行并记录原因，不应直接让用户请求失败。
**追问延展：** 怎么让问题可见？答：在状态和日志中暴露 degrade reason。
**项目举证点：** `core/sandbox_manager.py` fallback 判定包含 errno2/no such file。

### Q48. 你如何证明系统“不是拍脑袋可用”？
**详细回答：** 用证据链证明：测试结果、运行报表、状态快照、故障注入与回归门禁。可验收交付不是一句“我测过了”，而是可重复复核。
**追问延展：** 你会给老板看什么？答：关键 SLI 趋势 + 风险清单。
**项目举证点：** `docs/evidence/*` + `tests/*`。

### Q49. 若继续生产化，你前三个技术动作是什么？
**详细回答：** 1) Next 前端改实时 typed API；2) 建 eval CI 门禁（包括对抗样本）；3) 存储逐步迁移 Postgres 并做多实例一致性测试。这样可以同时提升实时性、质量稳定性和扩展性。
**追问延展：** 为什么这三件优先？答：直接影响线上可靠性与团队效率。
**项目举证点：** `web_console/lib/*`、`tests/*`、`telegram_store.py`。

### Q50. 你会如何回答“现在能上生产吗”？
**详细回答：** 我不会给二元答案，而是给条件化结论：在当前流量、SLO 目标、容错策略下是否满足上线门槛。并明确已知风险、降级路径、回滚策略和观测指标。生产决策是风险管理问题，不是功能演示问题。
**追问延展：** 面试官追问“你敢背锅吗”？答：我敢背可观测、可回滚、可追责的系统。
**项目举证点：** `/status`、`degradation_states`、`docs/evidence/`、回归测试。

---

## 联网检索来源（用于题目维度与回答校准）

1. OpenAI Evaluation Best Practices  
https://platform.openai.com/docs/guides/evaluation-best-practices

2. OpenAI Structured Outputs（function calling vs response format）  
https://platform.openai.com/docs/guides/structured-outputs/function-calling-vs-response-format

3. OpenAI Structured Outputs 介绍  
https://openai.com/index/introducing-structured-outputs-in-the-api/

4. OWASP Top 10 for LLM Applications  
https://owasp.org/www-project-top-10-for-large-language-model-applications/

5. OWASP GenAI Security Project（LLM01 Prompt Injection）  
https://genai.owasp.org/llmrisk/llm01-prompt-injection/

6. Anthropic Prompt Injection/Jailbreak Mitigation  
https://docs.anthropic.com/en/docs/test-and-evaluate/strengthen-guardrails/mitigate-jailbreaks

7. Telegram Bot API  
https://core.telegram.org/bots/api

8. Streamlit Session State  
https://docs.streamlit.io/develop/api-reference/caching-and-state/st.session_state

9. Pydantic Models  
https://docs.pydantic.dev/latest/concepts/models/

10. 面试题分布参考（公开仓库）  
https://github.com/llmgenai/LLMInterviewQuestions  
https://github.com/Devinterview-io/llms-interview-questions

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
