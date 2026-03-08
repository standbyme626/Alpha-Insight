# Upgrade11 执行合并计划（Merged Plan）

更新时间：2026-03-08  
来源文档：
- `docs/upgrade10_unfinished_items_summary.md`
- `升级11.md`

## 1. 目的

把“升级10未完成项”与“升级11彻底收口目标”合并为一份可直接执行的计划，避免重复解读与口径漂移。

## 2. 统一判断

当前项目状态：
- 已具备：Resource API 主链路、typed contract、准实时轮询、smoke 基础、运维/协议文档基础。
- 主要缺口：Phase2/3（协议拆层、编排拆层、统一 Action 入口）、前端 `monitors` 资源闭环、同参同构门禁与回滚验证门禁。

一句话：**升级11 = 在不破坏 Telegram 主链路前提下，完成“统一入口 + 统一协议 + 统一前端覆盖 + 统一门禁 + 统一文档口径”的最终收口。**

## 3. P0 必做项（先清主风险）

### P0-1 前端 monitors 资源闭环

目标：让前端与后端资源面一致。  
实施项：
- 新增 `web_console/app/api/resources/monitors/route.ts`
- 新增或补齐 dashboard monitors 页面/模块
- 接入现有轮询层（focus=3s, blur=15s, hidden pause）

DoD：
- 控制台可展示 monitors 列表与核心状态字段（`job_id/symbol/strategy_tier/enabled/next_run_at`）。
- monitors 页面行为与 runs/alerts/governance 一致。

### P0-2 统一 Action Service 入口

目标：收口 Web/Telegram 写操作路径。  
实施项：
- 落地 `services/action_service.py`
- Web 写操作通过 Action Service 复用 Telegram Action 能力

DoD：
- Web/Telegram 同参输入时，核心行为与状态写入语义一致。
- 不破坏现有 Telegram 命令链路。

### P0-3 Web vs Telegram 同参同构门禁

目标：把“同参同构”从口号变成自动化验证。  
实施项：
- 新增专项测试（建议 `tests/test_upgrade11_action_consistency.py`）
- 覆盖至少 analyze / monitor / route / pref 等关键动作

DoD：
- CI 可直接判定是否出现 Web/Telegram 语义漂移。

### P0-4 运行时协议拆层骨架

目标：补齐升级10 Phase2 的核心缺失文件。  
实施项：
- `core/sandbox_runtime.py`
- `core/execution_result.py`
- `core/contracts.py`

DoD：
- Policy 与 Runtime 职责边界清晰。
- ExecutionResult 标准字段有唯一定义与引用来源。

### P0-5 编排拆层骨架

目标：降低 `workflow_engine` 单点复杂度。  
实施项：
- `agents/workflow_graph.py`
- `agents/workflow_nodes.py`
- `agents/workflow_result_builder.py`
- `agents/workflow_governance_hooks.py`

DoD：
- 主编排职责由单文件迁移为分层模块。
- 节点契约与结果组装路径可独立测试。

## 4. P1 收口项（完成工程化闭环）

### P1-1 前端 contract 文件形态对齐

目标：把已实现能力对齐到规范结构。  
实施项：
- 补 `web_console/lib/parsers.ts`
- 补 `web_console/lib/type_guards.ts`
- 将 `contracts.ts` 中解析逻辑按职责拆分

DoD：
- `contracts/parsers/type_guards` 三层职责清晰。
- 字段缺失/新增时降级可控，页面不白屏。

### P1-2 Store 策略与部署口径关闭

目标：解决 SQLite/Postgres/StoreAdapter 角色模糊。  
实施项：
- README + 运维文档明确“当前正式支持路径”和“未来/预留路径”
- 给出切换条件或禁止条件

DoD：
- 不再出现“部署层、应用层、展示层各说各话”。

### P1-3 回滚门禁补齐

目标：验证双轨可回滚不是纸面要求。  
实施项：
- API 不可用时前端降级/快照 fallback 行为测试
- 旧入口可用性与回退条件验证（含 8501/8502/8503 口径）

DoD：
- 回滚触发条件、执行步骤、成功判据可自动化验证。

### P1-4 仓库治理收口

目标：清理工程卫生与误导信号。  
实施项：
- 保留 `.env.example`，避免敏感配置混入公开树
- README 中统一配置说明与安全提示

DoD：
- 仓库根目录不再出现明显治理噪声项。

## 5. 统一阶段执行顺序

1. Phase A（P0-1 ~ P0-3）：前端资源覆盖 + 统一动作入口 + 同构门禁。  
2. Phase B（P0-4 ~ P0-5）：运行时协议与编排拆层骨架。  
3. Phase C（P1-1 ~ P1-4）：文件形态、部署口径、回滚门禁、仓库治理收口。

## 6. 统一验收标准（升级11完成判据）

必须同时满足：
- 前端完整消费六类资源：`runs/alerts/governance/monitors/events/evidence`。
- README 与代码实现一致：API-first 主链路，快照仅 fallback/证据用途。
- 控制台具备准实时体验，关键事件具备事件流或等价实时更新。
- Web/Telegram 同参同构有自动化门禁。
- Store 策略与部署口径明确。
- 主备链路与回滚条件可验证。
- Telegram 主链路零回归。

## 7. 执行约束

- 每个阶段单独提交。
- 先 contract/test，再实现。
- 若阶段阻塞，优先保证主链路稳定与文档真实，禁止半替换主链路。
- 所有新增能力都必须附：变更清单、DoD 自查、测试结果、风险与回滚点。
