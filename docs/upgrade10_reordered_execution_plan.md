# Upgrade10 重排执行清单（按仓库现状）

更新时间：2026-03-08  
适用范围：`/home/kkk/Project/Alpha-Insight`

## 1. 文档目的

把 `升级10.md` 从“目标规格”重排为“可直接改代码的文件级执行清单”，用于按阶段实施与验收。

## 2. 执行协作约定

- MCP 负责找和压缩：先 `list/search/read excerpt` 缩小范围，避免全仓大段读取。
- Codex 负责改和验证：基于候选文件做最小改动，运行测试并给出证据。
- 默认顺序：先冻结 contract，再替换主数据链路，再做实时化与门禁补齐。

## 3. 当前现状摘要（已核对）

- 前端主链路仍依赖 `docs/evidence/upgrade7_frontend_resources.json` 快照。
- `ExecutionResult / ToolResult / Node Contract` 已有基础实现与测试。
- `docker-compose.telegram.yml` 有 Postgres，但 Python 主存储链路仍是 SQLite。
- `services/resource_api.py / store_adapter.py / run_store.py / artifact_store.py` 等文件尚不存在。
- `tests/smoke/`、`deploy/`、`docs/configuration_manual.md` 等 Phase4 产物尚不存在。

## 4. 重排后的执行顺序

## T0 契约冻结（先立规矩）

目标：冻结前后端契约与能力映射，避免边改边漂。  
文件：
- `web_console/lib/contracts.ts`
- `web_console/lib/types.ts`
- `core/node_contracts.py`
- `core/tool_result.py`
- `docs/upgrade10_capability_matrix.md`（新增）

DoD：
- 字段命名、枚举、错误码和兼容策略书面冻结。
- 前后端契约映射表覆盖 runs/alerts/governance/evidence 四类资源。

## T1 建立实时资源 API（最高优先）

目标：把前端主数据源从离线快照切到在线读取。  
文件：
- `services/resource_api.py`（新增）
- `services/governance_read_model.py`（新增）
- `services/events_read_model.py`（新增）
- `services/telegram_store.py`（复用只读查询能力）

DoD：
- 可返回 runs/alerts/governance/evidence/events 的在线数据。
- 具备明确错误响应与空数据响应。

## T2 改造 Web Console BFF（替换快照主链）

目标：`/api/resources/*` 不再读 JSON 文件。  
文件：
- `web_console/lib/resources.ts`
- `web_console/app/api/resources/runs/route.ts`
- `web_console/app/api/resources/alerts/route.ts`
- `web_console/app/api/resources/governance/route.ts`
- `web_console/app/api/resources/evidence/route.ts`

DoD：
- 前端可在不执行 `scripts/upgrade7_frontend_resources_export.py` 的情况下显示主数据。
- 快照文件仅保留为兼容与证据用途。

## T3 前端准实时（轮询层）

目标：先达成准实时（2-5 秒），再做事件流。  
文件：
- `web_console/lib/polling.ts`（新增）
- `web_console/lib/realtime.ts`（新增）
- `web_console/app/(dashboard)/runs/page.tsx`
- `web_console/app/(dashboard)/alerts/page.tsx`
- `web_console/app/(dashboard)/governance/page.tsx`

DoD：
- 页面聚焦时 2-5 秒自动刷新；失焦降频；隐藏暂停；支持手动刷新。
- 关键字段 `strategy_tier / tier_guarded / degrade-recover` 连续可见。

## T4 事件时间线接口（实时补强）

目标：时间线不再依赖页面静态拼装。  
文件：
- `services/events_read_model.py`（承接）
- `web_console/app/api/resources/events/route.ts`（新增）
- `web_console/components/governance-panel.tsx`
- `web_console/components/alerts-panel.tsx`

DoD：
- 可返回时间线事件（至少覆盖 degrade/recover/failure/guard）。
- 时间线可独立刷新。

## T5 存储抽象收口（RunStore/ArtifactStore）

目标：为后续 Postgres 对齐准备适配层，不直接散落表查询。  
文件：
- `services/store_adapter.py`（新增）
- `services/run_store.py`（新增）
- `services/artifact_store.py`（新增）
- `ui/typed_resource_client.py`（降级为兼容/导出用途）

DoD：
- 资源读取统一经 store 层出口。
- 保留 SQLite 路径兼容，不破坏 Telegram 主链路。

## T6 导出脚本降级为兼容工具

目标：快照导出保留，但不再是主运行依赖。  
文件：
- `scripts/upgrade7_frontend_resources_export.py`
- `web_console/README.md`
- `README.md`

DoD：
- 文档明确“导出=证据/兼容，不是主数据源”。
- 启动流程不再要求先导出才能用控制台。

## T7 测试门禁补齐（smoke + 一致性）

目标：补齐回归最短路径，防止重构回退。  
文件：
- `tests/smoke/test_webhook_smoke.py`（新增）
- `tests/smoke/test_scheduler_smoke.py`（新增）
- `tests/smoke/test_market_pulse_smoke.py`（新增）
- `tests/test_upgrade7_frontend_client.py`（调整断言到实时链路）

DoD：
- `pytest -q` 可跑。
- 新增 smoke 用例可重复执行。

## T8 运维与协议文档（Phase4 必需）

目标：收敛 README Unknown 项。  
文件：
- `docs/configuration_manual.md`（新增）
- `docs/webhook_contract.md`（新增）
- `docs/compliance.md`（新增）
- `deploy/systemd/`（新增）

DoD：
- 配置键、Webhook 协议、合规边界和托管策略可审查、可复核。

## 5. 阶段验收建议

- 每阶段提交前输出：变更文件清单、测试结果、风险点、回滚点。
- 强制不变规则：不破坏 Telegram 主链路；不绕过 contract；不新增无约束 schema。

## 6. 下一步（建议起始）

建议从 `T0 + T1` 一起开始，先冻结契约并建立实时 API，再进入前端切链路。  
这是当前仓库“收益最大且风险可控”的切入点。
