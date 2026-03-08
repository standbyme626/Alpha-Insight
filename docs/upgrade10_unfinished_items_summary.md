# Upgrade10 未完成内容汇总

更新时间：2026-03-08
依据文档：`升级10.md`（第 9/11/12/14/16 章）

> 本文档只汇总“未完成或待验证”项，不重复已完成内容。

## 1. 总览

当前状态可归纳为：
- Phase 0/1/4 多数目标已落地；
- Phase 2/3 仍有关键缺口；
- 第 14 章“最终验收标准”未全部满足。

## 2. 未完成项清单（按优先级）

| 优先级 | 对应章节 | 未完成项 | 当前证据 | 影响 |
| --- | --- | --- | --- | --- |
| P0 | 第9章 Phase 2 | 运行时协议分层文件未落地：`core/sandbox_runtime.py`、`core/execution_result.py`、`core/contracts.py` | 仓库缺失上述文件（存在 `core/sandbox_policy.py`、`core/tool_result.py`） | Policy/Runtime 分层与结果标准化仍未按规格闭环 |
| P0 | 第9章 Phase 2 | 编排拆层文件未落地：`agents/workflow_graph.py`、`agents/workflow_nodes.py`、`agents/workflow_result_builder.py`、`agents/workflow_governance_hooks.py` | 仍以 `agents/workflow_engine.py` 为核心；目标拆层文件不存在 | 大编排职责未按目标拆分，后续演进风险高 |
| P0 | 第9章 Phase 3 / 第14章 | 统一 Action 层未落地：`services/action_service.py` 缺失 | 仓库无该文件 | Web/Telegram 统一入口能力缺正式收口点 |
| P0 | 第14章 / 第16章 | 前端 `monitors` 资源链路未闭环（后端有，前端无） | `services/resource_api.py` 有 `/api/monitors`；但 `web_console/app/api/resources/monitors/route.ts` 与 dashboard monitors 页面不存在 | 第16章要求的 `/monitors` 实时化未达成 |
| P1 | 第9章 Phase 0 / 第16章 | `web_console/lib/type_guards.ts`、`web_console/lib/parsers.ts` 未按文档形态落地 | 这两个文件缺失；解析/守卫逻辑集中在 `web_console/lib/contracts.ts` | 能力部分已实现，但与规格文件结构不一致，降低可审查性 |
| P1 | 第11章 11.1 / 第14章 | “Web action 与 Telegram command 同参同构”专项门禁未看到独立测试 | 未检索到对应专项测试文件（已有 Telegram 相位测试与 smoke） | 最终验收关键条款缺硬证据 |
| P1 | 第12章 / 第14章 | 双轨入口（含 8503）可回滚能力缺显式验证证据 | 文档提出旧入口 8501/8502/8503 保持可回退，但未见专项回滚验证用例 | 回滚条款可操作性与可验证性不足 |

## 3. 规格-现状偏差说明（重点）

1. **Phase 2/3 是当前最大缺口**
- 已完成工作更多集中在资源 API、前端准实时、文档与 smoke 门禁。
- 规格书中要求的运行时协议拆层、编排拆层、统一 Action 层尚未形成对应代码骨架。

2. **`monitors` 在后端已可读，但前端未接入**
- `GET /api/monitors` 已在 `services/resource_api.py` 注册。
- 但 `web_console` 当前仅有 `runs/alerts/governance/evidence/events` 资源路由与页面。

3. **“解析器/守卫层”是实现存在、形态不符**
- 规格建议 `contracts + parsers + type_guards` 分文件。
- 当前集中在 `web_console/lib/contracts.ts`，能用但不满足规格拆分形态。

## 4. 建议下一步执行顺序

1. 先补 `monitors` 前端闭环（BFF route + dashboard page + 轮询接入）。
2. 立 `services/action_service.py`，把 Web/Telegram 行为映射到统一动作入口。
3. 建立“Web vs Telegram 同参同构”专项测试门禁。
4. 启动 Phase 2 骨架：`sandbox_runtime` + `execution_result` + workflow 拆层文件。
5. 把 `contracts.ts` 中解析逻辑拆到 `parsers.ts` / `type_guards.ts`（若继续遵循原规格形态）。

## 5. 备注

- 本清单是“未完成项盘点”，不是实现方案设计稿。
- 若要继续推进，可直接将本文件转为执行 backlog（按 P0/P1 创建任务并逐项关闭）。
