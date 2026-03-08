# Upgrade10 Capability Matrix (T0 Freeze)

更新时间：2026-03-08

## 范围

本矩阵用于冻结 T0 契约边界，作为 T1/T2 改造期间的对齐基线。

## 资源能力映射

| Capability | Current Source | Target API | Contract Key Fields | Status |
| --- | --- | --- | --- | --- |
| Runs list | `analysis_reports` (SQLite) via typed client | `GET /api/runs` | `run_id`, `request_id`, `symbol`, `summary`, `key_metrics`, `updated_at` | Frozen |
| Run detail | `analysis_reports` by `run_id/request_id` | `GET /api/runs/{run_id}` | `run_id`, `request_id`, `chat_id`, `key_metrics` | Frozen |
| Alerts list | `watch_events + notifications` | `GET /api/alerts` | `event_id`, `symbol`, `status`, `strategy_tier`, `tier_guarded`, `last_error` | Frozen |
| Governance | `degradation_states + metric_events` | `GET /api/governance` | `active_states`, `recovered_states`, `states[]`, `push_success_rate_24h` | Frozen |
| Monitors | `watch_jobs` | `GET /api/monitors` | `job_id`, `symbol`, `strategy_tier`, `enabled`, `next_run_at` | Frozen |
| Event timeline | `degradation_events + notifications` | `GET /api/events?since=` | `event_id`, `event_type`, `ts`, `title`, `summary`, `details` | Frozen |
| Evidence list | `docs/evidence/*.json` | `GET /api/evidence` | `name`, `path`, `generated_at`, `summary`, `updated_at` | Frozen |

## 统一版本约定

- Node contract schema version: `upgrade10.node_contract.v1`
- Tool result schema version: `upgrade10.tool_result.v1`
- Resource API schema version: `upgrade10.resource_api.v1`

## 响应信封约定

所有实时资源 API 默认返回：

```json
{
  "schema_version": "upgrade10.resource_api.v1",
  "generated_at": "2026-03-08T00:00:00+00:00",
  "data": {}
}
```

前端解析必须兼容：
- 新信封结构（推荐）
- 旧数组结构（过渡期）
