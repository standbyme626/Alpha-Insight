import type { AlertResource, DegradationStateResource, EvidenceResource, MonitorResource, RunResource } from "@/lib/types";

function statusTone(status: string): string {
  const normalized = status.toLowerCase();
  if (normalized === "delivered" || normalized === "recovered") {
    return "ok";
  }
  if (normalized.includes("retry") || normalized === "dlq" || normalized === "active") {
    return "warn";
  }
  if (normalized === "suppressed" || normalized === "failed") {
    return "danger";
  }
  return "neutral";
}

export function RunsTable({ rows }: { rows: RunResource[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>运行ID run_id</th>
          <th>标的 symbol</th>
          <th>成功 success</th>
          <th>回退 fallback</th>
          <th>重试 retry</th>
          <th>预算 budget</th>
          <th>P95延迟 p95_ms</th>
          <th>错误率 error_rate</th>
          <th>更新时间 updated_at</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const metrics = row.key_metrics || {};
          return (
            <tr key={row.run_id}>
              <td>{row.run_id}</td>
              <td>{row.symbol}</td>
              <td>{String(metrics.runtime_success ?? "")}</td>
              <td>{String(metrics.runtime_fallback_used ?? "")}</td>
              <td>{String(metrics.runtime_retry_count ?? "")}</td>
              <td>{String(metrics.runtime_budget_verdict ?? "")}</td>
              <td>{String(metrics.runtime_latency_p95_ms ?? "")}</td>
              <td>{String(metrics.runtime_error_rate ?? "")}</td>
              <td>{row.updated_at}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function AlertsTable({ rows }: { rows: AlertResource[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>事件ID event_id</th>
          <th>标的 symbol</th>
          <th>优先级 priority</th>
          <th>策略层级 strategy_tier</th>
          <th>层级守卫 tier_guarded</th>
          <th>通道 channel</th>
          <th>状态 status</th>
          <th>运行ID run_id</th>
          <th>最近错误 last_error</th>
          <th>更新时间 updated_at</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.event_id}:${row.channel}`}>
            <td>{row.event_id}</td>
            <td>{row.symbol}</td>
            <td>{row.priority}</td>
            <td>{row.strategy_tier}</td>
            <td>{row.tier_guarded ? "是" : "否"}</td>
            <td>{row.channel}</td>
            <td>
              <span className={`status-pill ${statusTone(row.status)}`}>{row.status || "未知 unknown"}</span>
            </td>
            <td>{row.run_id || ""}</td>
            <td>{row.last_error || row.suppressed_reason || ""}</td>
            <td>{row.updated_at}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function EvidenceTable({ rows }: { rows: EvidenceResource[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>名称 name</th>
          <th>生成时间 generated_at</th>
          <th>大小 size</th>
          <th>路径 path</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.name}>
            <td>{row.name}</td>
            <td>{row.generated_at || ""}</td>
            <td>{row.size_bytes}</td>
            <td>{row.path}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function GovernanceTable({ rows }: { rows: DegradationStateResource[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>状态键 state_key</th>
          <th>状态 status</th>
          <th>原因 reason</th>
          <th>触发时间 triggered_at</th>
          <th>恢复时间 recovered_at</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.state_key}>
            <td>{row.state_key}</td>
            <td>
              <span className={`status-pill ${statusTone(row.status)}`}>{row.status}</span>
            </td>
            <td>{row.reason}</td>
            <td>{row.triggered_at || ""}</td>
            <td>{row.recovered_at || ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function MonitorsTable({ rows }: { rows: MonitorResource[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>任务ID job_id</th>
          <th>标的 symbol</th>
          <th>策略层级 strategy_tier</th>
          <th>是否启用 enabled</th>
          <th>间隔(秒) interval_sec</th>
          <th>下次运行 next_run_at</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.job_id}>
            <td>{row.job_id}</td>
            <td>{row.symbol}</td>
            <td>{row.strategy_tier}</td>
            <td>{row.enabled ? "是" : "否"}</td>
            <td>{row.interval_sec}</td>
            <td>{row.next_run_at}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
