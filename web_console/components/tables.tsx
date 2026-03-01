import type { AlertResource, DegradationStateResource, EvidenceResource, RunResource } from "@/lib/types";

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
          <th>run_id</th>
          <th>symbol</th>
          <th>success</th>
          <th>fallback</th>
          <th>retry</th>
          <th>budget</th>
          <th>p95_ms</th>
          <th>error_rate</th>
          <th>updated_at</th>
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
          <th>event_id</th>
          <th>symbol</th>
          <th>priority</th>
          <th>strategy_tier</th>
          <th>tier_guarded</th>
          <th>channel</th>
          <th>status</th>
          <th>run_id</th>
          <th>last_error</th>
          <th>updated_at</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.event_id}:${row.channel}`}>
            <td>{row.event_id}</td>
            <td>{row.symbol}</td>
            <td>{row.priority}</td>
            <td>{row.strategy_tier}</td>
            <td>{row.tier_guarded ? "yes" : "no"}</td>
            <td>{row.channel}</td>
            <td>
              <span className={`status-pill ${statusTone(row.status)}`}>{row.status || "unknown"}</span>
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
          <th>name</th>
          <th>generated_at</th>
          <th>size</th>
          <th>path</th>
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
          <th>state_key</th>
          <th>status</th>
          <th>reason</th>
          <th>triggered_at</th>
          <th>recovered_at</th>
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
