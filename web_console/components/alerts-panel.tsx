"use client";

import { useMemo, useState } from "react";

import { SectionTitle, StatCard } from "@/components/cards";
import { AlertsTable } from "@/components/tables";
import type { AlertResource } from "@/lib/types";

function byLatest(a: AlertResource, b: AlertResource): number {
  return (Date.parse(b.updated_at || "") || 0) - (Date.parse(a.updated_at || "") || 0);
}

export function AlertsPanel({
  rows,
  activeDegradeStates
}: {
  rows: AlertResource[];
  activeDegradeStates: number;
}) {
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [tierFilter, setTierFilter] = useState("all");
  const [channelFilter, setChannelFilter] = useState("all");

  const statusOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.status).filter(Boolean))).sort();
  }, [rows]);
  const tierOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.strategy_tier).filter(Boolean))).sort();
  }, [rows]);
  const channelOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.channel).filter(Boolean))).sort();
  }, [rows]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return rows
      .filter((row) => {
        if (statusFilter !== "all" && row.status !== statusFilter) {
          return false;
        }
        if (tierFilter !== "all" && row.strategy_tier !== tierFilter) {
          return false;
        }
        if (channelFilter !== "all" && row.channel !== channelFilter) {
          return false;
        }
        if (!q) {
          return true;
        }
        const haystack = [row.event_id, row.symbol, row.channel, row.last_error || "", row.suppressed_reason || ""]
          .join(" ")
          .toLowerCase();
        return haystack.includes(q);
      })
      .sort(byLatest);
  }, [rows, query, statusFilter, tierFilter, channelFilter]);

  const delivered = filtered.filter((row) => row.status === "delivered").length;
  const guarded = filtered.filter((row) => row.tier_guarded).length;
  const failed = filtered.filter((row) => row.status === "failed" || row.status === "dlq").length;

  const timeline = filtered
    .filter(
      (row) =>
        row.tier_guarded ||
        Boolean(row.last_error) ||
        row.status === "failed" ||
        row.status === "dlq" ||
        row.status.startsWith("retry")
    )
    .slice(0, 16);

  return (
    <section className="panel">
      <SectionTitle title="Alerts" subtitle="Channel delivery states, strategy tier guard, and degradation hints" />
      <div className="stats-grid">
        <StatCard label="Filtered Alerts" value={filtered.length} />
        <StatCard label="Delivered" value={delivered} />
        <StatCard label="Tier Guarded" value={guarded} />
        <StatCard label="Failed/DLQ" value={failed} detail={`active degrade states=${activeDegradeStates}`} />
      </div>

      <div className="filter-row">
        <input
          className="filter-input"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Filter by event_id / symbol / error"
        />
        <select className="filter-select" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
          <option value="all">status: all</option>
          {statusOptions.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>
        <select className="filter-select" value={tierFilter} onChange={(event) => setTierFilter(event.target.value)}>
          <option value="all">tier: all</option>
          {tierOptions.map((tier) => (
            <option key={tier} value={tier}>
              {tier}
            </option>
          ))}
        </select>
        <select className="filter-select" value={channelFilter} onChange={(event) => setChannelFilter(event.target.value)}>
          <option value="all">channel: all</option>
          {channelOptions.map((channel) => (
            <option key={channel} value={channel}>
              {channel}
            </option>
          ))}
        </select>
      </div>

      <AlertsTable rows={filtered} />

      <section className="timeline-block">
        <h3>Error/Guard Timeline</h3>
        {timeline.length === 0 ? (
          <p className="timeline-empty">No guard/error events in current filter.</p>
        ) : (
          <ul className="timeline-list">
            {timeline.map((row) => (
              <li key={`${row.event_id}:${row.channel}:${row.updated_at}`}>
                <span className="timeline-time">{row.updated_at || row.trigger_ts}</span>
                <span className="timeline-main">
                  {row.event_id} · {row.symbol} · {row.channel} · {row.status}
                </span>
                <span className="timeline-sub">
                  tier={row.strategy_tier} guarded={row.tier_guarded ? "yes" : "no"}{" "}
                  {row.last_error || row.suppressed_reason || ""}
                </span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  );
}
