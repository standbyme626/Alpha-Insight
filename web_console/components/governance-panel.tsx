"use client";

import { useMemo, useState } from "react";

import { SectionTitle, StatCard } from "@/components/cards";
import { GovernanceTable } from "@/components/tables";
import type { DegradationStateResource, EventTimelineResource } from "@/lib/types";

function byLatest(a: DegradationStateResource, b: DegradationStateResource): number {
  return (Date.parse(b.updated_at || "") || 0) - (Date.parse(a.updated_at || "") || 0);
}

function eventByLatest(a: EventTimelineResource, b: EventTimelineResource): number {
  return (Date.parse(b.ts || "") || 0) - (Date.parse(a.ts || "") || 0);
}

type TimelineEvent = {
  id: string;
  ts: string;
  type: "degrade" | "recover";
  state_key: string;
  reason: string;
};

export function GovernancePanel({ rows, events }: { rows: DegradationStateResource[]; events: EventTimelineResource[] }) {
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");

  const statusOptions = useMemo(() => {
    return Array.from(new Set(rows.map((row) => row.status).filter(Boolean))).sort();
  }, [rows]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return rows
      .filter((row) => {
        if (statusFilter !== "all" && row.status !== statusFilter) {
          return false;
        }
        if (!q) {
          return true;
        }
        return [row.state_key, row.reason].join(" ").toLowerCase().includes(q);
      })
      .sort(byLatest);
  }, [rows, query, statusFilter]);

  const active = filtered.filter((row) => row.status === "active").length;
  const recovered = filtered.filter((row) => row.status === "recovered").length;

  const timeline = useMemo(() => {
    const fromEvents = events
      .filter((row) => row.event_type === "degrade_started" || row.event_type === "recover_finished")
      .sort(eventByLatest)
      .slice(0, 20)
      .map<TimelineEvent>((row) => ({
        id: `event:${row.event_id}`,
        ts: row.ts,
        type: row.event_type === "degrade_started" ? "degrade" : "recover",
        state_key: row.title,
        reason: row.summary
      }));

    if (fromEvents.length > 0) {
      return fromEvents;
    }

    const fallback: TimelineEvent[] = [];
    for (const row of filtered) {
      if (row.triggered_at) {
        fallback.push({
          id: `${row.state_key}:triggered:${row.triggered_at}`,
          ts: row.triggered_at,
          type: "degrade",
          state_key: row.state_key,
          reason: row.reason
        });
      }
      if (row.recovered_at) {
        fallback.push({
          id: `${row.state_key}:recovered:${row.recovered_at}`,
          ts: row.recovered_at,
          type: "recover",
          state_key: row.state_key,
          reason: row.reason
        });
      }
    }

    return fallback
      .sort((a, b) => (Date.parse(b.ts || "") || 0) - (Date.parse(a.ts || "") || 0))
      .slice(0, 20);
  }, [events, filtered]);

  return (
    <section className="panel">
      <SectionTitle title="Governance" subtitle="Reliability degrade/recover states with chronological timeline" />
      <div className="stats-grid">
        <StatCard label="State Keys" value={filtered.length} />
        <StatCard label="Active Degrade" value={active} />
        <StatCard label="Recovered" value={recovered} />
      </div>

      <div className="filter-row">
        <input
          className="filter-input"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Filter by state key / reason"
        />
        <select className="filter-select" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
          <option value="all">status: all</option>
          {statusOptions.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>
      </div>

      <GovernanceTable rows={filtered} />

      <section className="timeline-block">
        <h3>Degrade/Recover Timeline</h3>
        {timeline.length === 0 ? (
          <p className="timeline-empty">No timeline events in current filter.</p>
        ) : (
          <ul className="timeline-list">
            {timeline.map((item) => (
              <li key={item.id}>
                <span className="timeline-time">{item.ts}</span>
                <span className="timeline-main">
                  {item.type === "degrade" ? "degrade" : "recover"} · {item.state_key}
                </span>
                <span className="timeline-sub">{item.reason}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  );
}
