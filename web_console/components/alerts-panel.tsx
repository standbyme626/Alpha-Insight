"use client";

import { useMemo, useState } from "react";

import { SectionTitle, StatCard } from "@/components/cards";
import { AlertsTable } from "@/components/tables";
import type { AlertResource, EventTimelineResource } from "@/lib/types";

function byLatest(a: AlertResource, b: AlertResource): number {
  return (Date.parse(b.updated_at || "") || 0) - (Date.parse(a.updated_at || "") || 0);
}

function eventByLatest(a: EventTimelineResource, b: EventTimelineResource): number {
  return (Date.parse(b.ts || "") || 0) - (Date.parse(a.ts || "") || 0);
}

type TimelineRow = {
  id: string;
  ts: string;
  main: string;
  sub: string;
};

function readStringDetail(event: EventTimelineResource, key: string): string {
  const value = event.details[key];
  return typeof value === "string" ? value : "";
}

export function AlertsPanel({
  rows,
  activeDegradeStates,
  events
}: {
  rows: AlertResource[];
  activeDegradeStates: number;
  events: EventTimelineResource[];
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

  const timeline = useMemo<TimelineRow[]>(() => {
    const byEvents = events
      .filter((row) => row.event_type === "guard_triggered" || row.event_type === "delivery_failed")
      .sort(eventByLatest)
      .slice(0, 16)
      .map((row) => {
        const strategyTier = readStringDetail(row, "strategy_tier");
        return {
          id: `event:${row.event_id}`,
          ts: row.ts,
          main: `${row.event_type} · ${row.title}`,
          sub: `${strategyTier ? `策略层级=${strategyTier} · ` : ""}${row.summary}`
        };
      });

    if (byEvents.length > 0) {
      return byEvents;
    }

    return filtered
      .filter(
        (row) =>
          row.tier_guarded ||
          Boolean(row.last_error) ||
          row.status === "failed" ||
          row.status === "dlq" ||
          row.status.startsWith("retry")
      )
      .slice(0, 16)
      .map((row) => ({
        id: `alert:${row.event_id}:${row.channel}:${row.updated_at}`,
        ts: row.updated_at || row.trigger_ts,
        main: `${row.event_id} · ${row.symbol} · ${row.channel} · ${row.status}`,
        sub: `策略层级=${row.strategy_tier} · 守卫=${row.tier_guarded ? "是" : "否"} ${row.last_error || row.suppressed_reason || ""}`
      }));
  }, [events, filtered]);

  return (
    <section className="panel">
      <SectionTitle title="告警中心 Alerts" subtitle="通道投递状态、策略层级守卫与降级信号" />
      <div className="stats-grid">
        <StatCard label="筛选后告警 Filtered" value={filtered.length} />
        <StatCard label="已投递 Delivered" value={delivered} />
        <StatCard label="层级守卫触发 Guarded" value={guarded} />
        <StatCard label="失败/DLQ" value={failed} detail={`活跃降级状态=${activeDegradeStates}`} />
      </div>

      <div className="filter-row">
        <input
          className="filter-input"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="按 event_id / symbol / 错误信息过滤"
        />
        <select className="filter-select" value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
          <option value="all">状态：全部</option>
          {statusOptions.map((status) => (
            <option key={status} value={status}>
              {status}
            </option>
          ))}
        </select>
        <select className="filter-select" value={tierFilter} onChange={(event) => setTierFilter(event.target.value)}>
          <option value="all">策略层级：全部</option>
          {tierOptions.map((tier) => (
            <option key={tier} value={tier}>
              {tier}
            </option>
          ))}
        </select>
        <select className="filter-select" value={channelFilter} onChange={(event) => setChannelFilter(event.target.value)}>
          <option value="all">通道：全部</option>
          {channelOptions.map((channel) => (
            <option key={channel} value={channel}>
              {channel}
            </option>
          ))}
        </select>
      </div>

      <AlertsTable rows={filtered} />

      <section className="timeline-block">
        <h3>错误/守卫时间线 Error Guard Timeline</h3>
        {timeline.length === 0 ? (
          <p className="timeline-empty">当前筛选范围内没有守卫/错误事件。</p>
        ) : (
          <ul className="timeline-list">
            {timeline.map((row) => (
              <li key={row.id}>
                <span className="timeline-time">{row.ts}</span>
                <span className="timeline-main">{row.main}</span>
                <span className="timeline-sub">{row.sub}</span>
              </li>
            ))}
          </ul>
        )}
      </section>
    </section>
  );
}
