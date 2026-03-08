"use client";

import { useCallback, useMemo } from "react";

import { SectionTitle, StatCard } from "@/components/cards";
import { MonitorsTable } from "@/components/tables";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import type { MonitorResource } from "@/lib/types";

const INITIAL_ROWS: MonitorResource[] = [];

export default function MonitorsPage() {
  const loadMonitors = useCallback(async () => frontendClient.listMonitors(200), []);
  const { data: monitors, isRefreshing, pollingMode, lastRefreshedAt, error, refreshNow } = useRealtimeResource(
    loadMonitors,
    INITIAL_ROWS,
    {
      polling: {
        focusedMs: 3000,
        blurredMs: 15000,
        hiddenMs: null
      }
    }
  );

  const summary = useMemo(() => {
    const enabledCount = monitors.filter((row) => row.enabled).length;
    const disabledCount = monitors.length - enabledCount;
    return { enabledCount, disabledCount };
  }, [monitors]);

  return (
    <section className="panel">
      <SectionTitle title="监控任务 Monitors" subtitle="监控任务、策略分层与调度状态" />
      <div className="filter-row">
        <span className="timeline-sub">自动刷新：聚焦=3秒，失焦=15秒，隐藏=暂停 · 轮询模式={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "刷新中..." : "立即刷新"}
        </button>
      </div>
      {error ? <p className="timeline-empty">刷新失败：{error}</p> : null}
      <div className="stats-grid">
        <StatCard label="任务总数 Total Jobs" value={monitors.length} detail={lastRefreshedAt ? `更新时间=${lastRefreshedAt}` : undefined} />
        <StatCard label="启用 Enabled" value={summary.enabledCount} />
        <StatCard label="停用 Disabled" value={summary.disabledCount} />
      </div>
      <MonitorsTable rows={monitors} />
    </section>
  );
}
