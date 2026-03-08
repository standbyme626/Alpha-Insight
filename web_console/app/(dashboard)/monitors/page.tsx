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
      <SectionTitle title="Monitors" subtitle="Watch jobs with strategy tier and scheduler status" />
      <div className="filter-row">
        <span className="timeline-sub">auto-refresh: focus=3s, blur=15s, hidden=pause · mode={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "refreshing..." : "refresh now"}
        </button>
      </div>
      {error ? <p className="timeline-empty">refresh failed: {error}</p> : null}
      <div className="stats-grid">
        <StatCard label="Total Jobs" value={monitors.length} detail={lastRefreshedAt ? `updated=${lastRefreshedAt}` : undefined} />
        <StatCard label="Enabled" value={summary.enabledCount} />
        <StatCard label="Disabled" value={summary.disabledCount} />
      </div>
      <MonitorsTable rows={monitors} />
    </section>
  );
}
