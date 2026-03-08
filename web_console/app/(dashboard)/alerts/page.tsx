"use client";

import { useCallback } from "react";

import { AlertsPanel } from "@/components/alerts-panel";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import type { AlertResource, EventTimelineResource } from "@/lib/types";

type AlertsPageData = {
  rows: AlertResource[];
  activeDegradeStates: number;
  events: EventTimelineResource[];
};

const INITIAL_DATA: AlertsPageData = {
  rows: [],
  activeDegradeStates: 0,
  events: []
};

export default function AlertsPage() {
  const loadAlerts = useCallback(async (): Promise<AlertsPageData> => {
    const [rows, governance, events] = await Promise.all([
      frontendClient.listAlerts(120),
      frontendClient.listGovernance(),
      frontendClient.listEvents(200)
    ]);
    return {
      rows,
      activeDegradeStates: governance.filter((row) => row.status === "active").length,
      events
    };
  }, []);

  const { data, isRefreshing, pollingMode, lastRefreshedAt, error, refreshNow } = useRealtimeResource(loadAlerts, INITIAL_DATA, {
    polling: {
      focusedMs: 3000,
      blurredMs: 15000,
      hiddenMs: null
    }
  });

  return (
    <section className="panel">
      <div className="filter-row">
        <span className="timeline-sub">auto-refresh: focus=3s, blur=15s, hidden=pause · mode={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "refreshing..." : "refresh now"}
        </button>
      </div>
      {lastRefreshedAt ? <p className="timeline-sub">last updated: {lastRefreshedAt}</p> : null}
      {error ? <p className="timeline-empty">refresh failed: {error}</p> : null}
      <AlertsPanel rows={data.rows} activeDegradeStates={data.activeDegradeStates} events={data.events} />
    </section>
  );
}
