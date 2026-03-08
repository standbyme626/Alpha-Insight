"use client";

import { useCallback } from "react";

import { GovernancePanel } from "@/components/governance-panel";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import type { DegradationStateResource, EventTimelineResource } from "@/lib/types";

type GovernancePageData = {
  rows: DegradationStateResource[];
  events: EventTimelineResource[];
};

const INITIAL_DATA: GovernancePageData = {
  rows: [],
  events: []
};

export default function GovernancePage() {
  const loadGovernance = useCallback(async (): Promise<GovernancePageData> => {
    const [rows, events] = await Promise.all([frontendClient.listGovernance(), frontendClient.listEvents(200)]);
    return { rows, events };
  }, []);

  const { data, isRefreshing, pollingMode, lastRefreshedAt, error, refreshNow } = useRealtimeResource(
    loadGovernance,
    INITIAL_DATA,
    {
      polling: {
        focusedMs: 3000,
        blurredMs: 15000,
        hiddenMs: null
      }
    }
  );

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
      <GovernancePanel rows={data.rows} events={data.events} />
    </section>
  );
}
