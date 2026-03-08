"use client";

import { useCallback } from "react";

import { AlertsPanel } from "@/components/alerts-panel";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import { useSseRefresh } from "@/lib/sse";
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

function streamStateLabel(state: "connecting" | "open" | "error" | "closed"): string {
  if (state === "connecting") return "连接中";
  if (state === "open") return "已连接";
  if (state === "error") return "异常";
  return "已关闭";
}

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

  const eventsStream = useSseRefresh("/api/resources/events/stream?limit=200", refreshNow, {
    events: ["events"]
  });
  const alertsStream = useSseRefresh("/api/resources/alerts/stream?limit=120", refreshNow, {
    events: ["alerts"]
  });

  return (
    <section className="panel">
      <div className="filter-row">
        <span className="timeline-sub">自动刷新：聚焦=3秒，失焦=15秒，隐藏=暂停 · 轮询模式={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "刷新中..." : "立即刷新"}
        </button>
      </div>
      {lastRefreshedAt ? <p className="timeline-sub">上次刷新：{lastRefreshedAt}</p> : null}
      <p className="timeline-sub">
        事件流状态：events={streamStateLabel(eventsStream.state)}，alerts={streamStateLabel(alertsStream.state)}
        {eventsStream.lastEventAt ? ` · events最新=${eventsStream.lastEventAt}` : ""}
        {alertsStream.lastEventAt ? ` · alerts最新=${alertsStream.lastEventAt}` : ""}
      </p>
      {error ? <p className="timeline-empty">轮询刷新失败：{error}</p> : null}
      {eventsStream.error ? <p className="timeline-empty">{eventsStream.error}</p> : null}
      {alertsStream.error ? <p className="timeline-empty">{alertsStream.error}</p> : null}
      <AlertsPanel rows={data.rows} activeDegradeStates={data.activeDegradeStates} events={data.events} />
    </section>
  );
}
