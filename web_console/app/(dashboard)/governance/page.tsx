"use client";

import { useCallback } from "react";

import { GovernancePanel } from "@/components/governance-panel";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import { useSseRefresh } from "@/lib/sse";
import type { DegradationStateResource, EventTimelineResource } from "@/lib/types";

type GovernancePageData = {
  rows: DegradationStateResource[];
  events: EventTimelineResource[];
};

const INITIAL_DATA: GovernancePageData = {
  rows: [],
  events: []
};

function streamStateLabel(state: "connecting" | "open" | "error" | "closed"): string {
  if (state === "connecting") return "连接中";
  if (state === "open") return "已连接";
  if (state === "error") return "异常";
  return "已关闭";
}

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
  const eventsStream = useSseRefresh("/api/resources/events/stream?limit=200", refreshNow, {
    events: ["events"]
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
        事件流状态：{streamStateLabel(eventsStream.state)}
        {eventsStream.lastEventAt ? ` · 最新事件=${eventsStream.lastEventAt}` : ""}
      </p>
      {error ? <p className="timeline-empty">轮询刷新失败：{error}</p> : null}
      {eventsStream.error ? <p className="timeline-empty">{eventsStream.error}</p> : null}
      <GovernancePanel rows={data.rows} events={data.events} />
    </section>
  );
}
