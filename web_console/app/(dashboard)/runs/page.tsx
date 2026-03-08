"use client";

import { useCallback, useMemo } from "react";

import { StatCard, SectionTitle } from "@/components/cards";
import { RunsTable } from "@/components/tables";
import { frontendClient } from "@/lib/client";
import { useRealtimeResource } from "@/lib/realtime";
import type { RunResource } from "@/lib/types";

const INITIAL_ROWS: RunResource[] = [];

export default function RunsPage() {
  const loadRuns = useCallback(async () => frontendClient.listRuns(80), []);
  const { data: runs, isRefreshing, pollingMode, lastRefreshedAt, error, refreshNow } = useRealtimeResource(loadRuns, INITIAL_ROWS, {
    polling: {
      focusedMs: 3000,
      blurredMs: 15000,
      hiddenMs: null
    }
  });

  const summary = useMemo(() => {
    const successCount = runs.filter((row) => Boolean(row.key_metrics?.runtime_success)).length;
    const fallbackCount = runs.filter((row) => Boolean(row.key_metrics?.runtime_fallback_used)).length;
    const budgetFailCount = runs.filter((row) => String(row.key_metrics?.runtime_budget_verdict || "") === "fail").length;
    const budgetWarnCount = runs.filter((row) => String(row.key_metrics?.runtime_budget_verdict || "") === "warn").length;
    return {
      successCount,
      fallbackCount,
      budgetFailCount,
      budgetWarnCount
    };
  }, [runs]);

  return (
    <section className="panel">
      <SectionTitle title="运行记录 Runs" subtitle="资源主链路列表视图（Resource-first）" />
      <div className="filter-row">
        <span className="timeline-sub">自动刷新：聚焦=3秒，失焦=15秒，隐藏=暂停 · 轮询模式={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "刷新中..." : "立即刷新"}
        </button>
      </div>
      {error ? <p className="timeline-empty">刷新失败：{error}</p> : null}
      <div className="stats-grid">
        <StatCard label="总运行数 Total Runs" value={runs.length} detail={lastRefreshedAt ? `更新时间=${lastRefreshedAt}` : undefined} />
        <StatCard label="成功 Success" value={summary.successCount} />
        <StatCard label="触发回退 Fallback" value={summary.fallbackCount} />
        <StatCard label="预算告警/失败 Budget" value={`${summary.budgetWarnCount}/${summary.budgetFailCount}`} />
      </div>
      <RunsTable rows={runs} />
    </section>
  );
}
