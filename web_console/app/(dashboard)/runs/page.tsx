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
      <SectionTitle title="Runs" subtitle="Resource-first list view (react-admin/refine pattern)" />
      <div className="filter-row">
        <span className="timeline-sub">auto-refresh: focus=3s, blur=15s, hidden=pause · mode={pollingMode}</span>
        <button className="filter-select" onClick={() => void refreshNow()} disabled={isRefreshing}>
          {isRefreshing ? "refreshing..." : "refresh now"}
        </button>
      </div>
      {error ? <p className="timeline-empty">refresh failed: {error}</p> : null}
      <div className="stats-grid">
        <StatCard label="Total Runs" value={runs.length} detail={lastRefreshedAt ? `updated=${lastRefreshedAt}` : undefined} />
        <StatCard label="Success" value={summary.successCount} />
        <StatCard label="Fallback Used" value={summary.fallbackCount} />
        <StatCard label="Budget Warn/Fail" value={`${summary.budgetWarnCount}/${summary.budgetFailCount}`} />
      </div>
      <RunsTable rows={runs} />
    </section>
  );
}
