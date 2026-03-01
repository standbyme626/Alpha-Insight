import { StatCard, SectionTitle } from "@/components/cards";
import { RunsTable } from "@/components/tables";
import { frontendClient } from "@/lib/client";

export default async function RunsPage() {
  const runs = await frontendClient.listRuns(80);
  const successCount = runs.filter((row) => Boolean(row.key_metrics?.runtime_success)).length;
  const fallbackCount = runs.filter((row) => Boolean(row.key_metrics?.runtime_fallback_used)).length;
  const budgetFailCount = runs.filter((row) => String(row.key_metrics?.runtime_budget_verdict || "") === "fail").length;
  const budgetWarnCount = runs.filter((row) => String(row.key_metrics?.runtime_budget_verdict || "") === "warn").length;
  return (
    <section className="panel">
      <SectionTitle title="Runs" subtitle="Resource-first list view (react-admin/refine pattern)" />
      <div className="stats-grid">
        <StatCard label="Total Runs" value={runs.length} />
        <StatCard label="Success" value={successCount} />
        <StatCard label="Fallback Used" value={fallbackCount} />
        <StatCard label="Budget Warn/Fail" value={`${budgetWarnCount}/${budgetFailCount}`} />
      </div>
      <RunsTable rows={runs} />
    </section>
  );
}
