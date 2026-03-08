import fs from "node:fs/promises";
import path from "node:path";

import { parseAlerts, parseEvents, parseEvidence, parseGovernance, parseRuns, parseSnapshot } from "@/lib/contracts";
import type {
  AlertResource,
  DegradationStateResource,
  EventTimelineResource,
  EvidenceResource,
  FrontendResourceSnapshot,
  RunResource
} from "@/lib/types";

const RESOURCE_FILE = "upgrade7_frontend_resources.json";

const fallbackSnapshot: FrontendResourceSnapshot = {
  generated_at: new Date(0).toISOString(),
  db_path: "",
  runs: [],
  alerts: [],
  evidence: [],
  degradation_states: []
};

const upstreamBaseUrl =
  process.env.UPGRADE10_RESOURCE_API_BASE_URL || process.env.RESOURCE_API_BASE_URL || process.env.UPGRADE7_RESOURCE_API_BASE_URL || "";

function clampLimit(limit: number, fallback: number): number {
  if (!Number.isFinite(limit)) {
    return fallback;
  }
  return Math.max(1, Math.floor(limit));
}

function buildUpstreamUrl(pathname: string): string | null {
  const base = upstreamBaseUrl.trim();
  if (!base) {
    return null;
  }
  return `${base.replace(/\/$/, "")}${pathname}`;
}

async function fetchUpstream(pathname: string): Promise<unknown | null> {
  const url = buildUpstreamUrl(pathname);
  if (!url) {
    return null;
  }
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as unknown;
  } catch {
    return null;
  }
}

async function readSnapshotFile(): Promise<FrontendResourceSnapshot> {
  const candidate = path.resolve(process.cwd(), "..", "docs", "evidence", RESOURCE_FILE);
  try {
    const payload = await fs.readFile(candidate, "utf-8");
    const parsed = parseSnapshot(JSON.parse(payload));
    return {
      ...fallbackSnapshot,
      ...parsed
    };
  } catch {
    return fallbackSnapshot;
  }
}

function parseTimestamp(value: string): number {
  const ts = Date.parse(value || "");
  return Number.isFinite(ts) ? ts : 0;
}

function pickEventTs(value: EventTimelineResource): number {
  return parseTimestamp(value.ts);
}

function deriveEventsFromSnapshot(snapshot: FrontendResourceSnapshot): EventTimelineResource[] {
  const governanceEvents = snapshot.degradation_states.flatMap((row) => {
    const rows: EventTimelineResource[] = [];
    if (row.triggered_at) {
      rows.push({
        event_id: `snapshot:degrade:${row.state_key}:${row.triggered_at}`,
        event_type: "degrade_started",
        ts: row.triggered_at,
        title: row.state_key,
        summary: row.reason || "",
        details: {
          state_key: row.state_key,
          status: row.status,
          source: "snapshot"
        }
      });
    }
    if (row.recovered_at) {
      rows.push({
        event_id: `snapshot:recover:${row.state_key}:${row.recovered_at}`,
        event_type: "recover_finished",
        ts: row.recovered_at,
        title: row.state_key,
        summary: row.reason || "",
        details: {
          state_key: row.state_key,
          status: row.status,
          source: "snapshot"
        }
      });
    }
    return rows;
  });

  const alertEvents = snapshot.alerts.flatMap((row) => {
    const ts = row.updated_at || row.trigger_ts;
    if (!ts) {
      return [];
    }
    if (row.tier_guarded || (row.suppressed_reason || "").startsWith("strategy_tier_guard")) {
      return [
        {
          event_id: `snapshot:guard:${row.event_id}:${row.channel}:${ts}`,
          event_type: "guard_triggered",
          ts,
          title: `${row.symbol} via ${row.channel}`,
          summary: row.suppressed_reason || "strategy_tier_guard",
          details: {
            event_id: row.event_id,
            symbol: row.symbol,
            channel: row.channel,
            strategy_tier: row.strategy_tier,
            source: "snapshot"
          }
        }
      ];
    }
    if (row.last_error || row.status === "failed" || row.status === "dlq" || row.status.startsWith("retry")) {
      return [
        {
          event_id: `snapshot:delivery_failed:${row.event_id}:${row.channel}:${ts}`,
          event_type: "delivery_failed",
          ts,
          title: `${row.symbol} via ${row.channel}`,
          summary: row.last_error || row.status,
          details: {
            event_id: row.event_id,
            symbol: row.symbol,
            channel: row.channel,
            strategy_tier: row.strategy_tier,
            status: row.status,
            source: "snapshot"
          }
        }
      ];
    }
    return [];
  });

  return [...governanceEvents, ...alertEvents].sort((a, b) => pickEventTs(b) - pickEventTs(a));
}

function applyEventFilters(
  rows: EventTimelineResource[],
  options: { since: string | null | undefined; limit: number }
): EventTimelineResource[] {
  const { since, limit } = options;
  const sinceTs = since ? parseTimestamp(since) : 0;
  return rows
    .filter((row) => (!sinceTs ? true : pickEventTs(row) >= sinceTs))
    .slice(0, clampLimit(limit, 200));
}

export async function listRuns(limit = 50): Promise<RunResource[]> {
  const normalizedLimit = clampLimit(limit, 50);
  const payload = await fetchUpstream(`/api/runs?limit=${normalizedLimit}`);
  if (payload !== null) {
    return parseRuns(payload);
  }
  const snapshot = await readSnapshotFile();
  return snapshot.runs.slice(0, normalizedLimit);
}

export async function listAlerts(limit = 100): Promise<AlertResource[]> {
  const normalizedLimit = clampLimit(limit, 100);
  const payload = await fetchUpstream(`/api/alerts?limit=${normalizedLimit}`);
  if (payload !== null) {
    return parseAlerts(payload);
  }
  const snapshot = await readSnapshotFile();
  return snapshot.alerts.slice(0, normalizedLimit);
}

export async function listEvidence(limit = 100): Promise<EvidenceResource[]> {
  const normalizedLimit = clampLimit(limit, 100);
  const payload = await fetchUpstream(`/api/evidence?limit=${normalizedLimit}`);
  if (payload !== null) {
    return parseEvidence(payload);
  }
  const snapshot = await readSnapshotFile();
  return snapshot.evidence.slice(0, normalizedLimit);
}

export async function listGovernance(limit = 200): Promise<DegradationStateResource[]> {
  const normalizedLimit = clampLimit(limit, 200);
  const payload = await fetchUpstream(`/api/governance?limit=${normalizedLimit}`);
  if (payload !== null) {
    return parseGovernance(payload).slice(0, normalizedLimit);
  }
  const snapshot = await readSnapshotFile();
  return snapshot.degradation_states.slice(0, normalizedLimit);
}

export async function listEvents(options?: { since?: string | null; limit?: number }): Promise<EventTimelineResource[]> {
  const since = options?.since;
  const limit = clampLimit(options?.limit ?? 200, 200);
  const query = new URLSearchParams({ limit: String(limit) });
  if (since) {
    query.set("since", since);
  }

  const payload = await fetchUpstream(`/api/events?${query.toString()}`);
  if (payload !== null) {
    return applyEventFilters(parseEvents(payload), { since, limit });
  }

  const snapshot = await readSnapshotFile();
  return applyEventFilters(deriveEventsFromSnapshot(snapshot), { since, limit });
}

export async function getResourceMeta(): Promise<Pick<FrontendResourceSnapshot, "generated_at" | "db_path">> {
  const snapshot = await readSnapshotFile();
  return {
    generated_at: snapshot.generated_at,
    db_path: snapshot.db_path
  };
}
