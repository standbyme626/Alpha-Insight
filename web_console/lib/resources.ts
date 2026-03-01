import fs from "node:fs/promises";
import path from "node:path";

import { parseSnapshot } from "@/lib/contracts";
import type {
  AlertResource,
  DegradationStateResource,
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

export async function listRuns(limit = 50): Promise<RunResource[]> {
  const snapshot = await readSnapshotFile();
  return snapshot.runs.slice(0, Math.max(1, limit));
}

export async function listAlerts(limit = 100): Promise<AlertResource[]> {
  const snapshot = await readSnapshotFile();
  return snapshot.alerts.slice(0, Math.max(1, limit));
}

export async function listEvidence(limit = 100): Promise<EvidenceResource[]> {
  const snapshot = await readSnapshotFile();
  return snapshot.evidence.slice(0, Math.max(1, limit));
}

export async function listGovernance(): Promise<DegradationStateResource[]> {
  const snapshot = await readSnapshotFile();
  return snapshot.degradation_states;
}

export async function getResourceMeta(): Promise<Pick<FrontendResourceSnapshot, "generated_at" | "db_path">> {
  const snapshot = await readSnapshotFile();
  return {
    generated_at: snapshot.generated_at,
    db_path: snapshot.db_path
  };
}
