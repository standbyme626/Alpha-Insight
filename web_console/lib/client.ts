import { parseAlerts, parseEvidence, parseGovernance, parseRuns } from "@/lib/contracts";
import type {
  AlertResource,
  DegradationStateResource,
  EvidenceResource,
  RunResource
} from "@/lib/types";

const getBaseUrl = () => {
  if (typeof window !== "undefined") {
    return "";
  }
  return process.env.UPGRADE7_CONSOLE_BASE_URL || "http://localhost:8600";
};

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(`${getBaseUrl()}${url}`, {
    cache: "no-store"
  });
  if (!response.ok) {
    throw new Error(`request failed: ${response.status}`);
  }
  return (await response.json()) as T;
}

export const frontendClient = {
  listRuns: async (limit = 50): Promise<RunResource[]> => parseRuns(await getJson<unknown>(`/api/resources/runs?limit=${limit}`)),
  listAlerts: async (limit = 100): Promise<AlertResource[]> =>
    parseAlerts(await getJson<unknown>(`/api/resources/alerts?limit=${limit}`)),
  listEvidence: async (limit = 100): Promise<EvidenceResource[]> =>
    parseEvidence(await getJson<unknown>(`/api/resources/evidence?limit=${limit}`)),
  listGovernance: async (): Promise<DegradationStateResource[]> =>
    parseGovernance(await getJson<unknown>("/api/resources/governance"))
};
