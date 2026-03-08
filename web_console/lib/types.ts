export const RESOURCE_API_SCHEMA_VERSION = "upgrade10.resource_api.v1";

export type RunResource = {
  run_id: string;
  request_id: string;
  chat_id: string;
  symbol: string;
  summary: string;
  key_metrics: Record<string, unknown>;
  created_at: string;
  updated_at: string;
};

export type AlertResource = {
  event_id: string;
  symbol: string;
  priority: string;
  rule: string;
  strategy_tier: string;
  trigger_ts: string;
  run_id?: string | null;
  channel: string;
  status: string;
  tier_guarded: boolean;
  suppressed_reason?: string | null;
  last_error?: string | null;
  updated_at: string;
};

export type EvidenceResource = {
  name: string;
  path: string;
  generated_at?: string | null;
  size_bytes: number;
  summary: Record<string, unknown>;
  updated_at: string;
};

export type DegradationStateResource = {
  state_key: string;
  status: string;
  reason: string;
  triggered_at?: string | null;
  recovered_at?: string | null;
  updated_at: string;
};

export type MonitorResource = {
  job_id: string;
  chat_id: string;
  symbol: string;
  market: string;
  interval_sec: number;
  threshold: number;
  mode: string;
  scope: string;
  route_strategy: string;
  strategy_tier: string;
  enabled: boolean;
  next_run_at: string;
  last_run_at?: string | null;
  last_triggered_at?: string | null;
  last_error?: string | null;
  updated_at: string;
};

export type EventTimelineResource = {
  event_id: string;
  event_type: string;
  ts: string;
  title: string;
  summary: string;
  details: Record<string, unknown>;
};

export type FrontendResourceSnapshot = {
  generated_at: string;
  db_path: string;
  runs: RunResource[];
  alerts: AlertResource[];
  evidence: EvidenceResource[];
  degradation_states: DegradationStateResource[];
  monitors: MonitorResource[];
};

export type ApiEnvelope<T> = {
  schema_version: string;
  generated_at: string;
  data: T;
};
