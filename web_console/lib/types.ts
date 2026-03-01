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

export type FrontendResourceSnapshot = {
  generated_at: string;
  db_path: string;
  runs: RunResource[];
  alerts: AlertResource[];
  evidence: EvidenceResource[];
  degradation_states: DegradationStateResource[];
};
