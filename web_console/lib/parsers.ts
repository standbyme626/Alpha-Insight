import type {
  AlertResource,
  DegradationStateResource,
  EventTimelineResource,
  EvidenceResource,
  FrontendResourceSnapshot,
  MonitorResource,
  RunResource
} from "@/lib/types";
import { asArray, asBoolean, asInteger, asNullableString, asNumber, asObject, asString, isRecord, unwrapEnvelope } from "@/lib/type_guards";

function normalizeList<T>(items: unknown[], mapper: (value: unknown) => T | null): T[] {
  return items
    .map((item) => mapper(item))
    .filter((item): item is T => item !== null);
}

function toRunResource(value: unknown): RunResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    run_id: asString(value.run_id),
    request_id: asString(value.request_id),
    chat_id: asString(value.chat_id),
    symbol: asString(value.symbol),
    summary: asString(value.summary),
    key_metrics: asObject(value.key_metrics),
    created_at: asString(value.created_at),
    updated_at: asString(value.updated_at)
  };
}

function toAlertResource(value: unknown): AlertResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    event_id: asString(value.event_id),
    symbol: asString(value.symbol),
    priority: asString(value.priority),
    rule: asString(value.rule),
    strategy_tier: asString(value.strategy_tier, "execution-ready"),
    trigger_ts: asString(value.trigger_ts),
    run_id: asNullableString(value.run_id),
    channel: asString(value.channel),
    status: asString(value.status),
    tier_guarded: asBoolean(value.tier_guarded),
    suppressed_reason: asNullableString(value.suppressed_reason),
    last_error: asNullableString(value.last_error),
    updated_at: asString(value.updated_at)
  };
}

function toEvidenceResource(value: unknown): EvidenceResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    name: asString(value.name),
    path: asString(value.path),
    generated_at: asNullableString(value.generated_at),
    size_bytes: asNumber(value.size_bytes, 0),
    summary: asObject(value.summary),
    updated_at: asString(value.updated_at)
  };
}

function toDegradationStateResource(value: unknown): DegradationStateResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    state_key: asString(value.state_key),
    status: asString(value.status),
    reason: asString(value.reason),
    triggered_at: asNullableString(value.triggered_at),
    recovered_at: asNullableString(value.recovered_at),
    updated_at: asString(value.updated_at)
  };
}

function toEventTimelineResource(value: unknown): EventTimelineResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    event_id: asString(value.event_id),
    event_type: asString(value.event_type),
    ts: asString(value.ts),
    title: asString(value.title),
    summary: asString(value.summary),
    details: asObject(value.details)
  };
}

function toMonitorResource(value: unknown): MonitorResource | null {
  if (!isRecord(value)) {
    return null;
  }
  return {
    job_id: asString(value.job_id),
    chat_id: asString(value.chat_id),
    symbol: asString(value.symbol),
    market: asString(value.market),
    interval_sec: asInteger(value.interval_sec, 0),
    threshold: asNumber(value.threshold, 0),
    mode: asString(value.mode),
    scope: asString(value.scope, "single"),
    route_strategy: asString(value.route_strategy, "dual_channel"),
    strategy_tier: asString(value.strategy_tier, "execution-ready"),
    enabled: asBoolean(value.enabled),
    next_run_at: asString(value.next_run_at),
    last_run_at: asNullableString(value.last_run_at),
    last_triggered_at: asNullableString(value.last_triggered_at),
    last_error: asNullableString(value.last_error),
    updated_at: asString(value.updated_at)
  };
}

export function parseRuns(payload: unknown): RunResource[] {
  return normalizeList(asArray(unwrapEnvelope<unknown[]>(payload)), toRunResource);
}

export function parseAlerts(payload: unknown): AlertResource[] {
  return normalizeList(asArray(unwrapEnvelope<unknown[]>(payload)), toAlertResource);
}

export function parseEvidence(payload: unknown): EvidenceResource[] {
  return normalizeList(asArray(unwrapEnvelope<unknown[]>(payload)), toEvidenceResource);
}

export function parseGovernance(payload: unknown): DegradationStateResource[] {
  const unwrapped = unwrapEnvelope<unknown>(payload);
  if (Array.isArray(unwrapped)) {
    return normalizeList(unwrapped, toDegradationStateResource);
  }
  if (isRecord(unwrapped)) {
    return normalizeList(asArray(unwrapped.states), toDegradationStateResource);
  }
  return [];
}

export function parseEvents(payload: unknown): EventTimelineResource[] {
  const unwrapped = unwrapEnvelope<unknown>(payload);
  if (Array.isArray(unwrapped)) {
    return normalizeList(unwrapped, toEventTimelineResource);
  }
  if (isRecord(unwrapped)) {
    return normalizeList(asArray(unwrapped.events), toEventTimelineResource);
  }
  return [];
}

export function parseMonitors(payload: unknown): MonitorResource[] {
  return normalizeList(asArray(unwrapEnvelope<unknown[]>(payload)), toMonitorResource);
}

export function parseSnapshot(payload: unknown): FrontendResourceSnapshot {
  if (!isRecord(payload)) {
    return {
      generated_at: new Date(0).toISOString(),
      db_path: "",
      runs: [],
      alerts: [],
      evidence: [],
      degradation_states: [],
      monitors: []
    };
  }

  return {
    generated_at: asString(payload.generated_at, new Date(0).toISOString()),
    db_path: asString(payload.db_path),
    runs: parseRuns(payload.runs),
    alerts: parseAlerts(payload.alerts),
    evidence: parseEvidence(payload.evidence),
    degradation_states: parseGovernance(payload.degradation_states),
    monitors: parseMonitors(payload.monitors)
  };
}
