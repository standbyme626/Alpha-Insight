import { RESOURCE_API_SCHEMA_VERSION } from "@/lib/types";
import type {
  AlertResource,
  ApiEnvelope,
  DegradationStateResource,
  EventTimelineResource,
  EvidenceResource,
  FrontendResourceSnapshot,
  RunResource
} from "@/lib/types";

type UnknownRecord = Record<string, unknown>;

function isRecord(value: unknown): value is UnknownRecord {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asBoolean(value: unknown, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function asObject(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function unwrapEnvelope<T>(payload: unknown): T | unknown {
  if (!isRecord(payload)) {
    return payload;
  }
  if (!("data" in payload)) {
    return payload;
  }
  return payload.data as T;
}

function asSchemaVersion(value: unknown): string {
  return asString(value, RESOURCE_API_SCHEMA_VERSION);
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
  const sizeBytes = typeof value.size_bytes === "number" && Number.isFinite(value.size_bytes) ? value.size_bytes : 0;
  return {
    name: asString(value.name),
    path: asString(value.path),
    generated_at: asNullableString(value.generated_at),
    size_bytes: sizeBytes,
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

function normalizeList<T>(items: unknown[], mapper: (value: unknown) => T | null): T[] {
  return items
    .map((item) => mapper(item))
    .filter((item): item is T => item !== null);
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

export function parseEnvelope<T>(payload: unknown, parser: (value: unknown) => T): ApiEnvelope<T> {
  if (!isRecord(payload)) {
    return {
      schema_version: RESOURCE_API_SCHEMA_VERSION,
      generated_at: new Date(0).toISOString(),
      data: parser(payload)
    };
  }
  if (!("data" in payload)) {
    return {
      schema_version: RESOURCE_API_SCHEMA_VERSION,
      generated_at: new Date(0).toISOString(),
      data: parser(payload)
    };
  }
  return {
    schema_version: asSchemaVersion(payload.schema_version),
    generated_at: asString(payload.generated_at, new Date(0).toISOString()),
    data: parser(payload.data)
  };
}

export function parseSnapshot(payload: unknown): FrontendResourceSnapshot {
  if (!isRecord(payload)) {
    return {
      generated_at: new Date(0).toISOString(),
      db_path: "",
      runs: [],
      alerts: [],
      evidence: [],
      degradation_states: []
    };
  }

  return {
    generated_at: asString(payload.generated_at, new Date(0).toISOString()),
    db_path: asString(payload.db_path),
    runs: parseRuns(payload.runs),
    alerts: parseAlerts(payload.alerts),
    evidence: parseEvidence(payload.evidence),
    degradation_states: parseGovernance(payload.degradation_states)
  };
}
