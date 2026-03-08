import { parseAlerts, parseEvents, parseEvidence, parseGovernance, parseMonitors, parseRuns, parseSnapshot } from "@/lib/parsers";
import { asString, isRecord } from "@/lib/type_guards";
import { RESOURCE_API_SCHEMA_VERSION, RESOURCE_VERSIONS } from "@/lib/types";
import type { ApiEnvelope } from "@/lib/types";

function asSchemaVersion(value: unknown): string {
  return asString(value, RESOURCE_API_SCHEMA_VERSION);
}

export function parseEnvelope<T>(
  payload: unknown,
  parser: (value: unknown) => T,
  resource?: keyof typeof RESOURCE_VERSIONS
): ApiEnvelope<T> {
  if (!isRecord(payload) || !("data" in payload)) {
    return {
      schema_version: RESOURCE_API_SCHEMA_VERSION,
      resource,
      resource_version: resource ? RESOURCE_VERSIONS[resource] : undefined,
      generated_at: new Date(0).toISOString(),
      data: parser(payload)
    };
  }
  return {
    schema_version: asSchemaVersion(payload.schema_version),
    resource: asString(payload.resource, resource || ""),
    resource_version: asString(payload.resource_version, resource ? RESOURCE_VERSIONS[resource] : ""),
    generated_at: asString(payload.generated_at, new Date(0).toISOString()),
    data: parser(payload.data)
  };
}

export { parseAlerts, parseEvents, parseEvidence, parseGovernance, parseMonitors, parseRuns, parseSnapshot };
