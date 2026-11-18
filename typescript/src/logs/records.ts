// Service-owned. Mapping between SDK LogRecords and OpenObserve rows / OTLP JSON.

import {
  Severity,
  parseSeverity,
  severityText,
  type AttrValue,
  type Attributes,
  type LogRecord,
} from "../logging/index.js";
import { type JsonObject, flattenKey, microsToDate, num, str, toMicros } from "../http.js";

/** Row columns that {@link rowToLogRecord} maps to `LogRecord` fields instead of attributes. */
const RECORD_COLUMNS: ReadonlySet<string> = new Set([
  "_timestamp",
  "_o2_ingest_ts",
  "body",
  "message",
  "severity",
  "level",
  "severity_number",
  "severity_text",
  "trace_id",
  "span_id",
  "trace_flags",
  "flags",
  "observed_timestamp",
]);

/** Flattened resource columns → resource attribute names. */
const RESOURCE_COLUMNS: Readonly<Record<string, string>> = {
  service_name: "service.name",
  service_version: "service.version",
  service_namespace: "service.namespace",
  service_instance_id: "service.instance.id",
  deployment_environment: "deployment.environment",
  deployment_environment_name: "deployment.environment.name",
  host_name: "host.name",
};

/**
 * Converts a {@link LogRecord} into the row written by `_json` ingestion: flattened attributes and
 * resource keys plus `_timestamp` (µs), `body`, `severity`, `severity_number`, `trace_id`,
 * `span_id`, `trace_flags`. Record fields win over attributes with the same column name.
 */
export function logRecordToRow(record: LogRecord): JsonObject {
  const row: JsonObject = {};
  for (const [k, v] of Object.entries(record.resource)) row[flattenKey(k)] = columnValue(v);
  for (const [k, v] of Object.entries(record.attributes)) row[flattenKey(k)] = columnValue(v);
  row._timestamp = toMicros(record.timestamp, "logging.export");
  row.body = record.body;
  row.severity = record.severityText;
  row.severity_number = record.severity;
  if (record.traceId !== undefined) row.trace_id = record.traceId;
  if (record.spanId !== undefined) row.span_id = record.spanId;
  if (record.traceFlags !== undefined) row.trace_flags = record.traceFlags;
  return row;
}

function columnValue(value: AttrValue): AttrValue {
  return Array.isArray(value) ? JSON.stringify(value) : value;
}

/**
 * Normalizes an OpenObserve row (from `_search`) into a {@link LogRecord}: `_timestamp` (µs),
 * `body`|`message`, `severity`|`level` (+ `severity_number`), `trace_id`, `span_id`,
 * `trace_flags`; well-known resource columns (`service_name`, `host_name`, …) go to `resource`,
 * every other column to `attributes` (flattened names as stored by the server).
 */
export function rowToLogRecord(row: JsonObject): LogRecord {
  const text = str(row.severity) ?? str(row.severity_text) ?? str(row.level);
  const number = num(row.severity_number, Number.NaN);
  const severity: Severity =
    (Number.isInteger(number) ? parseSeverity(number) : null) ??
    (text === undefined ? null : parseSeverity(text)) ??
    Severity.Info;
  const record: LogRecord = {
    timestamp: microsToDate(num(row._timestamp)),
    severity,
    severityText: text ?? severityText(severity),
    body: bodyOf(row),
    attributes: {},
    resource: {},
  };
  const traceId = str(row.trace_id);
  if (traceId !== undefined) record.traceId = traceId.toLowerCase();
  const spanId = str(row.span_id);
  if (spanId !== undefined) record.spanId = spanId.toLowerCase();
  const flags = num(row.trace_flags, Number.NaN);
  if (Number.isInteger(flags)) record.traceFlags = flags;
  const attributes: Attributes = {};
  const resource: Attributes = {};
  for (const [k, v] of Object.entries(row)) {
    if (RECORD_COLUMNS.has(k) || v === null || v === undefined) continue;
    const value = attrValue(v);
    const resourceKey = RESOURCE_COLUMNS[k];
    if (resourceKey !== undefined) resource[resourceKey] = value;
    else attributes[k] = value;
  }
  record.attributes = attributes;
  record.resource = resource;
  return record;
}

function bodyOf(row: JsonObject): string {
  for (const key of ["body", "message", "log", "msg"]) {
    const v = row[key];
    if (typeof v === "string") return v;
    if (v !== undefined && v !== null) return JSON.stringify(v);
  }
  return "";
}

function attrValue(value: unknown): AttrValue {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean")
    return value;
  return JSON.stringify(value);
}

// ---------------------------------------------------------------------------------------------
// OTLP/JSON (ExportLogsServiceRequest)
// ---------------------------------------------------------------------------------------------

/** OTLP `AnyValue` in JSON form. */
export type OtlpAnyValue =
  | { stringValue: string }
  | { boolValue: boolean }
  | { intValue: string }
  | { doubleValue: number }
  | { arrayValue: { values: OtlpAnyValue[] } }
  | { kvlistValue: { values: OtlpKeyValue[] } };

/** OTLP `KeyValue` in JSON form. */
export interface OtlpKeyValue {
  key: string;
  value: OtlpAnyValue;
}

/** Converts an attribute value into an OTLP `AnyValue`. */
export function otlpValue(value: AttrValue): OtlpAnyValue {
  if (typeof value === "string") return { stringValue: value };
  if (typeof value === "boolean") return { boolValue: value };
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? { intValue: String(value) } : { doubleValue: value };
  }
  if (Array.isArray(value)) return { arrayValue: { values: value.map(otlpValue) } };
  return { kvlistValue: { values: otlpAttributes(value) } };
}

/** Converts an attribute map into OTLP `KeyValue`s. */
export function otlpAttributes(attributes: Attributes): OtlpKeyValue[] {
  return Object.entries(attributes).map(([key, value]) => ({ key, value: otlpValue(value) }));
}

function nanos(date: Date): string {
  return (BigInt(date.getTime()) * 1_000_000n).toString();
}

/**
 * Builds an OTLP/JSON `ExportLogsServiceRequest` from records (grouped by resource), with the
 * instrumentation scope `grow-codes-openobserve`.
 */
export function toOtlpLogsRequest(records: readonly LogRecord[], scopeVersion: string): JsonObject {
  const groups = new Map<string, { resource: Attributes; logs: JsonObject[] }>();
  for (const record of records) {
    const key = JSON.stringify(record.resource);
    let group = groups.get(key);
    if (group === undefined) {
      group = { resource: record.resource, logs: [] };
      groups.set(key, group);
    }
    const log: JsonObject = {
      timeUnixNano: nanos(record.timestamp),
      severityNumber: record.severity,
      severityText: record.severityText,
      body: { stringValue: record.body },
      attributes: otlpAttributes(record.attributes),
    };
    if (record.observedTimestamp !== undefined)
      log.observedTimeUnixNano = nanos(record.observedTimestamp);
    if (record.traceId !== undefined) log.traceId = record.traceId;
    if (record.spanId !== undefined) log.spanId = record.spanId;
    if (record.traceFlags !== undefined) log.flags = record.traceFlags;
    group.logs.push(log);
  }
  return {
    resourceLogs: [...groups.values()].map((g) => ({
      resource: { attributes: otlpAttributes(g.resource) },
      scopeLogs: [
        { scope: { name: "grow-codes-openobserve", version: scopeVersion }, logRecords: g.logs },
      ],
    })),
  };
}
