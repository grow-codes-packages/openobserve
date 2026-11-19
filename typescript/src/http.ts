// Service-owned. Shared helpers of the OpenObserve service modules: JSON decoding that keeps
// 64-bit integers exact, error remapping of vendor codes, time-unit and SQL helpers.

import {
  ConflictError,
  type HttpTransport,
  NotFoundError,
  OpenObserveError,
  type RequestOptions,
  type TransportResponse,
  ValidationError,
} from "./core/index.js";
import { type OpenObserveConfig, normalizeStreamName } from "./config.js";

/** Per-call options of every I/O method. */
export interface CallOptions {
  /** Cancels the call (`CancelledError`). */
  signal?: AbortSignal;
}

/** What a service module needs from its client. @internal */
export interface ClientContext {
  readonly transport: HttpTransport;
  readonly config: OpenObserveConfig;
}

/** Stream types of OpenObserve. */
export type StreamType = "logs" | "traces" | "metrics";

/** A JSON object as decoded by {@link parseJson}. */
export type JsonObject = Record<string, unknown>;

/**
 * Parses JSON like `JSON.parse`, but integers outside the safe range (nanosecond timestamps,
 * 64-bit counters) are returned as their exact decimal **string** instead of a rounded number.
 * Empty input returns `undefined`.
 */
export function parseJson(text: string): unknown {
  if (text.trim() === "") return undefined;
  return JSON.parse(
    text,
    function (_key: string, value: unknown, context?: { source?: string }): unknown {
      if (
        typeof value === "number" &&
        !Number.isSafeInteger(value) &&
        Number.isInteger(value) &&
        context?.source !== undefined &&
        /^-?\d+$/.test(context.source)
      ) {
        return context.source;
      }
      return value;
    },
  );
}

/**
 * Remaps an error from the transport to the OpenObserve-specific class: vendor code 20002 /
 * "stream not found" (HTTP 400) → `NotFoundError`, "already exists" (HTTP 400) → `ConflictError`.
 */
export function remapError(err: unknown): unknown {
  if (!(err instanceof OpenObserveError) || err.httpStatus !== 400) return err;
  const details = {
    httpStatus: 400,
    ...(err.requestId === undefined ? {} : { requestId: err.requestId }),
    cause: err,
  };
  if (/stream not found/i.test(err.message)) return new NotFoundError(err.message, details);
  if (/already exists/i.test(err.message)) return new ConflictError(err.message, details);
  return err;
}

/** Sends a request and decodes the JSON body with {@link parseJson}; remaps errors. */
export async function requestJson<T>(
  ctx: ClientContext,
  options: RequestOptions,
): Promise<TransportResponse<T | undefined>> {
  let res: TransportResponse;
  try {
    res = await ctx.transport.request({ ...options, responseType: "text" });
  } catch (err) {
    throw remapError(err);
  }
  let data: unknown;
  try {
    data = typeof res.data === "string" ? parseJson(res.data) : undefined;
  } catch {
    throw new ValidationError(
      `${options.operation ?? "request"}: invalid JSON in response (HTTP ${res.status})`,
      { httpStatus: res.status },
    );
  }
  return { ...res, data: data as T | undefined };
}

/** `/api/{org}/…` with every segment URL-encoded. */
export function orgPath(ctx: ClientContext, ...segments: string[]): string {
  return ["", "api", ctx.config.org, ...segments].map(encodeURIComponent).join("/");
}

/** Normalizes and validates a stream name (throws `ValidationError` when blank). */
export function streamName(name: string | undefined, fallback: string, operation: string): string {
  const normalized = normalizeStreamName(name ?? fallback);
  if (normalized === "") throw new ValidationError(`${operation}: stream name must not be blank`);
  return normalized;
}

/** A timestamp: a `Date`, or epoch **microseconds** as a number/bigint (exact row timestamps). */
export type Timestamp = Date | number | bigint;

/** Converts a {@link Timestamp} to epoch microseconds. */
export function toMicros(value: Timestamp, operation = "time"): number {
  if (value instanceof Date) {
    const ms = value.getTime();
    if (Number.isNaN(ms)) throw new ValidationError(`${operation}: invalid Date`);
    return ms * 1000;
  }
  const n = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isFinite(n)) throw new ValidationError(`${operation}: invalid timestamp`);
  return Math.trunc(n);
}

/** Converts epoch microseconds to a `Date` (millisecond precision). */
export function microsToDate(micros: number): Date {
  return new Date(Math.floor(micros / 1000));
}

/**
 * Converts a nanosecond value (number or exact decimal string) to microseconds, exactly when a
 * string is given. Returns `0` for anything unparsable.
 */
export function nsToMicros(value: unknown): number {
  if (typeof value === "string" && /^-?\d+$/.test(value)) return Number(BigInt(value) / 1000n);
  if (typeof value === "number" && Number.isFinite(value)) return Math.floor(value / 1000);
  return 0;
}

/** A number from a JSON value (numbers and numeric strings); `fallback` otherwise. */
export function num(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value)))
    return Number(value);
  return fallback;
}

/** A string from a JSON value; `undefined` for anything else (or empty strings). */
export function str(value: unknown): string | undefined {
  return typeof value === "string" && value !== "" ? value : undefined;
}

/** An object from a JSON value; `{}` for anything else. */
export function obj(value: unknown): JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as JsonObject)
    : {};
}

/** An array from a JSON value; `[]` for anything else. */
export function arr(value: unknown): unknown[] {
  return Array.isArray(value) ? (value as unknown[]) : [];
}

/** SQL string literal with single quotes doubled. */
export function sqlString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/** SQL identifier in double quotes (quotes doubled), e.g. `"default"`. */
export function sqlIdent(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

/** Flattens an attribute key like the server does (`http.method` → `http_method`). */
export function flattenKey(key: string): string {
  return key.toLowerCase().replace(/[^a-z0-9_]/g, "_");
}

/** Default search window: the last `ms` milliseconds up to now. */
export function window(
  start: Timestamp | undefined,
  end: Timestamp | undefined,
  defaultMs: number,
  operation: string,
  now: () => number = Date.now,
): { startMicros: number; endMicros: number } {
  const endMicros = end === undefined ? now() * 1000 + 60_000_000 : toMicros(end, operation);
  const startMicros =
    start === undefined ? endMicros - defaultMs * 1000 - 60_000_000 : toMicros(start, operation);
  if (startMicros > endMicros) throw new ValidationError(`${operation}: start is after end`);
  return { startMicros, endMicros };
}
