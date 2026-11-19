// Service-owned. TraceQueryClient for OpenObserve (API.md §5, DECISIONS 4).

import { NotFoundError, ValidationError } from "../core/index.js";
import {
  BaseTraceQueryClient,
  normalizeTraceId,
  sortSpans,
  type Span,
  type SpanAttrValue,
  type SpanAttributes,
  type SpanKindName,
  type Trace,
  type TraceSearchQuery,
  type TraceSummary,
} from "../tracing/index.js";
import {
  type CallOptions,
  type ClientContext,
  type JsonObject,
  type Timestamp,
  arr,
  flattenKey,
  nsToMicros,
  num,
  obj,
  orgPath,
  parseJson,
  requestJson,
  sqlIdent,
  sqlString,
  str,
  streamName,
  window,
} from "../http.js";

/** Default window of `searchTraces`: the last hour. */
export const DEFAULT_TRACE_SEARCH_WINDOW_MS = 60 * 60_000;
/** Default window of `getTrace`: the last 7 days. */
export const DEFAULT_GET_TRACE_WINDOW_MS = 7 * 24 * 60 * 60_000;
/** Default window of `listServices` / `listOperations`: the last 24 hours. */
export const DEFAULT_SERVICES_WINDOW_MS = 24 * 60 * 60_000;

/** Options of `getTrace`. */
export interface GetTraceOptions extends CallOptions {
  /** Default: `end` − 7 days. */
  start?: Timestamp;
  /** Default: now. */
  end?: Timestamp;
}

/** Options of `listServices` / `listOperations`. */
export interface TraceWindowOptions extends CallOptions {
  /** Default: `end` − 24 hours. */
  start?: Timestamp;
  end?: Timestamp;
}

/** Result of `traceTimeRange`. */
export interface TraceTimeRange {
  found: boolean;
  /** Stream that holds the trace. */
  stream?: string;
  startMicros?: number;
  endMicros?: number;
}

const KINDS: Readonly<Record<string, SpanKindName>> = {
  "0": "unspecified",
  "1": "internal",
  "2": "server",
  "3": "client",
  "4": "producer",
  "5": "consumer",
};

/** Span row columns that are mapped to span fields or are internal to OpenObserve. */
const SPAN_COLUMNS: ReadonlySet<string> = new Set([
  "_timestamp",
  "_o2_ingest_ts",
  "trace_id",
  "span_id",
  "reference_parent_span_id",
  "reference_parent_trace_id",
  "reference_ref_type",
  "operation_name",
  "span_kind",
  "service_name",
  "start_time",
  "end_time",
  "duration",
  "span_status",
  "status_code",
  "status_message",
  "flags",
  "events",
  "links",
]);

function attrValue(value: unknown): SpanAttrValue {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean")
    return value;
  return JSON.stringify(value);
}

function attributesOf(source: JsonObject, skip: (key: string) => boolean): SpanAttributes {
  const out: SpanAttributes = {};
  for (const [k, v] of Object.entries(source)) {
    if (skip(k) || v === null || v === undefined) continue;
    out[k] = attrValue(v);
  }
  return out;
}

function jsonList(value: unknown): JsonObject[] {
  if (typeof value !== "string") return arr(value).map(obj);
  try {
    return arr(parseJson(value)).map(obj);
  } catch {
    return [];
  }
}

/**
 * Maps an OpenObserve span row to the normalized {@link Span}: `start_time`/`end_time` are
 * nanoseconds (exact when they arrive as strings), `span_kind` is a string number, `events` and
 * `links` are JSON strings. Internal columns (`_timestamp`, `infer_service_*`, `o2_db_*`, …) are
 * dropped; the rest become attributes with the server's flattened names.
 */
export function rowToSpan(row: JsonObject): Span {
  const startMicros = nsToMicros(row.start_time);
  const endMicros = nsToMicros(row.end_time);
  const statusText = (str(row.span_status) ?? "").toUpperCase();
  const statusCode = num(row.status_code);
  const code =
    statusText === "ERROR" || statusCode === 2
      ? "error"
      : statusText === "OK" || statusCode === 1
        ? "ok"
        : "unset";
  const status: Span["status"] = { code };
  const message = str(row.status_message);
  if (message !== undefined) status.message = message;
  const span: Span = {
    traceId: (str(row.trace_id) ?? "").toLowerCase(),
    spanId: (str(row.span_id) ?? "").toLowerCase(),
    name: str(row.operation_name) ?? "",
    kind: KINDS[scalarText(row.span_kind) ?? "0"] ?? "unspecified",
    serviceName: str(row.service_name) ?? "",
    startMicros,
    endMicros,
    durationMicros: endMicros >= startMicros ? endMicros - startMicros : num(row.duration),
    status,
    attributes: attributesOf(
      row,
      (k) => SPAN_COLUMNS.has(k) || k.startsWith("infer_service_") || k.startsWith("o2_db_"),
    ),
    events: jsonList(row.events).map((e) => ({
      name: str(e.name) ?? "",
      timeMicros: nsToMicros(e._timestamp),
      attributes: attributesOf(e, (k) => k === "name" || k === "_timestamp"),
    })),
    links: jsonList(row.links).map((l) => {
      const context = obj(l.context);
      return {
        traceId: (str(context.traceId) ?? str(context.trace_id) ?? "").toLowerCase(),
        spanId: (str(context.spanId) ?? str(context.span_id) ?? "").toLowerCase(),
        attributes: attributesOf(
          l,
          (k) =>
            k === "context" || k === "droppedAttributesCount" || k === "dropped_attributes_count",
        ),
      };
    }),
  };
  const parent = str(row.reference_parent_span_id);
  if (parent !== undefined) span.parentSpanId = parent.toLowerCase();
  return span;
}

/** Maps one `traces/latest` hit to a {@link TraceSummary}. */
export function hitToSummary(hit: JsonObject): TraceSummary {
  const first = obj(hit.first_event);
  const summary: TraceSummary = {
    traceId: (str(hit.trace_id) ?? "").toLowerCase(),
    startMicros: nsToMicros(hit.start_time),
    durationMicros: num(hit.duration),
    spanCount: num(arr(hit.spans)[0]),
  };
  const service = str(first.service_name);
  if (service !== undefined) summary.rootServiceName = service;
  const name = str(first.operation_name);
  if (name !== undefined) summary.rootName = name;
  return summary;
}

/** Builds the `traces/latest` filter (a SQL `WHERE` fragment) for a search query. */
export function traceFilter(query: TraceSearchQuery): string {
  const parts: string[] = [];
  if (query.service !== undefined) parts.push(`service_name = ${sqlString(query.service)}`);
  if (query.operation !== undefined) parts.push(`operation_name = ${sqlString(query.operation)}`);
  for (const [key, value] of Object.entries(query.tags ?? {})) {
    const column = flattenKey(key);
    if (column === "") throw new ValidationError("tracing.searchTraces: empty tag key");
    parts.push(`${column} = ${sqlString(value)}`);
  }
  for (const [value, op] of [
    [query.minDurationMicros, ">="],
    [query.maxDurationMicros, "<="],
  ] as const) {
    if (value === undefined) continue;
    if (!Number.isFinite(value) || value < 0) {
      throw new ValidationError("tracing.searchTraces: durations must be >= 0");
    }
    parts.push(`duration ${op} ${Math.trunc(value)}`);
  }
  return parts.join(" AND ");
}

/**
 * {@link TraceQueryClient} on one traces stream (default: the config's `tracesStream`).
 *
 * `searchTraces` uses `traces/latest`; with a filter each summary covers only the matching spans
 * (start, duration and span count), which keeps the call cheap. `getDependencies` throws
 * `UnsupportedError`: the service graph is an enterprise feature (404 in the OSS server).
 */
export class OpenObserveTraceQuery extends BaseTraceQueryClient {
  readonly #ctx: ClientContext;
  /** The traces stream. */
  readonly stream: string;

  /** @internal Use `client.traces` or `client.traceQuery(stream)`. */
  constructor(ctx: ClientContext, stream?: string) {
    super();
    this.#ctx = ctx;
    this.stream = streamName(stream, ctx.config.tracesStream, "tracing.query");
  }

  /**
   * All spans of a trace (`traces/{id}/details`, paginated), or `null` when the trace (or the
   * stream) does not exist in the window (default: the last 7 days).
   */
  async getTrace(traceId: string, opts: GetTraceOptions = {}): Promise<Trace | null> {
    const id = normalizeTraceId(traceId);
    const { startMicros, endMicros } = window(
      opts.start,
      opts.end,
      DEFAULT_GET_TRACE_WINDOW_MS,
      "tracing.getTrace",
    );
    const spans: Span[] = [];
    let after: { start: string; spanId: string } | undefined;
    for (let page = 0; page < 1000; page++) {
      let data: JsonObject;
      try {
        const res = await requestJson(this.#ctx, {
          path: orgPath(this.#ctx, this.stream, "traces", id, "details"),
          query: {
            start_time: startMicros,
            end_time: endMicros,
            size: 1000,
            after_start_time: after?.start,
            after_span_id: after?.spanId,
          },
          operation: `tracing.getTrace ${id}`,
          ...signal(opts),
        });
        data = obj(res.data);
      } catch (err) {
        if (err instanceof NotFoundError) return null;
        throw err;
      }
      spans.push(...arr(data.hits).map((h) => rowToSpan(obj(h))));
      const next = obj(data.next_after);
      const nextStart = scalarText(next.start_time);
      const nextSpan = str(next.span_id);
      if (data.has_more !== true || nextStart === undefined || nextSpan === undefined) break;
      after = { start: nextStart, spanId: nextSpan };
    }
    if (spans.length === 0) return null;
    const unique = new Map(spans.map((s) => [s.spanId, s]));
    return { traceId: id, spans: sortSpans([...unique.values()]) };
  }

  /** Recent traces matching the query (`traces/latest`); default window: the last hour. */
  async searchTraces(
    query: TraceSearchQuery = {},
    opts: CallOptions = {},
  ): Promise<TraceSummary[]> {
    const limit = Math.trunc(query.limit ?? 20);
    if (limit <= 0) throw new ValidationError("tracing.searchTraces: limit must be positive");
    const { startMicros, endMicros } = window(
      query.start,
      query.end,
      DEFAULT_TRACE_SEARCH_WINDOW_MS,
      "tracing.searchTraces",
    );
    const filter = traceFilter(query);
    let data: JsonObject;
    try {
      const res = await requestJson(this.#ctx, {
        path: orgPath(this.#ctx, this.stream, "traces", "latest"),
        query: {
          start_time: startMicros,
          end_time: endMicros,
          from: 0,
          size: limit,
          filter: filter === "" ? undefined : filter,
        },
        operation: "tracing.searchTraces",
        ...signal(opts),
      });
      data = obj(res.data);
    } catch (err) {
      if (err instanceof NotFoundError) return [];
      throw err;
    }
    return arr(data.hits)
      .map((h) => hitToSummary(obj(h)))
      .slice(0, limit);
  }

  /** Distinct `service_name` values in the window (default: the last 24 hours), sorted. */
  async listServices(opts: TraceWindowOptions = {}): Promise<string[]> {
    return this.#distinct("service_name", "", "tracing.listServices", opts);
  }

  /** Distinct operation names of one service in the window, sorted. */
  async listOperations(service: string, opts: TraceWindowOptions = {}): Promise<string[]> {
    return this.#distinct(
      "operation_name",
      ` WHERE service_name = ${sqlString(service)}`,
      "tracing.listOperations",
      opts,
    );
  }

  async #distinct(
    column: string,
    where: string,
    operation: string,
    opts: TraceWindowOptions,
  ): Promise<string[]> {
    const { startMicros, endMicros } = window(
      opts.start,
      opts.end,
      DEFAULT_SERVICES_WINDOW_MS,
      operation,
    );
    let hits: unknown[];
    try {
      const res = await requestJson(this.#ctx, {
        method: "POST",
        path: orgPath(this.#ctx, "_search"),
        query: { type: "traces" },
        json: {
          query: {
            sql: `SELECT DISTINCT ${column} FROM ${sqlIdent(this.stream)}${where} ORDER BY ${column}`,
            start_time: startMicros,
            end_time: endMicros,
            from: 0,
            size: 10_000,
          },
        },
        idempotent: true,
        operation,
        ...signal(opts),
      });
      hits = arr(obj(res.data).hits);
    } catch (err) {
      if (err instanceof NotFoundError) return [];
      throw err;
    }
    const values = hits.map((h) => str(obj(h)[column])).filter((v): v is string => v !== undefined);
    return [...new Set(values)].sort();
  }

  /** Where and when a trace was stored (`traces/time_range`, uses the server's trace index). */
  async traceTimeRange(traceId: string, opts: CallOptions = {}): Promise<TraceTimeRange> {
    const id = normalizeTraceId(traceId);
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, "traces", "time_range"),
      query: { trace_id: id },
      operation: `tracing.traceTimeRange ${id}`,
      ...signal(opts),
    });
    const result = arr(obj(res.data).results)
      .map(obj)
      .find((r) => str(r.status) === "found");
    if (result === undefined) return { found: false };
    const range = obj(result.range);
    const out: TraceTimeRange = {
      found: true,
      startMicros: num(range.start_time),
      endMicros: num(range.end_time),
    };
    const stream = str(result.stream);
    if (stream !== undefined) out.stream = stream;
    return out;
  }
}

function scalarText(value: unknown): string | undefined {
  return typeof value === "string" || typeof value === "number" ? String(value) : undefined;
}

function signal(opts: CallOptions): { signal?: AbortSignal } {
  return opts.signal === undefined ? {} : { signal: opts.signal };
}
