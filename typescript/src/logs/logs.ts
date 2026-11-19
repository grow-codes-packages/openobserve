// Service-owned. Logs ingestion (`_json`, `_multi`, `_bulk`, OTLP) and search (`_search`,
// `_around`, `_values`, histogram, polling tail) — API.md §2–§3.

import { gzipSync } from "node:zlib";
import { SDK_VERSION, ValidationError } from "../core/index.js";
import type { LogRecord, TailHandle } from "../logging/index.js";
import {
  type CallOptions,
  type ClientContext,
  type JsonObject,
  type StreamType,
  type Timestamp,
  arr,
  microsToDate,
  num,
  obj,
  orgPath,
  requestJson,
  sqlIdent,
  str,
  streamName,
  toMicros,
  window,
} from "../http.js";
import { toOtlpLogsRequest } from "./records.js";

/** Default search window when `start` is not given: the last 15 minutes. */
export const DEFAULT_SEARCH_WINDOW_MS = 15 * 60_000;

/** Options of the ingest calls. */
export interface IngestOptions extends CallOptions {
  /** Send the body with `Content-Encoding: gzip`. Default false. */
  gzip?: boolean;
}

/** Result of `ingestJson` / `ingestMulti`: per-stream counters (HTTP 200 even on partial failure). */
export interface IngestResult {
  /** Stream name as normalized by the server. */
  stream: string;
  successful: number;
  failed: number;
  /** Last per-record error reported by the server (e.g. "Too old data …"). */
  error?: string;
}

/** One `_bulk` action. Only `index` and `create` are accepted (OpenObserve ignores the others). */
export interface BulkAction {
  /** Default `index`. */
  action?: "index" | "create";
  /** Target stream (`_index`). */
  stream: string;
  document: JsonObject;
}

/** One `_bulk` result item. */
export interface BulkItem {
  action: string;
  stream: string;
  /** HTTP-like status of the item (200 = created). */
  status: number;
  error?: { type: string; reason: string };
}

/** Result of `ingestBulk`. Always check `errors`: item failures keep HTTP 200. */
export interface BulkResult {
  took: number;
  errors: boolean;
  items: BulkItem[];
}

/** Result of `ingestOtlp`. */
export interface OtlpIngestResult {
  accepted: number;
  rejected: number;
}

/** Options of `search`. Times are `Date`s or epoch microseconds. */
export interface SearchOptions extends CallOptions {
  /** Default: `end` − 15 minutes. */
  start?: Timestamp;
  /** Default: now. */
  end?: Timestamp;
  /** Row offset. Default 0. */
  from?: number;
  /** Page size. Default 100 (a SQL `LIMIT` overrides it). */
  size?: number;
  /** Also fetch the exact number of matching rows (one extra request) into `totalHits`. */
  trackTotalHits?: boolean;
  /** Stream type of the query. Default `logs`. */
  type?: StreamType;
}

/** Result of `search` and `searchAround`. */
export interface SearchResult {
  /** Flat rows; each has `_timestamp` (µs). */
  hits: JsonObject[];
  /** Rows in this page (the server's `total`). */
  total: number;
  /** Exact number of matching rows, only with `trackTotalHits`. */
  totalHits?: number;
  from: number;
  size: number;
  /** Server time in ms. */
  took: number;
  scanRecords: number;
  isPartial: boolean;
  /** Bucket width in seconds (histogram queries). */
  histogramInterval?: number;
  /** The server's search trace id. */
  requestId?: string;
}

/** Options of `searchAround`. */
export interface SearchAroundOptions extends CallOptions {
  /** Rows to return around the anchor (anchor included). Default 10. */
  size?: number;
  /** Optional `SELECT …` that filters the rows. */
  sql?: string;
  type?: StreamType;
}

/** Options of `valuesOf`. */
export interface ValuesOptions extends CallOptions {
  start?: Timestamp;
  end?: Timestamp;
  /** Values per field. Default 10. */
  size?: number;
  type?: StreamType;
}

/** Distinct values of one field, most frequent first. */
export interface FieldValues {
  field: string;
  values: { value: string | number | boolean; count: number }[];
}

/** Options of `histogram`. */
export interface HistogramOptions extends CallOptions {
  start?: Timestamp;
  end?: Timestamp;
  /** Bucket width such as `"1 minute"` or `"10 second"`. Default: chosen by the server. */
  interval?: string;
  /** SQL `WHERE` condition, e.g. `level = 'error'`. */
  filter?: string;
  type?: StreamType;
}

/** One histogram bucket. */
export interface HistogramBucket {
  /** Bucket start (UTC). */
  start: Date;
  /** The server's bucket key (naive UTC ISO string). */
  key: string;
  count: number;
}

/** Result of `histogram`. */
export interface Histogram {
  buckets: HistogramBucket[];
  /** Bucket width in seconds. */
  intervalSeconds?: number;
}

/** Options of `tail`. */
export interface LogTailOptions {
  /** SQL `WHERE` condition, e.g. `match_all('error')`. */
  filter?: string;
  /** Only rows at or after this time. Default: now. */
  start?: Timestamp;
  /** Poll interval. Default 1000 ms. */
  intervalMs?: number;
  /** Re-read window that catches late rows; duplicates are removed. Default 1000 ms. */
  overlapMs?: number;
  /** Rows per poll request. Default 1000. */
  pageSize?: number;
  /** Called on query errors; tailing continues. */
  onError?: (error: unknown) => void;
  type?: StreamType;
}

/** The OpenObserve logs module: `client.logs`. */
export class OpenObserveLogs {
  readonly #ctx: ClientContext;

  /** @internal Use `client.logs`. */
  constructor(ctx: ClientContext) {
    this.#ctx = ctx;
  }

  /**
   * Writes records with `POST /api/{org}/{stream}/_json`. Give `_timestamp` in epoch µs (ms, ns
   * and RFC 3339 are auto-detected by the server); without it the server uses its own time.
   */
  async ingestJson(
    stream: string,
    records: readonly JsonObject[] | JsonObject,
    opts: IngestOptions = {},
  ): Promise<IngestResult> {
    const name = streamName(stream, "", "logs.ingestJson");
    const list = Array.isArray(records) ? records : [records];
    return this.#ingest("_json", name, JSON.stringify(list), "application/json", opts);
  }

  /**
   * Writes newline-delimited JSON with `POST /api/{org}/{stream}/_multi`. One bad line rejects
   * the whole request (`ValidationError`).
   */
  async ingestMulti(
    stream: string,
    ndjson: string | readonly JsonObject[],
    opts: IngestOptions = {},
  ): Promise<IngestResult> {
    const name = streamName(stream, "", "logs.ingestMulti");
    const body =
      typeof ndjson === "string" ? ndjson : ndjson.map((r) => JSON.stringify(r)).join("\n") + "\n";
    return this.#ingest("_multi", name, body, "application/x-ndjson", opts);
  }

  async #ingest(
    endpoint: string,
    stream: string,
    body: string,
    contentType: string,
    opts: IngestOptions,
  ): Promise<IngestResult> {
    const operation = `logs.ingest ${stream}`;
    const res = await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, stream, endpoint),
      ...encodeBody(body, contentType, opts.gzip === true),
      operation,
      ...signal(opts),
    });
    const status = arr(obj(res.data).status).map(obj);
    const first = status.find((s) => s.name === stream) ?? status[0] ?? {};
    const result: IngestResult = {
      stream: str(first.name) ?? stream,
      successful: num(first.successful),
      failed: num(first.failed),
    };
    const error = str(first.error);
    if (error !== undefined) result.error = this.#ctx.transport.redact(error);
    return result;
  }

  /**
   * Elasticsearch-compatible bulk write: `POST /api/{org}/_bulk`. Accepts structured actions or a
   * raw NDJSON body. `delete` and `update` actions are rejected client-side with
   * `ValidationError` because the server silently ignores them.
   */
  async ingestBulk(
    actions: readonly BulkAction[] | string,
    opts: IngestOptions = {},
  ): Promise<BulkResult> {
    const body = typeof actions === "string" ? checkRawBulk(actions) : bulkBody(actions);
    const res = await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, "_bulk"),
      ...encodeBody(body, "application/x-ndjson", opts.gzip === true),
      operation: "logs.ingestBulk",
      ...signal(opts),
    });
    const data = obj(res.data);
    const items: BulkItem[] = arr(data.items).map((raw) => {
      const entry = Object.entries(obj(raw))[0] ?? ["index", {}];
      const item = obj(entry[1]);
      const out: BulkItem = {
        action: entry[0],
        stream: str(item._index) ?? "",
        status: num(item.status, 200),
      };
      if (item.error !== undefined && item.error !== null) {
        const err = obj(item.error);
        out.error = {
          type: str(err.type) ?? "error",
          reason: this.#ctx.transport.redact(str(err.reason) ?? ""),
        };
      }
      return out;
    });
    return {
      took: num(data.took),
      errors: data.errors === true || items.some((i) => i.status >= 300),
      items,
    };
  }

  /**
   * Writes SDK log records as OTLP/JSON (`POST /api/{org}/v1/logs`, header `stream-name`). The
   * server stores `body`, `severity`, `severity_number`, `trace_id`, `span_id` and flattened
   * attributes; trace flags are not stored.
   */
  async ingestOtlp(
    records: readonly LogRecord[],
    opts: IngestOptions & { stream?: string } = {},
  ): Promise<OtlpIngestResult> {
    const stream = streamName(opts.stream, this.#ctx.config.stream, "logs.ingestOtlp");
    if (records.length === 0) return { accepted: 0, rejected: 0 };
    const body = JSON.stringify(toOtlpLogsRequest(records, SDK_VERSION));
    const res = await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, "v1", "logs"),
      ...encodeBody(body, "application/json", opts.gzip === true, { "stream-name": stream }),
      operation: `logs.ingestOtlp ${stream}`,
      ...signal(opts),
    });
    const partial = obj(obj(res.data).partialSuccess ?? obj(res.data).partial_success);
    const rejected = Math.min(
      records.length,
      num(partial.rejectedLogRecords ?? partial.rejected_log_records),
    );
    return { accepted: records.length - rejected, rejected };
  }

  /**
   * Runs a SQL query: `POST /api/{org}/_search?type=logs`. Quote stream names with double quotes
   * (`SELECT * FROM "default"`); without `ORDER BY` rows come newest first. Times are converted to
   * microseconds (the server silently returns nothing for millisecond values).
   */
  async search(sql: string, opts: SearchOptions = {}): Promise<SearchResult> {
    if (sql.trim() === "") throw new ValidationError("logs.search: sql must not be empty");
    const { startMicros, endMicros } = window(
      opts.start,
      opts.end,
      DEFAULT_SEARCH_WINDOW_MS,
      "logs.search",
    );
    const query = {
      sql,
      start_time: startMicros,
      end_time: endMicros,
      from: Math.max(0, Math.trunc(opts.from ?? 0)),
      size: Math.trunc(opts.size ?? 100),
    };
    const type = opts.type ?? "logs";
    const result = await this.#search(query, type, opts);
    if (opts.trackTotalHits === true) {
      // The count comes back as the only row, so it must not be skipped by `from`.
      const counted = await this.#search({ ...query, from: 0, track_total_hits: true }, type, opts);
      result.totalHits = num(obj(counted.hits[0]).zo_sql_num, counted.total);
    }
    return result;
  }

  async #search(query: JsonObject, type: StreamType, opts: CallOptions): Promise<SearchResult> {
    const res = await requestJson(this.#ctx, {
      method: "POST",
      path: orgPath(this.#ctx, "_search"),
      query: { type },
      json: { query },
      idempotent: true,
      operation: "logs.search",
      ...signal(opts),
    });
    return searchResult(obj(res.data));
  }

  /**
   * Rows around one timestamp: `GET /api/{org}/{stream}/_around`. The anchor is a `Date` or the
   * exact `_timestamp` (µs) of a row; the anchor row is included.
   */
  async searchAround(
    stream: string,
    timestamp: Timestamp,
    opts: SearchAroundOptions = {},
  ): Promise<SearchResult> {
    const name = streamName(stream, "", "logs.searchAround");
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, name, "_around"),
      query: {
        key: toMicros(timestamp, "logs.searchAround"),
        size: Math.trunc(opts.size ?? 10),
        type: opts.type ?? "logs",
        sql: opts.sql === undefined ? undefined : Buffer.from(opts.sql).toString("base64url"),
      },
      operation: `logs.searchAround ${name}`,
      ...signal(opts),
    });
    return searchResult(obj(res.data));
  }

  /** Distinct values with counts: `GET /api/{org}/{stream}/_values?fields=a,b`. */
  async valuesOf(
    stream: string,
    fields: readonly string[],
    opts: ValuesOptions = {},
  ): Promise<FieldValues[]> {
    const name = streamName(stream, "", "logs.valuesOf");
    if (fields.length === 0) throw new ValidationError("logs.valuesOf: fields must not be empty");
    const { startMicros, endMicros } = window(
      opts.start,
      opts.end,
      DEFAULT_SEARCH_WINDOW_MS,
      "logs.valuesOf",
    );
    const res = await requestJson(this.#ctx, {
      path: orgPath(this.#ctx, name, "_values"),
      query: {
        fields: fields.join(","),
        start_time: startMicros,
        end_time: endMicros,
        size: Math.trunc(opts.size ?? 10),
        type: opts.type ?? "logs",
      },
      operation: `logs.valuesOf ${name}`,
      ...signal(opts),
    });
    return arr(obj(res.data).hits).map((raw) => {
      const hit = obj(raw);
      return {
        field: str(hit.field) ?? "",
        values: arr(hit.values).map((v) => {
          const entry = obj(v);
          const key = entry.zo_sql_key;
          return {
            value:
              typeof key === "string" || typeof key === "number" || typeof key === "boolean"
                ? key
                : JSON.stringify(key),
            count: num(entry.zo_sql_num),
          };
        }),
      };
    });
  }

  /** Record counts per time bucket (a `histogram(_timestamp, …)` query via `_search`). */
  async histogram(stream: string, opts: HistogramOptions = {}): Promise<Histogram> {
    const name = streamName(stream, "", "logs.histogram");
    const interval = opts.interval === undefined ? "" : `, '${checkInterval(opts.interval)}'`;
    const where = opts.filter === undefined ? "" : ` WHERE ${opts.filter}`;
    const sql =
      `SELECT histogram(_timestamp${interval}) AS zo_sql_key, count(*) AS zo_sql_num ` +
      `FROM ${sqlIdent(name)}${where} GROUP BY zo_sql_key ORDER BY zo_sql_key`;
    const searchOpts: SearchOptions = { size: -1, type: opts.type ?? "logs" };
    if (opts.start !== undefined) searchOpts.start = opts.start;
    if (opts.end !== undefined) searchOpts.end = opts.end;
    if (opts.signal !== undefined) searchOpts.signal = opts.signal;
    const result = await this.search(sql, searchOpts);
    const out: Histogram = {
      buckets: result.hits.map((hit) => {
        const key = str(hit.zo_sql_key) ?? "";
        return { start: parseNaiveUtc(key), key, count: num(hit.zo_sql_num) };
      }),
    };
    if (result.histogramInterval !== undefined) out.intervalSeconds = result.histogramInterval;
    return out;
  }

  /**
   * Follows a stream by polling `_search` with a `_timestamp` cursor (OpenObserve has no live
   * tail). Every poll re-reads `overlapMs` before the cursor (never before `start`) and drops rows
   * it already delivered (by full-row equality), so rows that are exact duplicates within that
   * window are delivered once.
   */
  tail(stream: string, onRow: (row: JsonObject) => void, opts: LogTailOptions = {}): TailHandle {
    const name = streamName(stream, "", "logs.tail");
    const intervalMs = opts.intervalMs ?? 1000;
    const overlap = Math.max(0, opts.overlapMs ?? 1000) * 1000;
    const pageSize = Math.max(1, opts.pageSize ?? 1000);
    // Rows before `floor` (the requested start) are never delivered: the overlap window stops there.
    const floor = opts.start === undefined ? Date.now() * 1000 : toMicros(opts.start, "logs.tail");
    let cursor = floor;
    const seen = new Map<string, number>();
    const controller = new AbortController();
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const poll = async (): Promise<void> => {
      for (;;) {
        const from = Math.max(floor, cursor - overlap);
        const where = opts.filter === undefined ? "" : ` AND (${opts.filter})`;
        const sql = `SELECT * FROM ${sqlIdent(name)} WHERE _timestamp >= ${from}${where} ORDER BY _timestamp ASC`;
        const page = await this.search(sql, {
          start: from,
          end: (Date.now() + 60_000) * 1000,
          size: pageSize,
          type: opts.type ?? "logs",
          signal: controller.signal,
        });
        let fresh = 0;
        for (const row of page.hits) {
          if (stopped) return;
          const key = JSON.stringify(row);
          if (seen.has(key)) continue;
          const ts = num(row._timestamp);
          seen.set(key, ts);
          fresh++;
          if (ts > cursor) cursor = ts;
          onRow(row);
        }
        for (const [key, ts] of seen) if (ts < cursor - overlap) seen.delete(key);
        if (page.hits.length < pageSize || fresh === 0) return;
      }
    };
    const tick = async (): Promise<void> => {
      try {
        await poll();
      } catch (error) {
        if (!stopped) opts.onError?.(error);
      }
      if (!stopped) schedule();
    };
    const schedule = (): void => {
      timer = setTimeout(() => void tick(), intervalMs);
      timer.unref();
    };
    schedule();
    return {
      stop(): void {
        stopped = true;
        clearTimeout(timer);
        controller.abort();
      },
    };
  }
}

function signal(opts: CallOptions): { signal?: AbortSignal } {
  return opts.signal === undefined ? {} : { signal: opts.signal };
}

function encodeBody(
  body: string,
  contentType: string,
  gzip: boolean,
  extra: Record<string, string> = {},
): { body: string | Uint8Array; headers: Record<string, string> } {
  const headers: Record<string, string> = { "content-type": contentType, ...extra };
  if (!gzip) return { body, headers };
  headers["content-encoding"] = "gzip";
  return { body: new Uint8Array(gzipSync(body)), headers };
}

function bulkBody(actions: readonly BulkAction[]): string {
  if (actions.length === 0) throw new ValidationError("logs.ingestBulk: no actions");
  const lines: string[] = [];
  for (const a of actions) {
    const action: string = a.action ?? "index";
    if (action !== "index" && action !== "create") {
      throw new ValidationError(
        `logs.ingestBulk: unsupported action "${action}" (only index and create)`,
      );
    }
    const stream = streamName(a.stream, "", "logs.ingestBulk");
    lines.push(JSON.stringify({ [action]: { _index: stream } }), JSON.stringify(a.document));
  }
  return lines.join("\n") + "\n";
}

function checkRawBulk(body: string): string {
  const lines = body.split("\n").filter((l) => l.trim() !== "");
  if (lines.length === 0) throw new ValidationError("logs.ingestBulk: no actions");
  for (let i = 0; i < lines.length; i += 2) {
    let action: unknown;
    try {
      action = JSON.parse(lines[i] ?? "");
    } catch {
      throw new ValidationError(`logs.ingestBulk: line ${i + 1} is not JSON`);
    }
    const name = Object.keys(obj(action))[0];
    if (name !== "index" && name !== "create") {
      throw new ValidationError(
        `logs.ingestBulk: unsupported action "${name ?? ""}" on line ${i + 1} (only index and create)`,
      );
    }
  }
  return body.endsWith("\n") ? body : body + "\n";
}

function searchResult(data: JsonObject): SearchResult {
  const result: SearchResult = {
    hits: arr(data.hits).map(obj),
    total: num(data.total),
    from: num(data.from),
    size: num(data.size),
    took: num(data.took),
    scanRecords: num(data.scan_records),
    isPartial: data.is_partial === true,
  };
  if (typeof data.histogram_interval === "number")
    result.histogramInterval = data.histogram_interval;
  const id = str(data.trace_id);
  if (id !== undefined) result.requestId = id;
  return result;
}

function checkInterval(interval: string): string {
  if (!/^\s*\d+\s*[a-z]+\s*$/i.test(interval)) {
    throw new ValidationError('logs.histogram: interval must look like "1 minute"');
  }
  return interval.trim();
}

function parseNaiveUtc(key: string): Date {
  const iso = /[zZ]|[+-]\d\d:?\d\d$/.test(key) ? key : `${key}Z`;
  const date = new Date(iso);
  return Number.isNaN(date.getTime()) ? microsToDate(0) : date;
}
