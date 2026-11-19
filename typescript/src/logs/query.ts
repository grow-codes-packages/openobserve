// Service-owned. LogQueryClient for OpenObserve (DECISIONS 2 and 3).

import {
  BaseLogQueryClient,
  type LogRecord,
  type LogSearchOptions,
  type LogSearchResult,
  type TailHandle,
  type TailOptions,
} from "../logging/index.js";
import { ValidationError } from "../core/index.js";
import { sqlIdent, sqlString, streamName } from "../http.js";
import type { OpenObserveLogs, SearchOptions } from "./logs.js";
import type { OpenObserveStreams } from "./streams.js";
import { rowToLogRecord } from "./records.js";

/** `search()` options of {@link OpenObserveLogQuery}. */
export interface OpenObserveLogSearchOptions extends LogSearchOptions {
  /** Also fetch the exact number of matches into `total` (one extra request). */
  trackTotalHits?: boolean;
}

/** True when `query` is raw SQL (starts with `SELECT`, case-insensitive). */
export function isSqlQuery(query: string): boolean {
  return /^\s*select\s/i.test(query);
}

/**
 * Builds the SQL for a text query: `SELECT * FROM "<stream>" WHERE match_all('<text>') ORDER BY
 * _timestamp <order>`; an empty text matches every row.
 */
export function textQuerySql(stream: string, text: string, order: "asc" | "desc"): string {
  const where = text.trim() === "" ? "" : ` WHERE match_all(${sqlString(text)})`;
  return `SELECT * FROM ${sqlIdent(stream)}${where} ORDER BY _timestamp ${order.toUpperCase()}`;
}

/**
 * {@link LogQueryClient} on one stream (default: the config's `stream`). A query that starts with
 * `SELECT` is raw SQL; any other text becomes `match_all('<text>')` (full-text search over the
 * stream's full-text keys, case-insensitive). The cursor is a row offset.
 */
export class OpenObserveLogQuery extends BaseLogQueryClient {
  readonly #logs: OpenObserveLogs;
  readonly #streams: OpenObserveStreams;
  /** The stream queried by text queries, `listLabels` and `tail`. */
  readonly stream: string;

  /** @internal Use `client.logQuery()`. */
  constructor(logs: OpenObserveLogs, streams: OpenObserveStreams, stream: string) {
    super();
    this.#logs = logs;
    this.#streams = streams;
    this.stream = streamName(stream, "", "logging.query");
  }

  /** Searches records; default window: the last 15 minutes, newest first, 100 per page. */
  async search(query: string, opts: OpenObserveLogSearchOptions = {}): Promise<LogSearchResult> {
    const limit = Math.trunc(opts.limit ?? 100);
    if (limit <= 0) throw new ValidationError("logging.search: limit must be positive");
    const offset = opts.cursor === undefined ? 0 : Number(opts.cursor);
    if (!Number.isSafeInteger(offset) || offset < 0) {
      throw new ValidationError("logging.search: invalid cursor");
    }
    const sql = isSqlQuery(query) ? query : textQuerySql(this.stream, query, opts.order ?? "desc");
    const searchOpts: SearchOptions = { from: offset, size: limit };
    if (opts.start !== undefined) searchOpts.start = opts.start;
    if (opts.end !== undefined) searchOpts.end = opts.end;
    if (opts.signal !== undefined) searchOpts.signal = opts.signal;
    if (opts.trackTotalHits === true) searchOpts.trackTotalHits = true;
    const result = await this.#logs.search(sql, searchOpts);
    const out: LogSearchResult = { records: result.hits.map(rowToLogRecord) };
    if (result.hits.length >= limit) out.nextCursor = String(offset + result.hits.length);
    if (result.totalHits !== undefined) out.total = result.totalHits;
    return out;
  }

  /**
   * Follows new records. Text queries use the cursor-based `logs.tail` on {@link stream}; raw
   * SQL queries use the generic polling tail on top of {@link search}.
   */
  override tail(
    query: string,
    onRecord: (record: LogRecord) => void,
    opts: TailOptions = {},
  ): TailHandle {
    if (isSqlQuery(query)) return super.tail(query, onRecord, opts);
    return this.#logs.tail(
      this.stream,
      (row) => {
        onRecord(rowToLogRecord(row));
      },
      {
        ...(query.trim() === "" ? {} : { filter: `match_all(${sqlString(query)})` }),
        ...(opts.start === undefined ? {} : { start: opts.start }),
        ...(opts.intervalMs === undefined ? {} : { intervalMs: opts.intervalMs }),
        ...(opts.onError === undefined ? {} : { onError: opts.onError }),
      },
    );
  }

  /** Names of the log streams of the organization. */
  override async listStreams(): Promise<string[]> {
    return (await this.#streams.listStreams("logs")).map((s) => s.name);
  }

  /** Field names of {@link stream} (from its schema). */
  override async listLabels(): Promise<string[]> {
    return (await this.#streams.getStreamSchema(this.stream)).schema.map((f) => f.name);
  }
}
