// Service-owned. LogExporter implementations for the category BatchingLogClient.

import { ValidationError } from "../core/index.js";
import type { ExportOptions, ExportResult, LogExporter, LogRecord } from "../logging/index.js";
import type { OpenObserveLogs } from "./logs.js";
import { logRecordToRow } from "./records.js";

/**
 * Exports records with `_json` ingestion into one stream (API.md §2 recommendation): per-stream
 * counters give exact accepted/rejected numbers.
 */
export class OpenObserveLogExporter implements LogExporter {
  readonly #logs: OpenObserveLogs;
  /** Target stream. */
  readonly stream: string;

  constructor(logs: OpenObserveLogs, stream: string) {
    this.#logs = logs;
    this.stream = stream;
  }

  async export(records: LogRecord[], opts: ExportOptions): Promise<ExportResult> {
    if (records.length === 0) return { accepted: 0, rejected: 0 };
    const result = await this.#logs.ingestJson(this.stream, records.map(logRecordToRow), {
      gzip: opts.gzip,
      ...(opts.signal === undefined ? {} : { signal: opts.signal }),
    });
    const rejected = Math.min(records.length, result.failed);
    const out: ExportResult = { accepted: records.length - rejected, rejected };
    if (rejected > 0) {
      out.error = new ValidationError(
        `logging.export ${this.stream}: ${rejected} records rejected` +
          (result.error === undefined ? "" : ` (${result.error.slice(0, 300)})`),
      );
    }
    return out;
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}

/** Exports records as OTLP/JSON to `/api/{org}/v1/logs` with the `stream-name` header. */
export class OpenObserveOtlpLogExporter implements LogExporter {
  readonly #logs: OpenObserveLogs;
  /** Target stream. */
  readonly stream: string;

  constructor(logs: OpenObserveLogs, stream: string) {
    this.#logs = logs;
    this.stream = stream;
  }

  async export(records: LogRecord[], opts: ExportOptions): Promise<ExportResult> {
    const result = await this.#logs.ingestOtlp(records, {
      stream: this.stream,
      gzip: opts.gzip,
      ...(opts.signal === undefined ? {} : { signal: opts.signal }),
    });
    const out: ExportResult = { ...result };
    if (result.rejected > 0) {
      out.error = new ValidationError(
        `logging.export ${this.stream}: ${result.rejected} records rejected`,
      );
    }
    return out;
  }

  shutdown(): Promise<void> {
    return Promise.resolve();
  }
}
