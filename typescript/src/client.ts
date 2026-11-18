// Service-owned: the OpenObserve client (API.md §1) and its service modules.

import {
  type HealthStatus,
  HttpTransport,
  type TransportOptions,
  UnsupportedError,
  healthStatus,
  probeHealth,
} from "./core/index.js";
import { BatchingLogClient, type BatchingLogClientOptions } from "./logging/index.js";
import { OpenObserveConfig, type OpenObserveConfigOptions } from "./config.js";
import { type CallOptions, type ClientContext, obj, orgPath, requestJson, str } from "./http.js";
import { OpenObserveAdmin } from "./admin.js";
import { OpenObserveLogExporter, OpenObserveOtlpLogExporter } from "./logs/exporter.js";
import { OpenObserveLogs } from "./logs/logs.js";
import { OpenObserveLogQuery } from "./logs/query.js";
import { OpenObserveStreams } from "./logs/streams.js";
import { OpenObserveTraceQuery } from "./traces/query.js";

export type { CallOptions } from "./http.js";

/** Test seams (undici `MockAgent`, fake sleep). Not part of the stable API. */
export type ClientInternals = Pick<TransportOptions, "dispatcher" | "sleep" | "now">;

/** Options of {@link OpenObserveClient.logClient}. */
export interface OpenObserveLogClientOptions extends BatchingLogClientOptions {
  /** Target stream. Default: the config's `stream`. */
  stream?: string;
  /** `json` (`_json` ingestion, default) or `otlp` (OTLP/JSON to `/v1/logs`). */
  protocol?: "json" | "otlp";
}

/** Client for OpenObserve. */
export class OpenObserveClient implements ClientContext {
  /** The immutable configuration. */
  readonly config: OpenObserveConfig;
  /** Logs ingestion and search. */
  readonly logs: OpenObserveLogs;
  /** Stream management. */
  readonly streams: OpenObserveStreams;
  /** Trace queries on the config's `tracesStream`. */
  readonly traces: OpenObserveTraceQuery;
  /** Organizations, service accounts and passcode. */
  readonly admin: OpenObserveAdmin;
  readonly #transport: HttpTransport;
  readonly #logClients = new Set<BatchingLogClient>();
  #closed: Promise<void> | null = null;

  /** Accepts a {@link OpenObserveConfig} or plain options. */
  constructor(config: OpenObserveConfigOptions = {}, internals: ClientInternals = {}) {
    const cfg = OpenObserveConfig.from(config);
    this.config = cfg;
    this.#transport = new HttpTransport({
      ...internals,
      config: cfg,
      defaultHeaders: (): Record<string, string> => {
        const auth = cfg.authorization();
        return auth === undefined ? {} : { authorization: auth };
      },
      knownSecrets: () => cfg.knownSecrets(),
    });
    this.logs = new OpenObserveLogs(this);
    this.streams = new OpenObserveStreams(this);
    this.traces = new OpenObserveTraceQuery(this);
    this.admin = new OpenObserveAdmin(this);
  }

  /** Builds a client from `GROW_OPENOBSERVE_*` / `OPENOBSERVE_*` environment variables. */
  static fromEnv(overrides: OpenObserveConfigOptions = {}): OpenObserveClient {
    return new OpenObserveClient(OpenObserveConfig.fromEnv(overrides));
  }

  /** The shared HTTP transport (for service modules). @internal */
  get transport(): HttpTransport {
    return this.#transport;
  }

  /**
   * Unauthenticated liveness: `GET /healthz` → `{"status":"ok"}`. Never throws for an unhealthy
   * or unreachable service (only `CANCELLED` and `CONFIG`).
   */
  health(options: CallOptions = {}): Promise<HealthStatus> {
    return probeHealth(async () => {
      const res = await requestJson(this, { path: "/healthz", operation: "health", ...options });
      const { status, ...details } = obj(res.data);
      return healthStatus(typeof status === "string" ? status : "ok", details);
    });
  }

  /**
   * Authenticated round trip: `GET /api/{org}/config`. Returns the latency in milliseconds;
   * `AuthenticationError` on bad or missing credentials.
   */
  async ping(options: CallOptions = {}): Promise<number> {
    const start = performance.now();
    await this.#transport.get(orgPath(this, "config"), {
      operation: "ping",
      responseType: "none",
      ...options,
    });
    return performance.now() - start;
  }

  /** Server version from the authenticated `GET /api/{org}/config` (e.g. `v1.1.2`). */
  async serverVersion(options: CallOptions = {}): Promise<string> {
    const res = await requestJson(this, {
      path: orgPath(this, "config"),
      operation: "serverVersion",
      ...options,
    });
    const version = str(obj(res.data).version);
    if (version === undefined) {
      throw new UnsupportedError("serverVersion: the service does not report a version");
    }
    return version;
  }

  /**
   * A batching {@link LogClient} that exports to one stream. The client is flushed and closed by
   * {@link close}.
   */
  logClient(options: OpenObserveLogClientOptions = {}): BatchingLogClient {
    const { stream, protocol, ...batching } = options;
    const target = stream ?? this.config.stream;
    const exporter =
      protocol === "otlp"
        ? new OpenObserveOtlpLogExporter(this.logs, target)
        : new OpenObserveLogExporter(this.logs, target);
    const client = new BatchingLogClient(exporter, batching);
    this.#logClients.add(client);
    return client;
  }

  /** A {@link LogQueryClient} on a stream (default: the config's `stream`). */
  logQuery(stream: string = this.config.stream): OpenObserveLogQuery {
    return new OpenObserveLogQuery(this.logs, this.streams, stream);
  }

  /** A {@link TraceQueryClient} on a traces stream (default: the config's `tracesStream`). */
  traceQuery(stream: string = this.config.tracesStream): OpenObserveTraceQuery {
    return new OpenObserveTraceQuery(this, stream);
  }

  /** Flushes and closes every log client made by {@link logClient}, then releases connections. Idempotent. */
  close(): Promise<void> {
    this.#closed ??= (async () => {
      await Promise.allSettled([...this.#logClients].map((c) => c.close()));
      this.#logClients.clear();
      await this.#transport.close();
    })();
    return this.#closed;
  }
}
