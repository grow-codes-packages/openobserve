import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import {
  ConfigError,
  NotFoundError,
  OpenObserveClient,
  OpenObserveConfig,
  UnsupportedError,
  ValidationError,
  createOpenObserveSpanExporter,
  grpcEndpoint,
  hitToSummary,
  initTracing,
  openObserveTracingDefaults,
  resolveTracingConfig,
  rowToSpan,
  traceFilter,
  tracesEndpoint,
} from "../src/index.js";
import { fixture, fx } from "./helpers/fixtures.js";
import {
  type MockHandler,
  type MockResponse,
  type MockServer,
  startServer,
} from "./helpers/server.js";

const servers: MockServer[] = [];
const clients: OpenObserveClient[] = [];
afterEach(async () => {
  await Promise.all(clients.splice(0).map((c) => c.close()));
  await Promise.all(servers.splice(0).map((s) => s.close()));
});

const creds = { user: "admin@stack.local", password: "Gw1-secret-pass" };
const TRACE = "038a1ed5af73678e47809f854dabaae9";

async function setup(
  handler: MockHandler | MockResponse[],
): Promise<{ s: MockServer; client: OpenObserveClient }> {
  const s = await startServer(handler);
  servers.push(s);
  const client = new OpenObserveClient({
    url: s.url,
    ...creds,
    tracesStream: "sdk_traces_6moiazbq",
    retry: { maxRetries: 0 },
  });
  clients.push(client);
  return { s, client };
}

const q = (s: MockServer, i = 0): URLSearchParams =>
  new URL(s.requests[i]!.url, "http://x").searchParams;

describe("span mapping", () => {
  it("maps a span row exactly (ns → µs, kind, status, events, links)", () => {
    const hits = (fixture("getTrace.details").response.body as any).hits;
    const db = rowToSpan(hits[1]);
    expect(db).toMatchObject({
      traceId: TRACE,
      spanId: "00f067aa0ba902b7",
      parentSpanId: "b7ad6b7169203331",
      name: "db.query",
      kind: "client",
      serviceName: "sdk-a-6moiazbq",
      startMicros: 1795033200613151,
      endMicros: 1795033200643151,
      durationMicros: 30000,
      status: { code: "error", message: "boom" },
    });
    expect(db.attributes).toMatchObject({ db_system: "postgresql", retry: "true", ratio: "0.5" });
    expect(
      Object.keys(db.attributes).some((k) => k.startsWith("o2_db_") || k.startsWith("infer_")),
    ).toBe(false);
    expect(db.events).toEqual([
      { name: "cache.miss", timeMicros: 1795033200623151, attributes: { key: "u1" } },
    ]);
    expect(db.links).toEqual([
      {
        traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
        spanId: "00f067aa0ba902b8",
        attributes: { kind: "follows" },
      },
    ]);
    const root = rowToSpan(hits[0]);
    expect(root.parentSpanId).toBeUndefined();
    expect(root.kind).toBe("server");
    expect(root.status).toEqual({ code: "ok" });
  });

  it("tolerates odd rows", () => {
    const span = rowToSpan({
      span_kind: 9,
      status_code: 0,
      start_time: 5000,
      end_time: 1000,
      duration: 7,
      events: "not json",
      links: [
        { context: { trace_id: "AB", span_id: "CD" }, dropped_attributes_count: 0, x: { y: 1 } },
      ],
      extra: null,
    });
    expect(span.kind).toBe("unspecified");
    expect(span.status).toEqual({ code: "unset" });
    expect(span.durationMicros).toBe(7);
    expect(span.events).toEqual([]);
    expect(span.links).toEqual([{ traceId: "ab", spanId: "cd", attributes: { x: '{"y":1}' } }]);
    expect(rowToSpan({ status_code: 2 }).status.code).toBe("error");
    expect(rowToSpan({ status_code: 1, start_time: "x" }).status.code).toBe("ok");
    expect(rowToSpan({ links: [{}] }).links[0]).toEqual({
      traceId: "",
      spanId: "",
      attributes: {},
    });
  });

  it("maps trace summaries", () => {
    const hits = (fixture("searchTraces.latest").response.body as any).hits;
    expect(hitToSummary(hits[1])).toEqual({
      traceId: TRACE,
      rootServiceName: "sdk-a-6moiazbq",
      rootName: "GET /checkout",
      startMicros: 1795033200603151,
      durationMicros: 50000,
      spanCount: 3,
    });
    expect(hitToSummary({})).toEqual({
      traceId: "",
      startMicros: 0,
      durationMicros: 0,
      spanCount: 0,
    });
  });

  it("builds trace filters with escaping", () => {
    expect(
      traceFilter({
        service: "a'b",
        operation: "GET /x",
        tags: { "db.system": "postgresql" },
        minDurationMicros: 40000.7,
        maxDurationMicros: 90000,
      }),
    ).toBe(
      "service_name = 'a''b' AND operation_name = 'GET /x' AND db_system = 'postgresql' AND duration >= 40000 AND duration <= 90000",
    );
    expect(traceFilter({})).toBe("");
    expect(() => traceFilter({ minDurationMicros: -1 })).toThrow(ValidationError);
    expect(() => traceFilter({ tags: { "": "x" } })).toThrow(/tag key/);
  });
});

describe("OpenObserveTraceQuery", () => {
  it("getTrace sends a 7-day µs window and maps spans", async () => {
    const { s, client } = await setup([fx("getTrace.details")]);
    const trace = await client.traces.getTrace(TRACE.toUpperCase());
    expect(new URL(s.requests[0]!.url, "http://x").pathname).toBe(
      `/api/default/sdk_traces_6moiazbq/traces/${TRACE}/details`,
    );
    const params = q(s);
    expect(Number(params.get("end_time")) - Number(params.get("start_time"))).toBe(
      7 * 24 * 3600 * 1_000_000 + 60_000_000,
    );
    expect(trace?.traceId).toBe(TRACE);
    expect(trace?.spans.map((sp) => sp.name)).toEqual([
      "GET /checkout",
      "db.query",
      expect.any(String),
    ]);
  });

  it("getTrace follows pagination", async () => {
    const page2 = fx("getTrace.details", (b) => {
      b.hits = b.hits.slice(1);
      return b;
    });
    const { s, client } = await setup([fx("getTrace.page1"), page2]);
    const trace = await client.traces.getTrace(TRACE, { start: new Date(1), end: new Date(2) });
    expect(trace?.spans).toHaveLength(3);
    expect(q(s, 1).get("after_start_time")).toBe("1795033200603151382");
    expect(q(s, 1).get("after_span_id")).toBe("b7ad6b7169203331");
    expect(q(s).get("start_time")).toBe("1000");
  });

  it("getTrace returns null for empty hits and missing streams", async () => {
    const { client } = await setup([
      fx("getTrace.not_found"),
      fx("getTrace.400_stream_not_found"),
      { status: 500, body: "x" },
    ]);
    expect(await client.traces.getTrace(TRACE)).toBeNull();
    expect(await client.traces.getTrace(TRACE)).toBeNull();
    await expect(client.traces.getTrace(TRACE)).rejects.toMatchObject({ code: "UNAVAILABLE" });
    await expect(client.traces.getTrace("xyz")).rejects.toBeInstanceOf(ValidationError);
  });

  it("searchTraces sends filter, window and limit", async () => {
    const { s, client } = await setup([fx("searchTraces.filter_service")]);
    const res = await client.traces.searchTraces({ service: "sdk-a-6moiazbq", limit: 5 });
    const params = q(s);
    expect(new URL(s.requests[0]!.url, "http://x").pathname).toBe(
      "/api/default/sdk_traces_6moiazbq/traces/latest",
    );
    expect(params.get("filter")).toBe("service_name = 'sdk-a-6moiazbq'");
    expect(params.get("size")).toBe("5");
    expect(Number(params.get("end_time")) - Number(params.get("start_time"))).toBe(
      3600 * 1_000_000 + 60_000_000,
    );
    expect(res.length).toBeGreaterThan(0);
    expect(res[0]!.rootServiceName).toMatch(/^sdk-/);
  });

  it("searchTraces without filter, no match, missing stream, bad limit", async () => {
    const { s, client } = await setup([
      fx("searchTraces.latest"),
      fx("searchTraces.filter_no_match"),
      fx("search.400_stream_not_found"),
      { status: 401, body: "Unauthorized Access" },
    ]);
    expect(await client.traces.searchTraces()).toHaveLength(2);
    expect(q(s).has("filter")).toBe(false);
    expect(await client.traces.searchTraces({ operation: "none" })).toEqual([]);
    expect(await client.traces.searchTraces({})).toEqual([]);
    await expect(client.traces.searchTraces({})).rejects.toMatchObject({ code: "AUTHENTICATION" });
    await expect(client.traces.searchTraces({ limit: 0 })).rejects.toThrow(/limit/);
  });

  it("listServices / listOperations query DISTINCT on the traces stream", async () => {
    const { s, client } = await setup([
      fx("listServices.ok", (b) => {
        b.hits.push({ service_name: "sdk-a-6moiazbq" }, {});
        return b;
      }),
      fx("listOperations.ok"),
      fx("search.400_stream_not_found"),
      { status: 403, body: "no" },
    ]);
    expect(await client.traces.listServices()).toEqual(["sdk-a-6moiazbq", "sdk-b-6moiazbq"]);
    const sent = JSON.parse(s.requests[0]!.body).query;
    expect(sent.sql).toBe(
      'SELECT DISTINCT service_name FROM "sdk_traces_6moiazbq" ORDER BY service_name',
    );
    expect(sent.end_time - sent.start_time).toBe(24 * 3600 * 1_000_000 + 60_000_000);
    expect(s.requests[0]!.url).toBe("/api/default/_search?type=traces");
    const ops = await client.traces.listOperations("sdk-a-6moiazbq", {
      start: new Date(1),
      end: new Date(2),
    });
    expect(ops.length).toBeGreaterThan(0);
    expect(JSON.parse(s.requests[1]!.body).query.sql).toContain(
      "WHERE service_name = 'sdk-a-6moiazbq'",
    );
    expect(await client.traces.listServices()).toEqual([]);
    await expect(client.traces.listServices()).rejects.toMatchObject({ code: "PERMISSION_DENIED" });
  });

  it("traceTimeRange maps found and not_found", async () => {
    const { s, client } = await setup([
      fx("traceTimeRange.ok"),
      fx("traceTimeRange.not_found"),
      { status: 200, body: { results: [{ status: "found" }] } },
    ]);
    expect(await client.traces.traceTimeRange(TRACE)).toEqual({
      found: true,
      stream: "sdk_traces_6moiazbq",
      startMicros: 1795033200603151,
      endMicros: 1795033200653152,
    });
    expect(s.requests[0]!.url).toBe(`/api/default/traces/time_range?trace_id=${TRACE}`);
    expect(await client.traces.traceTimeRange(TRACE)).toEqual({ found: false });
    expect(await client.traces.traceTimeRange(TRACE)).toEqual({
      found: true,
      startMicros: 0,
      endMicros: 0,
    });
  });

  it("getDependencies is unsupported (enterprise only) and waitForTrace polls getTrace", async () => {
    const { client } = await setup([fx("getTrace.not_found"), fx("getTrace.details")]);
    await expect(client.traces.getDependencies()).rejects.toBeInstanceOf(UnsupportedError);
    const trace = await client.traces.waitForTrace(TRACE, {
      intervalMs: 5,
      timeoutMs: 5000,
      minSpans: 2,
    });
    expect(trace.spans.length).toBe(3);
    expect(NotFoundError).toBeDefined();
  });
});

describe("initTracing", () => {
  it("builds endpoints and defaults", () => {
    const cfg = new OpenObserveConfig({
      url: "http://oo:5080/base/",
      org: "acme",
      ...creds,
      tracesStream: "TR",
    });
    expect(tracesEndpoint(cfg)).toBe("http://oo:5080/base/api/acme/v1/traces");
    expect(grpcEndpoint(cfg)).toBe("http://oo:5081");
    expect(grpcEndpoint(new OpenObserveConfig({ url: "https://oo" }))).toBe("https://oo:5081");
    expect(grpcEndpoint(new OpenObserveConfig({ url: "http://oo:9000" }))).toBe("http://oo:9000");
    const d = openObserveTracingDefaults(cfg);
    expect(d.protocol).toBe("http/protobuf");
    expect(d.headers).toEqual({
      "stream-name": "tr",
      authorization: `Basic ${Buffer.from(`${creds.user}:${creds.password}`).toString("base64")}`,
    });
    expect(openObserveTracingDefaults(new OpenObserveConfig(), "X").headers).toEqual({
      "stream-name": "x",
    });
  });

  it("exports spans as OTLP/HTTP protobuf with auth and stream headers", async () => {
    const s = await startServer(() => ({
      status: 200,
      headers: { "content-type": "application/x-protobuf" },
      body: "",
    }));
    servers.push(s);
    const handle = initTracing({
      serviceName: "svc-test",
      setGlobal: false,
      compression: "none",
      openobserve: { url: s.url, ...creds },
      stream: "Traces.A",
    });
    handle.tracer("t").startSpan("op").end();
    await handle.forceFlush(5000);
    await handle.shutdown();
    await handle.shutdown();
    const req = s.requests[0]!;
    expect(req.url).toBe("/api/default/v1/traces");
    expect(req.headers["content-type"]).toBe("application/x-protobuf");
    expect(req.headers["stream-name"]).toBe("traces_a");
    expect(req.headers.authorization).toMatch(/^Basic /);
    expect(req.headers["user-agent"]).toContain("grow-codes-openobserve");
    expect(handle.config.toString()).not.toContain(creds.password);
  });

  it("native protocol sends OTLP/JSON (gzip) and env config is the default", async () => {
    const s = await startServer(() => ({ status: 200, body: {} }));
    servers.push(s);
    const prev = { ...process.env };
    process.env.GROW_OPENOBSERVE_URL = s.url;
    process.env.OPENOBSERVE_USER = creds.user;
    process.env.OPENOBSERVE_PASSWORD = creds.password;
    try {
      const handle = initTracing({ serviceName: "svc-json", protocol: "native", setGlobal: false });
      handle.tracer("t").startSpan("op-json").end();
      await handle.forceFlush(5000);
      await handle.shutdown();
    } finally {
      for (const k of ["GROW_OPENOBSERVE_URL", "OPENOBSERVE_USER", "OPENOBSERVE_PASSWORD"]) {
        if (prev[k] === undefined) delete process.env[k];
        else process.env[k] = prev[k];
      }
    }
    expect(s.requests).toHaveLength(1);
    expect(s.requests[0]!.headers["content-type"]).toBe("application/json");
  });

  it("grpc builds a gRPC exporter with metadata; serviceName is required", async () => {
    const cfg = new OpenObserveConfig({ url: "http://127.0.0.1:5080", ...creds });
    const resolved = resolveTracingConfig(
      { serviceName: "g", protocol: "grpc", tls: { insecureSkipVerify: true } },
      { defaults: openObserveTracingDefaults(cfg) },
    );
    const exporter = createOpenObserveSpanExporter(
      { ...resolved, endpoint: undefined } as never,
      cfg,
    );
    await exporter.shutdown();
    const handle = initTracing({
      serviceName: "g",
      protocol: "grpc",
      setGlobal: false,
      openobserve: cfg,
    });
    expect(handle.config.endpoint).toBe("http://127.0.0.1:5081");
    await handle.shutdown();
    const json = createOpenObserveSpanExporter(
      resolveTracingConfig({ serviceName: "j", protocol: "native", tls: { caCertPem: "x" } }),
      cfg,
    );
    await json.shutdown();
    expect(() => initTracing({ serviceName: " ", openobserve: cfg })).toThrow(ConfigError);
    expect(gunzipSync).toBeDefined();
  });
});
