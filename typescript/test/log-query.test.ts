import { afterEach, describe, expect, it } from "vitest";
import {
  OpenObserveClient,
  OpenObserveLogExporter,
  OpenObserveOtlpLogExporter,
  Severity,
  ValidationError,
  isSqlQuery,
  parseJson,
  textQuerySql,
  type LogRecord,
} from "../src/index.js";
import { fx } from "./helpers/fixtures.js";
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

async function setup(
  handler: MockHandler | MockResponse[],
): Promise<{ s: MockServer; client: OpenObserveClient }> {
  const s = await startServer(handler);
  servers.push(s);
  const client = new OpenObserveClient({
    url: s.url,
    user: "admin@stack.local",
    password: "Gw1-secret-pass",
    stream: "sdk_logs_6moiazbq",
    retry: { maxRetries: 0 },
  });
  clients.push(client);
  return { s, client };
}

const sent = (s: MockServer, i = 0): any => JSON.parse(s.requests[i]!.body);

describe("query helpers", () => {
  it("detects SQL and builds text queries with escaping", () => {
    expect(isSqlQuery("  select * from x")).toBe(true);
    expect(isSqlQuery("selected items")).toBe(false);
    expect(textQuerySql("s", "it's", "desc")).toBe(
      `SELECT * FROM "s" WHERE match_all('it''s') ORDER BY _timestamp DESC`,
    );
    expect(textQuerySql("s", " ", "asc")).toBe(`SELECT * FROM "s" ORDER BY _timestamp ASC`);
  });

  it("parseJson keeps unsafe integers exact as strings", () => {
    expect(parseJson('{"a":1795033200603151382,"b":1.5,"c":12,"d":1e300}')).toEqual({
      a: "1795033200603151382",
      b: 1.5,
      c: 12,
      d: 1e300,
    });
    expect(parseJson(" ")).toBeUndefined();
  });
});

describe("OpenObserveLogQuery", () => {
  it("search: text query → match_all on the stream, records normalized, cursor", async () => {
    const { s, client } = await setup([
      fx("search.match_all", (b) => {
        b.hits = [b.hits[0], b.hits[0]];
        return b;
      }),
    ]);
    const query = client.logQuery();
    const res = await query.search("needle6moiazbq", {
      limit: 2,
      cursor: "4",
      order: "asc",
      start: new Date(1),
      end: new Date(2),
    });
    const q = sent(s).query;
    expect(q.sql).toBe(
      `SELECT * FROM "sdk_logs_6moiazbq" WHERE match_all('needle6moiazbq') ORDER BY _timestamp ASC`,
    );
    expect(q.from).toBe(4);
    expect(q.size).toBe(2);
    expect(res.records).toHaveLength(2);
    expect(res.records[0]!.body).toMatch(/needle/);
    expect(res.nextCursor).toBe("6");
    expect(res.total).toBeUndefined();
  });

  it("search: raw SQL is sent as is; total with trackTotalHits", async () => {
    const { s, client } = await setup([fx("search.ok"), fx("search.track_total_hits")]);
    const res = await client.logQuery().search("SELECT * FROM x", {
      trackTotalHits: true,
      signal: new AbortController().signal,
    });
    expect(sent(s).query.sql).toBe("SELECT * FROM x");
    expect(res.total).toBe(5);
    expect(res.nextCursor).toBeUndefined();
  });

  it("search validates limit and cursor", async () => {
    const { client } = await setup([]);
    const q = client.logQuery();
    await expect(q.search("x", { limit: 0 })).rejects.toBeInstanceOf(ValidationError);
    await expect(q.search("x", { cursor: "-1" })).rejects.toThrow(/cursor/);
    await expect(q.search("x", { cursor: "abc" })).rejects.toThrow(/cursor/);
  });

  it("listStreams and listLabels", async () => {
    const { s, client } = await setup([fx("listStreams.logs"), fx("getStreamSchema.ok")]);
    const q = client.logQuery();
    expect(await q.listStreams()).toContain("passcode_6moiazbq");
    expect(await q.listLabels()).toContain("message");
    expect(s.requests[1]!.url).toBe("/api/default/streams/sdk_logs_6moiazbq/schema?type=logs");
  });

  it("waitForLog polls search until a record matches", async () => {
    const { client } = await setup([
      fx("search.ok", (b) => ({ ...b, hits: [] })),
      fx("search.match_all"),
    ]);
    const r = await client
      .logQuery()
      .waitForLog("needle6moiazbq", { intervalMs: 5, timeoutMs: 5000 });
    expect(r.body).toMatch(/needle/);
  });

  it("tail: text queries use the cursor tail with match_all; SQL uses the generic poller", async () => {
    const { s, client } = await setup((req) => {
      const sql: string = JSON.parse(req.body).query.sql;
      return {
        status: 200,
        body: {
          hits:
            sql.includes("match_all") || sql.startsWith("SELECT * FROM x")
              ? [{ _timestamp: Date.now() * 1000 + 5, message: `m ${sql.length}` }]
              : [],
        },
      };
    });
    const q = client.logQuery();
    const a: LogRecord[] = [];
    const errors: unknown[] = [];
    const h1 = q.tail("needle", (r) => a.push(r), {
      intervalMs: 5,
      start: new Date(Date.now() - 1000),
      onError: (e) => errors.push(e),
    });
    await expect.poll(() => a.length, { timeout: 5000 }).toBeGreaterThanOrEqual(1);
    h1.stop();
    expect(JSON.parse(s.requests[0]!.body).query.sql).toContain("match_all('needle')");
    const h2 = q.tail(" ", () => undefined, { intervalMs: 5 });
    await expect
      .poll(() => s.requests.some((r) => !JSON.parse(r.body).query.sql.includes("match_all")))
      .toBe(true);
    h2.stop();
    const b: LogRecord[] = [];
    const h3 = q.tail("SELECT * FROM x", (r) => b.push(r), {
      intervalMs: 5,
      start: new Date(Date.now() - 1000),
    });
    await expect.poll(() => b.length, { timeout: 5000 }).toBeGreaterThanOrEqual(1);
    h3.stop();
    expect(errors).toEqual([]);
  });
});

const rec = (body: string): LogRecord => ({
  timestamp: new Date(),
  severity: Severity.Info,
  severityText: "INFO",
  body,
  attributes: {},
  resource: {},
});

describe("exporters and logClient", () => {
  it("json exporter reports rejected records with the server reason", async () => {
    const { client } = await setup([
      fx("ingestJson.200_partial_failure"),
      { status: 200, body: { status: [{ name: "s", successful: 0, failed: 1 }] } },
    ]);
    const exp = new OpenObserveLogExporter(client.logs, "s");
    const res = await exp.export([rec("a"), rec("b")], { gzip: false });
    expect(res.accepted).toBe(1);
    expect(res.rejected).toBe(1);
    expect(res.error?.message).toMatch(/1 records rejected \(Too old data/);
    const res2 = await exp.export([rec("a")], {
      gzip: false,
      signal: new AbortController().signal,
    });
    expect(res2.error?.message).toBe("logging.export s: 1 records rejected");
    expect(await exp.export([], { gzip: false })).toEqual({ accepted: 0, rejected: 0 });
    await exp.shutdown();
  });

  it("otlp exporter reports partial success", async () => {
    const { client } = await setup([
      { status: 200, body: { partial_success: { rejected_log_records: 1 } } },
      { status: 200, body: "" },
    ]);
    const exp = new OpenObserveOtlpLogExporter(client.logs, "s");
    const res = await exp.export([rec("a"), rec("b")], {
      gzip: true,
      signal: new AbortController().signal,
    });
    expect(res).toMatchObject({ accepted: 1, rejected: 1 });
    expect(res.error).toBeInstanceOf(ValidationError);
    expect(await exp.export([rec("c")], { gzip: false })).toEqual({ accepted: 1, rejected: 0 });
    await exp.shutdown();
  });

  it("logClient sends redacted records through _json or OTLP", async () => {
    const { s, client } = await setup((req) =>
      req.url.endsWith("/_json")
        ? {
            status: 200,
            body: { code: 200, status: [{ name: "sdk_logs_6moiazbq", successful: 1, failed: 0 }] },
          }
        : { status: 200, body: {} },
    );
    const json = client.logClient({
      batch: { flushIntervalMs: 60_000, gzip: false },
      resource: { "service.name": "svc" },
    });
    json.logger("app").warn("hello", { password: "hunter2", user: "u" });
    await json.flush();
    const row = sent(s)[0];
    expect(row).toMatchObject({
      body: "hello",
      severity: "WARN",
      severity_number: 13,
      service_name: "svc",
      user: "u",
    });
    expect(JSON.stringify(row)).not.toContain("hunter2");
    const otlp = client.logClient({
      protocol: "otlp",
      stream: "other",
      batch: { flushIntervalMs: 60_000 },
    });
    otlp.send({ message: "via otlp" });
    await otlp.flush();
    expect(s.requests[1]!.url).toBe("/api/default/v1/logs");
    expect(s.requests[1]!.headers["stream-name"]).toBe("other");
    expect(json.stats().sent).toBe(1);
  });
});
