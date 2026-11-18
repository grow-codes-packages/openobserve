import { afterEach, describe, expect, it } from "vitest";
import {
  ConflictError,
  NotFoundError,
  OpenObserveClient,
  Severity,
  ValidationError,
  logRecordToRow,
  rowToLogRecord,
  toOtlpLogsRequest,
  otlpValue,
  type LogRecord,
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

async function setup(
  handler: MockHandler | MockResponse[],
  extra: Record<string, unknown> = {},
): Promise<{ s: MockServer; client: OpenObserveClient }> {
  const s = await startServer(handler);
  servers.push(s);
  const client = new OpenObserveClient({
    url: s.url,
    ...creds,
    retry: { maxRetries: 0 },
    ...extra,
  });
  clients.push(client);
  return { s, client };
}

function body(s: MockServer, i = 0): any {
  return JSON.parse(s.requests[i]!.body);
}

describe("logs.ingestJson / ingestMulti", () => {
  it("posts a JSON array and returns the stream counters", async () => {
    const { s, client } = await setup([fx("ingestJson.ok")]);
    const rows = fixture("ingestJson.ok").request.body as Record<string, unknown>[];
    const res = await client.logs.ingestJson("sdk_logs_6moiazbq", rows);
    expect(res).toEqual({ stream: "sdk_logs_6moiazbq", successful: 3, failed: 0 });
    expect(s.requests[0]!.method).toBe("POST");
    expect(s.requests[0]!.url).toBe("/api/default/sdk_logs_6moiazbq/_json");
    expect(s.requests[0]!.headers["content-type"]).toBe("application/json");
    expect(body(s)).toHaveLength(3);
  });

  it("normalizes the stream name and wraps a single object", async () => {
    const { s, client } = await setup([fx("ingestJson.stream_name_normalized")]);
    await client.logs.ingestJson("Sdk-Mixed.X", { message: "one" });
    expect(s.requests[0]!.url).toBe("/api/default/sdk_mixed_x/_json");
    expect(body(s)).toEqual([{ message: "one" }]);
  });

  it("reports partial failures (HTTP 200) with the server error", async () => {
    const { client } = await setup([fx("ingestJson.200_partial_failure")]);
    const res = await client.logs.ingestJson("sdk_logs_6moiazbq_partial", [{}, {}]);
    expect(res.successful).toBe(1);
    expect(res.failed).toBe(1);
    expect(res.error).toMatch(/Too old data/);
  });

  it("falls back when the status list is empty", async () => {
    const { client } = await setup([{ status: 200, body: { code: 200 } }]);
    expect(await client.logs.ingestJson("x", [])).toEqual({
      stream: "x",
      successful: 0,
      failed: 0,
    });
  });

  it("gzips the body on request", async () => {
    const { s, client } = await setup((req) => {
      expect(req.headers["content-encoding"]).toBe("gzip");
      return fx("ingestJson.gzip");
    });
    await client.logs.ingestJson("s", [{ message: "zip" }], { gzip: true });
    expect(s.requests).toHaveLength(1);
  });

  it("maps 400 invalid JSON and 401", async () => {
    const { client } = await setup([fx("ingestJson.400_invalid_json"), fx("ingestJson.401")]);
    await expect(client.logs.ingestJson("s", [{}])).rejects.toBeInstanceOf(ValidationError);
    await expect(client.logs.ingestJson("s", [{}])).rejects.toMatchObject({
      code: "AUTHENTICATION",
    });
  });

  it("rejects a blank stream name client-side", async () => {
    const { client } = await setup([]);
    await expect(client.logs.ingestJson(" ", [{}])).rejects.toBeInstanceOf(ValidationError);
  });

  it("ingestMulti sends NDJSON from rows or a string", async () => {
    const { s, client } = await setup([fx("ingestMulti.ok")]);
    const res = await client.logs.ingestMulti("s", [{ a: 1 }, { a: 2 }]);
    expect(res.successful).toBeGreaterThan(0);
    expect(s.requests[0]!.url).toBe("/api/default/s/_multi");
    expect(s.requests[0]!.body).toBe('{"a":1}\n{"a":2}\n');
    expect(s.requests[0]!.headers["content-type"]).toBe("application/x-ndjson");
    await client.logs.ingestMulti("s", '{"a":3}\n');
    expect(s.requests[1]!.body).toBe('{"a":3}\n');
  });

  it("ingestMulti maps a bad line to ValidationError", async () => {
    const { client } = await setup([fx("ingestMulti.400_invalid_line")]);
    await expect(client.logs.ingestMulti("s", "{bad")).rejects.toBeInstanceOf(ValidationError);
  });
});

describe("logs.ingestBulk", () => {
  it("builds the NDJSON action/document pairs", async () => {
    const { s, client } = await setup([fx("ingestBulk.ok")]);
    const res = await client.logs.ingestBulk([
      { stream: "Bulk.S", document: { message: "a" } },
      { action: "create", stream: "bulk_s", document: { message: "b" } },
    ]);
    expect(s.requests[0]!.url).toBe("/api/default/_bulk");
    expect(s.requests[0]!.body).toBe(
      '{"index":{"_index":"bulk_s"}}\n{"message":"a"}\n{"create":{"_index":"bulk_s"}}\n{"message":"b"}\n',
    );
    expect(res.errors).toBe(false);
    expect(res.items.length).toBeGreaterThan(0);
    expect(res.items[0]).toMatchObject({ action: "index", status: 200 });
  });

  it("reports item errors that keep HTTP 200", async () => {
    const { client } = await setup([fx("ingestBulk.200_item_error")]);
    const res = await client.logs.ingestBulk('{"index":{"_index":"s"}}\n{"message":"old"}', {
      gzip: true,
    });
    expect(res.errors).toBe(true);
    expect(res.items[0]).toMatchObject({
      status: 422,
      error: { type: "timestamp_parsing_failed" },
    });
  });

  it("parses minimal items", async () => {
    const { client } = await setup([
      { status: 200, body: { items: [{ index: { error: {} } }, {}] } },
    ]);
    const res = await client.logs.ingestBulk([{ stream: "s", document: {} }]);
    expect(res.items[0]).toEqual({
      action: "index",
      stream: "",
      status: 200,
      error: { type: "error", reason: "" },
    });
    expect(res.items[1]!.action).toBe("index");
    expect(res.took).toBe(0);
  });

  it("rejects delete/update actions and bad raw bodies client-side", async () => {
    const { s, client } = await setup([]);
    await expect(
      client.logs.ingestBulk([{ action: "delete" as "index", stream: "s", document: {} }]),
    ).rejects.toThrow(/unsupported action "delete"/);
    await expect(client.logs.ingestBulk([])).rejects.toThrow(/no actions/);
    await expect(client.logs.ingestBulk('{"delete":{"_index":"s","_id":"1"}}\n')).rejects.toThrow(
      /unsupported action/,
    );
    await expect(client.logs.ingestBulk("not json\n{}")).rejects.toThrow(/not JSON/);
    await expect(client.logs.ingestBulk("\n\n")).rejects.toThrow(/no actions/);
    await expect(client.logs.ingestBulk("[]\n{}")).rejects.toThrow(/unsupported action ""/);
    expect(s.requests).toHaveLength(0);
  });
});

const record = (over: Partial<LogRecord> = {}): LogRecord => ({
  timestamp: new Date("2026-11-18T20:00:00.123Z"),
  severity: Severity.Warn,
  severityText: "WARN",
  body: "hello",
  attributes: { "http.method": "GET", n: 1.5, list: [1, 2], nested: { a: true } },
  resource: { "service.name": "svc" },
  traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
  spanId: "00f067aa0ba902b7",
  traceFlags: 1,
  ...over,
});

describe("logs.ingestOtlp", () => {
  it("posts OTLP/JSON with the stream-name header", async () => {
    const { s, client } = await setup([fx("otlpLogs.json")], { stream: "def" });
    const res = await client.logs.ingestOtlp([record(), record({ resource: {} })], {
      stream: "Otlp",
    });
    expect(res).toEqual({ accepted: 2, rejected: 0 });
    const req = s.requests[0]!;
    expect(req.url).toBe("/api/default/v1/logs");
    expect(req.headers["stream-name"]).toBe("otlp");
    const sent = JSON.parse(req.body);
    expect(sent.resourceLogs).toHaveLength(2);
    const log = sent.resourceLogs[0].scopeLogs[0].logRecords[0];
    expect(log).toMatchObject({
      timeUnixNano: "1795032000123000000",
      severityNumber: 13,
      severityText: "WARN",
      body: { stringValue: "hello" },
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
      flags: 1,
    });
  });

  it("uses the default stream, gzip and partial success", async () => {
    const { s, client } = await setup(
      [{ status: 200, body: { partialSuccess: { rejectedLogRecords: "1" } } }],
      { stream: "def" },
    );
    const res = await client.logs.ingestOtlp([record({ observedTimestamp: new Date(0) })], {
      gzip: true,
    });
    expect(res).toEqual({ accepted: 0, rejected: 1 });
    expect(s.requests[0]!.headers["stream-name"]).toBe("def");
    expect(s.requests[0]!.headers["content-encoding"]).toBe("gzip");
    expect(await client.logs.ingestOtlp([])).toEqual({ accepted: 0, rejected: 0 });
  });

  it("maps the OTLP 401", async () => {
    const { client } = await setup([fx("otlpLogs.401")]);
    await expect(client.logs.ingestOtlp([record()])).rejects.toMatchObject({
      code: "AUTHENTICATION",
    });
  });

  it("encodes every OTLP value type", () => {
    expect(otlpValue("s")).toEqual({ stringValue: "s" });
    expect(otlpValue(true)).toEqual({ boolValue: true });
    expect(otlpValue(3)).toEqual({ intValue: "3" });
    expect(otlpValue(0.5)).toEqual({ doubleValue: 0.5 });
    expect(otlpValue([1])).toEqual({ arrayValue: { values: [{ intValue: "1" }] } });
    expect(otlpValue({ k: "v" })).toEqual({
      kvlistValue: { values: [{ key: "k", value: { stringValue: "v" } }] },
    });
    const req = toOtlpLogsRequest(
      [record({ traceId: undefined, spanId: undefined, traceFlags: undefined })],
      "1",
    );
    const log = (req.resourceLogs as any)[0].scopeLogs[0].logRecords[0];
    expect(log.traceId).toBeUndefined();
  });
});

describe("log record mapping", () => {
  it("record → row flattens keys and keeps record fields", () => {
    const row = logRecordToRow(record({ attributes: { body: "attr", "User.Id": 7, l: ["a"] } }));
    expect(row).toEqual({
      service_name: "svc",
      body: "hello",
      user_id: 7,
      l: '["a"]',
      _timestamp: 1795032000123000,
      severity: "WARN",
      severity_number: 13,
      trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
      span_id: "00f067aa0ba902b7",
      trace_flags: 1,
    });
    const bare = logRecordToRow(
      record({ traceId: undefined, spanId: undefined, traceFlags: undefined }),
    );
    expect(bare.trace_id).toBeUndefined();
  });

  it("row → record handles OTLP rows, plain rows and odd values", () => {
    const otlp = (fixture("search.otlp_logs").response.body as any).hits[0];
    const r = rowToLogRecord(otlp);
    expect(r.body).toBe("otlp pb needle6moiazbq");
    expect(r.severity).toBe(Severity.Warn);
    expect(r.severityText).toBe("WARN");
    expect(r.traceId).toBe("038a1ed5af73678e47809f854dabaae9");
    expect(r.spanId).toBe("00f067aa0ba902b7");
    expect(r.resource).toEqual({ "service.name": "sdk-a-6moiazbq", "service.version": "1.2.3" });
    expect(r.attributes.user_id).toBe(42);
    expect(r.timestamp.getTime()).toBe(1795033200453);

    const plain = rowToLogRecord({
      _timestamp: 1795033200241779,
      message: "m",
      level: "error",
      host_name: "h",
      obj: { a: 1 },
      gone: null,
      trace_flags: 1,
    });
    expect(plain.severity).toBe(Severity.Error);
    expect(plain.severityText).toBe("error");
    expect(plain.body).toBe("m");
    expect(plain.resource["host.name"]).toBe("h");
    expect(plain.attributes.obj).toBe('{"a":1}');
    expect(plain.attributes.gone).toBeUndefined();
    expect(plain.traceFlags).toBe(1);

    expect(rowToLogRecord({ log: { k: 1 } }).body).toBe('{"k":1}');
    expect(rowToLogRecord({ msg: "x", severity_number: 99 }).severity).toBe(Severity.Info);
    expect(rowToLogRecord({ level: "loud" }).severityText).toBe("loud");
    expect(rowToLogRecord({}).body).toBe("");
    expect(rowToLogRecord({ severity_number: 17 }).severityText).toBe("ERROR");
  });
});

describe("logs.search", () => {
  it("sends µs times and maps the envelope", async () => {
    const { s, client } = await setup([fx("search.ok")]);
    const start = new Date("2026-11-18T19:00:00Z");
    const end = new Date("2026-11-18T20:00:00Z");
    const res = await client.logs.search('SELECT * FROM "s"', { start, end, from: 1, size: 2 });
    expect(s.requests[0]!.url).toBe("/api/default/_search?type=logs");
    expect(body(s).query).toEqual({
      sql: 'SELECT * FROM "s"',
      start_time: start.getTime() * 1000,
      end_time: end.getTime() * 1000,
      from: 1,
      size: 2,
    });
    expect(res.hits.length).toBeGreaterThan(0);
    expect(res.requestId).toMatch(/^[0-9a-f]{32}$/);
    expect(res.isPartial).toBe(false);
    expect(res.totalHits).toBeUndefined();
  });

  it("defaults to the last 15 minutes and accepts µs numbers", async () => {
    const { s, client } = await setup([fx("search.ok")]);
    const before = Date.now() * 1000;
    await client.logs.search("SELECT 1");
    const q = body(s).query;
    expect(q.end_time - q.start_time).toBe(15 * 60_000_000 + 60_000_000);
    expect(q.start_time).toBeGreaterThanOrEqual(before - 15 * 60_000_000 - 1_000_000);
    await client.logs.search("SELECT 1", { start: 10, end: 20n, type: "traces" });
    expect(body(s, 1).query.start_time).toBe(10);
    expect(body(s, 1).query.end_time).toBe(20);
    expect(s.requests[1]!.url).toContain("type=traces");
  });

  it("fetches totalHits with track_total_hits", async () => {
    const { s, client } = await setup([fx("search.paging"), fx("search.track_total_hits")]);
    const res = await client.logs.search("SELECT * FROM s", { trackTotalHits: true, size: 2 });
    expect(res.totalHits).toBe(5);
    expect(body(s, 1).query.track_total_hits).toBe(true);
    expect(body(s, 1).query.from).toBe(0);
  });

  it("validates input client-side", async () => {
    const { client } = await setup([]);
    await expect(client.logs.search(" ")).rejects.toBeInstanceOf(ValidationError);
    await expect(
      client.logs.search("SELECT 1", { start: new Date(2000), end: new Date(1000) }),
    ).rejects.toThrow(/start is after end/);
    await expect(client.logs.search("SELECT 1", { start: new Date("x") })).rejects.toThrow(
      /invalid Date/,
    );
    await expect(client.logs.search("SELECT 1", { start: Number.NaN })).rejects.toThrow(
      /invalid timestamp/,
    );
  });

  it.each([
    ["search.400_stream_not_found", NotFoundError],
    ["search.400_bad_sql", ValidationError],
    ["search.400_unknown_field", ValidationError],
    ["search.422_missing_start_time", ValidationError],
    ["search.400_invalid_time_range", ValidationError],
  ])("maps %s", async (name, cls) => {
    const { client } = await setup([fx(name)]);
    const err = await client.logs.search("SELECT 1").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(cls);
    expect((err as { httpStatus: number }).httpStatus).toBeGreaterThanOrEqual(400);
  });

  it("maps 'already exists' to ConflictError", async () => {
    const { client } = await setup([fx("createServiceAccount.400_duplicate")]);
    await expect(client.logs.search("SELECT 1")).rejects.toBeInstanceOf(ConflictError);
  });
});

describe("logs.searchAround / valuesOf / histogram", () => {
  it("searchAround sends key (µs), size and a base64url SQL filter", async () => {
    const { s, client } = await setup([fx("searchAround.ok")]);
    const res = await client.logs.searchAround("sdk_logs_6moiazbq", 1795033200242779, {
      size: 4,
      sql: "SELECT * FROM s WHERE a='?'",
    });
    const url = new URL(s.requests[0]!.url, "http://x");
    expect(url.pathname).toBe("/api/default/sdk_logs_6moiazbq/_around");
    expect(url.searchParams.get("key")).toBe("1795033200242779");
    expect(url.searchParams.get("size")).toBe("4");
    expect(Buffer.from(url.searchParams.get("sql")!, "base64url").toString()).toBe(
      "SELECT * FROM s WHERE a='?'",
    );
    expect(res.hits.length).toBe(5);
    expect(res.requestId).toBeUndefined();
    await client.logs.searchAround("s", new Date(1000));
    expect(new URL(s.requests[1]!.url, "http://x").searchParams.get("key")).toBe("1000000");
  });

  it("searchAround maps a missing stream", async () => {
    const { client } = await setup([fx("searchAround.400_stream_not_found")]);
    await expect(client.logs.searchAround("x", new Date())).rejects.toBeInstanceOf(NotFoundError);
  });

  it("valuesOf maps fields and counts", async () => {
    const { s, client } = await setup([
      fx("valuesOf.ok", (b) => {
        b.hits.push({ field: "o", values: [{ zo_sql_key: { a: 1 }, zo_sql_num: 1 }] });
        return b;
      }),
    ]);
    const res = await client.logs.valuesOf("sdk_logs_6moiazbq", ["level", "run_id"], {
      start: new Date(1),
      end: new Date(2),
      size: 3,
    });
    expect(res[0]).toEqual({
      field: "level",
      values: [
        { value: "debug", count: 2 },
        { value: "error", count: 1 },
        { value: "info", count: 1 },
        { value: "warn", count: 1 },
      ],
    });
    expect(res[2]!.values[0]!.value).toBe('{"a":1}');
    const url = new URL(s.requests[0]!.url, "http://x");
    expect(url.searchParams.get("fields")).toBe("level,run_id");
    expect(url.searchParams.get("start_time")).toBe("1000");
  });

  it("valuesOf validates fields", async () => {
    const { client } = await setup([fx("valuesOf.400_missing_time")]);
    await expect(client.logs.valuesOf("s", [])).rejects.toThrow(/fields/);
    await expect(client.logs.valuesOf("s", ["a"])).rejects.toBeInstanceOf(ValidationError);
  });

  it("histogram builds the SQL and parses buckets", async () => {
    const { s, client } = await setup([
      fx("histogram.ok"),
      fx("histogram.ok", (b) => {
        delete b.histogram_interval;
        b.hits = [{ zo_sql_key: "bad", zo_sql_num: 1 }, { zo_sql_key: "2026-11-18T20:20:00Z" }];
        return b;
      }),
    ]);
    const res = await client.logs.histogram("sdk_logs_6moiazbq", {
      interval: "1 minute",
      filter: "run_id = '6moiazbq'",
      start: new Date(1),
      end: new Date(2),
      signal: new AbortController().signal,
    });
    expect(body(s).query.sql).toBe(
      "SELECT histogram(_timestamp, '1 minute') AS zo_sql_key, count(*) AS zo_sql_num FROM \"sdk_logs_6moiazbq\" WHERE run_id = '6moiazbq' GROUP BY zo_sql_key ORDER BY zo_sql_key",
    );
    expect(res.intervalSeconds).toBe(60);
    expect(res.buckets).toEqual([
      { start: new Date("2026-11-18T20:20:00Z"), key: "2026-11-18T20:20:00", count: 5 },
    ]);
    const other = await client.logs.histogram("s");
    expect(body(s, 1).query.sql).toContain("histogram(_timestamp) AS");
    expect(other.intervalSeconds).toBeUndefined();
    expect(other.buckets[0]!.start.getTime()).toBe(0);
    expect(other.buckets[1]!.count).toBe(0);
    await expect(client.logs.histogram("s", { interval: "1'; DROP" })).rejects.toThrow(/interval/);
  });
});

describe("logs.tail", () => {
  it("polls with a µs cursor, de-duplicates the overlap and pages", async () => {
    const rows = [
      { _timestamp: 2_000_000, message: "a" },
      { _timestamp: 3_000_000, message: "b" },
    ];
    const { s, client } = await setup((_req, i) => {
      if (i === 0) return { status: 200, body: { hits: rows } };
      if (i === 1)
        return { status: 200, body: { hits: [rows[1], { _timestamp: 3_500_000, message: "c" }] } };
      if (i === 2)
        return { status: 200, body: { hits: [{ _timestamp: 3_500_000, message: "c" }] } };
      return { status: 200, body: { hits: [] } };
    });
    const seen: string[] = [];
    const handle = client.logs.tail("Tail.S", (row) => seen.push(String(row.message)), {
      start: 1_000_000,
      filter: "match_all('x')",
      intervalMs: 5,
      pageSize: 2,
      overlapMs: 1000,
    });
    await expect.poll(() => s.requests.length, { timeout: 5000 }).toBeGreaterThanOrEqual(4);
    handle.stop();
    handle.stop();
    expect(seen).toEqual(["a", "b", "c"]);
    const first = body(s).query;
    expect(first.sql).toBe(
      "SELECT * FROM \"tail_s\" WHERE _timestamp >= 0 AND (match_all('x')) ORDER BY _timestamp ASC",
    );
    expect(first.start_time).toBe(0);
    expect(body(s, 1).query.sql).toContain("_timestamp >= 2000000");
  });

  it("reports errors and keeps polling; stops mid-page", async () => {
    let calls = 0;
    const { client } = await setup((_req, i) => {
      calls = i + 1;
      return i === 0
        ? { status: 400, body: { code: 400, message: "boom" } }
        : {
            status: 200,
            body: {
              hits: [
                { _timestamp: 1, m: 1 },
                { _timestamp: 2, m: 2 },
              ],
            },
          };
    });
    const errors: unknown[] = [];
    const got: unknown[] = [];
    const handle = client.logs.tail(
      "s",
      (row) => {
        got.push(row);
        handle.stop();
      },
      { intervalMs: 5, onError: (e) => errors.push(e) },
    );
    await expect.poll(() => got.length, { timeout: 5000 }).toBe(1);
    expect(errors[0]).toBeInstanceOf(ValidationError);
    expect(calls).toBe(2);
    const quiet = client.logs.tail("s", () => undefined, { intervalMs: 5 });
    quiet.stop();
  });
});
