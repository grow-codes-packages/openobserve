// Logs module against the real service: every ingest call is read back with search.

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import {
  OpenObserveClient,
  Severity,
  ValidationError,
  type LogRecord,
} from "@grow-codes/openobserve";
import { eventually, rootClient, suffix, uniqueStream } from "./helpers.ts";

const stream = uniqueStream("logs");
let client: OpenObserveClient;

before(() => {
  client = rootClient();
});

after(async () => {
  await client.streams.deleteStream(stream).catch(() => undefined);
  await client.close();
});

const sql = (where: string): string =>
  `SELECT * FROM "${stream}" WHERE ${where} ORDER BY _timestamp ASC`;

async function rows(where: string, min: number): Promise<Record<string, unknown>[]> {
  const res = await eventually(
    () => client.logs.search(sql(where)),
    (r) => r.hits.length >= min,
  );
  return res.hits;
}

test("ingestJson → search round trip (flattened fields, µs timestamps)", async () => {
  const now = Date.now() * 1000;
  const res = await client.logs.ingestJson(stream, [
    { message: `json alpha ${suffix}`, level: "info", src: "json", n: 1, _timestamp: now },
    {
      message: "json beta",
      level: "error",
      src: "json",
      n: 2,
      _timestamp: now + 1,
      user: { id: 7 },
    },
  ]);
  assert.deepEqual(res, { stream, successful: 2, failed: 0 });
  const hits = await rows("src = 'json'", 2);
  assert.equal(hits[0]!.message, `json alpha ${suffix}`);
  assert.equal(hits[0]!._timestamp, now);
  assert.equal(hits[1]!.user_id, 7);
});

test("ingestJson reports a too-old record as failed (HTTP 200)", async () => {
  const res = await client.logs.ingestJson(stream, [
    { message: "fresh", src: "partial" },
    { message: "too old", src: "partial", _timestamp: (Date.now() - 10 * 3600_000) * 1000 },
  ]);
  assert.equal(res.successful, 1);
  assert.equal(res.failed, 1);
  assert.match(res.error ?? "", /old/i);
});

test("ingestMulti (gzip) → search round trip", async () => {
  const res = await client.logs.ingestMulti(
    stream,
    [0, 1, 2].map((i) => ({ message: `multi ${i}`, src: "multi", level: "debug" })),
    { gzip: true },
  );
  assert.equal(res.successful, 3);
  const hits = await rows("src = 'multi'", 3);
  assert.deepEqual(hits.map((h) => h.message).sort(), ["multi 0", "multi 1", "multi 2"]);
});

test("ingestBulk → search round trip; delete actions are rejected client-side", async () => {
  const res = await client.logs.ingestBulk([
    { stream, document: { message: "bulk a", src: "bulk" } },
    { action: "create", stream, document: { message: "bulk b", src: "bulk" } },
  ]);
  assert.equal(res.errors, false);
  assert.equal(res.items.length, 2);
  assert.ok(res.items.every((i) => i.status === 200 && i.stream === stream));
  assert.equal((await rows("src = 'bulk'", 2)).length, 2);
  await assert.rejects(
    client.logs.ingestBulk(`{"delete":{"_index":"${stream}","_id":"1"}}\n`),
    ValidationError,
  );
});

test("ingestOtlp → search round trip (body, severity, trace ids)", async () => {
  const record: LogRecord = {
    timestamp: new Date(),
    severity: Severity.Warn,
    severityText: "WARN",
    body: `otlp needle ${suffix}`,
    attributes: { src: "otlp", "user.id": 42 },
    resource: { "service.name": `ts-otlp-${suffix}` },
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    spanId: "00f067aa0ba902b7",
  };
  const res = await client.logs.ingestOtlp([record], { stream, gzip: true });
  assert.deepEqual(res, { accepted: 1, rejected: 0 });
  const [hit] = await rows("src = 'otlp'", 1);
  assert.equal(hit!.body, `otlp needle ${suffix}`);
  assert.equal(hit!.severity, "WARN");
  assert.equal(hit!.severity_number, 13);
  assert.equal(hit!.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
  assert.equal(hit!.service_name, `ts-otlp-${suffix}`);
  assert.equal(hit!.user_id, 42);
});

test("search with paging and trackTotalHits", async () => {
  await rows("src = 'multi'", 3);
  const page = await client.logs.search(sql("src = 'multi'"), {
    from: 1,
    size: 1,
    trackTotalHits: true,
  });
  assert.equal(page.hits.length, 1);
  assert.equal(page.totalHits, 3);
  assert.match(page.requestId ?? "", /^[0-9a-f]{32}$/);
});

test("searchAround returns rows around an exact µs anchor", async () => {
  const [anchor] = await rows("src = 'json'", 1);
  const res = await client.logs.searchAround(stream, anchor!._timestamp as number, { size: 4 });
  assert.ok(res.hits.some((h) => h._timestamp === anchor!._timestamp));
  assert.ok(res.hits.length <= 4);
});

test("valuesOf counts distinct values", async () => {
  await rows("src = 'multi'", 3);
  const values = await client.logs.valuesOf(stream, ["src"], { size: 20 });
  const src = values.find((v) => v.field === "src");
  assert.ok(src);
  assert.equal(src.values.find((v) => v.value === "multi")?.count, 3);
});

test("histogram buckets the records", async () => {
  await rows("src = 'multi'", 3);
  const h = await client.logs.histogram(stream, { interval: "1 minute", filter: "src = 'multi'" });
  assert.equal(
    h.buckets.reduce((sum, b) => sum + b.count, 0),
    3,
  );
  assert.equal(h.intervalSeconds, 60);
  assert.ok(h.buckets[0]!.start instanceof Date);
});

test("tail delivers rows ingested after it started, once each", async () => {
  const seen: string[] = [];
  const handle = client.logs.tail(stream, (row) => seen.push(String(row.message)), {
    filter: "src = 'tail'",
    intervalMs: 200,
  });
  try {
    await new Promise((r) => setTimeout(r, 300));
    await client.logs.ingestJson(stream, [
      { message: "tail 1", src: "tail" },
      { message: "tail 2", src: "tail" },
    ]);
    await eventually(
      async () => seen.length,
      (n) => n >= 2,
    );
    await new Promise((r) => setTimeout(r, 600));
  } finally {
    handle.stop();
  }
  assert.deepEqual([...seen].sort(), ["tail 1", "tail 2"]);
});

test("LogClient (json and otlp) → LogQueryClient round trip", async () => {
  const logs = client.logClient({ stream, resource: { "service.name": "ts-logclient" } });
  const otlp = client.logClient({ stream, protocol: "otlp" });
  logs
    .logger("it", { src: "logclient" })
    .error(`logclient json ${suffix}`, { password: "hunter2-x" });
  otlp.send({ message: `logclient otlp ${suffix}`, attributes: { src: "logclient" } });
  await logs.flush();
  await otlp.flush();
  assert.equal(logs.stats().sent, 1);
  assert.equal(otlp.stats().sent, 1);
  const query = client.logQuery(stream);
  const rec = await query.waitForLog(`logclient json ${suffix}`, {
    timeoutMs: 20_000,
    intervalMs: 250,
  });
  assert.equal(rec.severity, Severity.Error);
  assert.equal(rec.resource["service.name"], "ts-logclient");
  assert.equal(rec.attributes.password, "[REDACTED]");
  const res = await query.search(`logclient otlp ${suffix}`, { trackTotalHits: true });
  assert.equal(res.total, 1);
  assert.equal(res.records[0]!.body, `logclient otlp ${suffix}`);
  const sqlRes = await query.search(sql("src = 'logclient'"), { limit: 10 });
  assert.equal(sqlRes.records.length, 2);
  assert.ok((await query.listStreams()).includes(stream));
  assert.ok((await query.listLabels()).includes("src"));
  const tailed: string[] = [];
  const handle = query.tail(`needle-tail-${suffix}`, (r) => tailed.push(r.body), {
    intervalMs: 200,
  });
  try {
    await new Promise((r) => setTimeout(r, 300));
    await client.logs.ingestJson(stream, [{ message: `needle-tail-${suffix}` }]);
    await eventually(
      async () => tailed.length,
      (n) => n >= 1,
    );
  } finally {
    handle.stop();
  }
  assert.deepEqual(tailed, [`needle-tail-${suffix}`]);
});
