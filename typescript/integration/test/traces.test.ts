// Tracing against the real service: initTracing exports spans, the query client reads them back.

import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { SpanKind, SpanStatusCode, context, trace } from "@opentelemetry/api";
import {
  OpenObserveClient,
  UnsupportedError,
  initTracing,
  type TracingHandle,
} from "@grow-codes/openobserve";
import { rootClient, suffix, uniqueStream } from "./helpers.ts";

const stream = uniqueStream("traces");
const service = `ts-svc-${suffix}`;
let client: OpenObserveClient;
let handle: TracingHandle;
let traceId = "";

before(async () => {
  client = rootClient();
  handle = initTracing({
    serviceName: service,
    serviceVersion: "1.2.3",
    environment: "test",
    setGlobal: false,
    openobserve: client.config,
    stream,
  });
  const tracer = handle.tracer("it");
  const root = tracer.startSpan("GET /checkout", {
    kind: SpanKind.SERVER,
    attributes: { "http.method": "GET" },
  });
  const ctx = trace.setSpan(context.active(), root);
  const child = tracer.startSpan(
    "db.query",
    { kind: SpanKind.CLIENT, attributes: { "db.system": "postgresql" } },
    ctx,
  );
  child.addEvent("cache.miss", { key: "u1" });
  child.setStatus({ code: SpanStatusCode.ERROR, message: "boom" });
  child.end();
  root.end();
  traceId = root.spanContext().traceId;
  await handle.forceFlush(10_000);
});

after(async () => {
  await handle.shutdown();
  await client.streams.deleteStream(stream, { type: "traces" }).catch(() => undefined);
  await client.close();
});

test("initTracing → waitForTrace / getTrace round trip with the span tree", async () => {
  const q = client.traceQuery(stream);
  const t = await q.waitForTrace(traceId, { timeoutMs: 30_000, intervalMs: 250, minSpans: 2 });
  assert.equal(t.traceId, traceId);
  const root = t.spans.find((s) => s.name === "GET /checkout")!;
  const child = t.spans.find((s) => s.name === "db.query")!;
  assert.equal(root.kind, "server");
  assert.equal(root.serviceName, service);
  assert.equal(child.parentSpanId, root.spanId);
  assert.equal(child.kind, "client");
  assert.equal(child.status.code, "error");
  assert.equal(child.status.message, "boom");
  assert.equal(child.attributes.db_system, "postgresql");
  assert.equal(child.events[0]?.name, "cache.miss");
  assert.ok(root.durationMicros >= child.durationMicros);
  const again = await q.getTrace(traceId);
  assert.equal(again?.spans.length, 2);
  assert.equal(await q.getTrace("ffffffffffffffffffffffffffffffff"), null);
});

test("searchTraces finds the trace by service, operation and tag", async () => {
  const q = client.traceQuery(stream);
  const all = await q.searchTraces({ limit: 10 });
  const hit = all.find((s) => s.traceId === traceId);
  assert.ok(hit, JSON.stringify(all));
  assert.equal(hit.rootServiceName, service);
  assert.equal(hit.rootName, "GET /checkout");
  assert.equal(hit.spanCount, 2);
  const byService = await q.searchTraces({
    service,
    operation: "db.query",
    tags: { "db.system": "postgresql" },
  });
  assert.ok(byService.some((s) => s.traceId === traceId));
  const none = await q.searchTraces({ service: `missing-${suffix}` });
  assert.deepEqual(none, []);
  const slow = await q.searchTraces({ service, minDurationMicros: 60_000_000 });
  assert.deepEqual(slow, []);
});

test("listServices / listOperations / traceTimeRange", async () => {
  const q = client.traceQuery(stream);
  assert.ok((await q.listServices()).includes(service));
  assert.deepEqual(await q.listOperations(service), ["GET /checkout", "db.query"]);
  const range = await q.traceTimeRange(traceId);
  assert.equal(range.found, true);
  assert.equal(range.stream, stream);
  assert.ok((range.endMicros ?? 0) >= (range.startMicros ?? 0));
  assert.deepEqual(await q.traceTimeRange("ffffffffffffffffffffffffffffffff"), { found: false });
});

test("getDependencies is unsupported in OSS; client.traces uses the configured traces stream", async () => {
  await assert.rejects(client.traceQuery(stream).getDependencies(), UnsupportedError);
  assert.equal(client.traces.stream, process.env.OPENOBSERVE_TRACES_STREAM);
});

test("native protocol (OTLP/JSON) exports too", async () => {
  const h = initTracing({
    serviceName: `${service}-json`,
    protocol: "native",
    setGlobal: false,
    openobserve: client.config,
    stream,
  });
  const span = h.tracer("it").startSpan("json-op");
  span.end();
  await h.forceFlush(10_000);
  await h.shutdown();
  const t = await client
    .traceQuery(stream)
    .waitForTrace(span.spanContext().traceId, { timeoutMs: 30_000, intervalMs: 250 });
  assert.equal(t.spans[0]?.serviceName, `${service}-json`);
});
