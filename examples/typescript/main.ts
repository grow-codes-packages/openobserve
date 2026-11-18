// OpenObserve TypeScript SDK tour: configuration from the environment, health and version,
// raw ingestion + SQL search, the batching LogClient + LogQueryClient, streams, and tracing.
//
//   (cd ../../typescript && npm install && npm run build)   # or use the published package
//   npm install
//   OPENOBSERVE_URL=http://127.0.0.1:5080 OPENOBSERVE_USER=root@example.com \
//   OPENOBSERVE_PASSWORD=... npm start

import { OpenObserveClient, OpenObserveError, initTracing, withSpan } from "@grow-codes/openobserve";

const client = OpenObserveClient.fromEnv({
  // Warnings only; the default logger is silent.
  logger: { debug() {}, info() {}, warn: console.warn, error: console.error },
});
console.log(`config: ${client.config}`); // credentials print as [REDACTED]

const stream = `example_${Date.now().toString(36)}`;

try {
  console.log("health:", await client.health()); // never throws for an unreachable service
  console.log("server version:", await client.serverVersion());

  // 1. Raw ingestion (µs timestamps; the server flattens nested fields: user.id → user_id).
  const ingested = await client.logs.ingestJson(stream, [
    { message: "checkout started", level: "info", user: { id: 7 } },
    { message: "payment failed", level: "error", user: { id: 7 } },
  ]);
  console.log("ingested:", ingested);

  // 2. SQL search (quote stream names; times are converted to microseconds for you).
  const found = await client.logs.search(
    `SELECT * FROM "${stream}" WHERE level = 'error' ORDER BY _timestamp DESC`,
    { start: new Date(Date.now() - 5 * 60_000), trackTotalHits: true },
  );
  console.log(`errors: ${found.totalHits}`, found.hits[0]);
  console.log("levels:", await client.logs.valuesOf(stream, ["level"]));

  // 3. Batching LogClient (redacts secrets before sending) + LogQueryClient.
  const logs = client.logClient({ stream, resource: { "service.name": "example" } });
  logs.logger("checkout").warn("card declined", { orderId: 42, token: "never-sent" });
  await logs.flush();
  const record = await client.logQuery(stream).waitForLog("card declined", { timeoutMs: 10_000 });
  console.log("read back:", record.severityText, record.body, record.attributes);

  // 4. Streams.
  await client.streams.updateStreamSettings(stream, { retentionDays: 7, fullTextSearchKeys: ["message"] });
  const schema = await client.streams.getStreamSchema(stream);
  console.log("fields:", schema.schema.map((f) => f.name).join(", "));

  // 5. Tracing: OTLP/HTTP to /api/{org}/v1/traces with the same credentials.
  const tracing = initTracing({ serviceName: "example", openobserve: client.config });
  const traceId = await withSpan("checkout", async (span) => {
    span.setAttribute("order.id", 42);
    return span.spanContext().traceId;
  });
  await tracing.forceFlush();
  const trace = await client.traces.waitForTrace(traceId, { timeoutMs: 10_000 });
  console.log("trace:", trace.spans.map((s) => `${s.serviceName} ${s.name} ${s.durationMicros}µs`));
  await tracing.shutdown();

  await client.streams.deleteStream(stream);
} catch (err) {
  if (err instanceof OpenObserveError) {
    // Canonical code + HTTP status; the message never contains credentials.
    console.error(`${err.code}: ${err.message}`, { httpStatus: err.httpStatus });
    process.exitCode = 1;
  } else {
    throw err;
  }
} finally {
  await client.close(); // flushes the LogClient too
}
