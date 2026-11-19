# Ingest

Four ways to write logs. All of them create the stream on first write and accept `gzip`.

| Function | Request | Notes |
| - | - | - |
| `ingestJson(stream, records)` | `POST /api/{org}/{stream}/_json` | JSON array; nested objects are flattened with `_` (`user.id` → `user_id`) |
| `ingestMulti(stream, ndjson)` | `POST /api/{org}/{stream}/_multi` | newline-delimited JSON; one bad line rejects the whole request (`VALIDATION`) |
| `ingestBulk(actions)` | `POST /api/{org}/_bulk` | Elasticsearch format; only `index` and `create` — `delete`/`update` are rejected client-side (`VALIDATION`) because the server silently ignores them |
| `ingestOtlp(records, {stream})` | `POST /api/{org}/v1/logs` + `stream-name` | SDK `LogRecord`s as OTLP/HTTP JSON; trace flags are not stored by OpenObserve |

Behaviour (identical in every language):

1. `_timestamp` may be epoch **microseconds** (milliseconds, nanoseconds and RFC 3339 strings are
   detected by the server); without it the server time is used. Records older than the ingest
   window are rejected per record.
2. The server answers **HTTP 200 even when records were rejected**. `ingestJson`/`ingestMulti`
   return `{stream, successful, failed, error?}`; `ingestBulk` returns `{took, errors, items}` —
   always check `errors`; `ingestOtlp` returns `{accepted, rejected}`.
3. Server error texts in results are passed through redaction.

=== "TypeScript"

    ```ts
    const logs = client.logs;
    const r = await logs.ingestJson("app", [
      { message: "payment failed", level: "error", _timestamp: Date.now() * 1000 },
    ]);
    if (r.failed > 0) console.warn(r.error);

    await logs.ingestMulti("app", '{"message":"a"}\n{"message":"b"}\n', { gzip: true });
    await logs.ingestMulti("app", [{ message: "a" }, { message: "b" }]);

    const bulk = await logs.ingestBulk([
      { stream: "app", document: { message: "via bulk" } },
      { action: "create", stream: "audit", document: { message: "created" } },
    ]);
    if (bulk.errors) console.warn(bulk.items.filter((i) => i.error));

    await logs.ingestOtlp([{ timestamp: new Date(), severity: Severity.Info, severityText: "INFO",
      body: "otlp line", attributes: {}, resource: { "service.name": "api" } }], { stream: "app" });
    ```

=== "Rust"

    ```rust
    use grow_openobserve::logs::{BulkAction, IngestOptions, OtlpIngestOptions};
    use serde_json::json;

    let logs = client.logs();
    let r = logs
        .ingest_json("app", &[json!({"message": "payment failed", "level": "error"})], IngestOptions::default())
        .await?;
    logs.ingest_multi("app", "{\"message\":\"a\"}\n{\"message\":\"b\"}\n", IngestOptions::gzip(true)).await?;
    logs.ingest_multi_records("app", &[json!({"message": "a"})], IngestOptions::default()).await?;

    let bulk = logs
        .ingest_bulk(
            &[BulkAction::index("app", json!({"message": "via bulk"})),
              BulkAction::create("audit", json!({"message": "created"}))],
            IngestOptions::default(),
        )
        .await?;
    assert!(!bulk.errors);

    let otlp = logs
        .ingest_otlp(&records, OtlpIngestOptions { stream: Some("app".into()), gzip: true })
        .await?;
    ```

=== "Go"

    ```go
    logs := client.Logs()
    r, err := logs.IngestJSON(ctx, "app", []map[string]any{
        {"message": "payment failed", "level": "error", "_timestamp": time.Now().UnixMicro()},
    })
    if r.Failed > 0 { /* r.Streams[0].Error */ }

    _, err = logs.IngestMulti(ctx, "app", []byte("{\"message\":\"a\"}\n{\"message\":\"b\"}\n"))

    bulk, err := logs.IngestBulk(ctx, []openobserve.BulkAction{
        {Index: "app", Document: map[string]any{"message": "via bulk"}},
        {Action: "create", Index: "audit", Document: map[string]any{"message": "created"}},
    })
    if bulk.Errors { /* inspect bulk.Items */ }

    res, err := logs.IngestOTLP(ctx, []openobserve.LogRecord{{
        Timestamp: time.Now(), Severity: openobserve.SeverityInfo, Body: "otlp line",
        Resource: map[string]openobserve.AttrValue{"service.name": "api"},
    }}, openobserve.OTLPIngestOptions{Stream: "app", Gzip: true})
    ```

=== "Python"

    ```python
    import time
    from grow_openobserve import BulkAction

    r = client.logs.ingest_json(
        "app", [{"message": "payment failed", "level": "error", "_timestamp": int(time.time() * 1e6)}]
    )
    if r.failed:
        print(r.error)

    client.logs.ingest_multi("app", '{"message":"a"}\n{"message":"b"}\n', gzip=True)
    client.logs.ingest_multi("app", [{"message": "a"}, {"message": "b"}])

    bulk = client.logs.ingest_bulk(
        [
            BulkAction(stream="app", document={"message": "via bulk"}),
            BulkAction(stream="audit", document={"message": "created"}, action="create"),
        ]
    )
    assert not bulk.errors

    result = client.logs.ingest_otlp(records, stream="app", gzip=True)  # records: list[LogRecord]
    ```

=== "Java"

    ```java
    Logs logs = client.logs();
    IngestResult r = logs.ingestJson("app",
        List.of(Map.of("message", "payment failed", "level", "error")));
    if (r.failed() > 0) { /* r.streams().get(0).error() */ }

    logs.ingestMulti("app", "{\"message\":\"a\"}\n{\"message\":\"b\"}\n");

    BulkResult bulk = logs.ingestBulk(List.of(
        BulkAction.index("app", Map.of("message", "via bulk")),
        BulkAction.create("audit", Map.of("message", "created"))));
    if (bulk.errors()) { /* bulk.items() */ }

    OtlpIngestResult otlp = logs.ingestOtlp(List.of(
        LogRecord.builder().timestamp(Instant.now()).body("otlp line")
            .resource(Map.of("service.name", "api")).build()), "app", true);
    ```

For continuous shipping use the batching [log client](logging.md) instead of calling these per
record: it batches, compresses, redacts and retries in the background.
