# Log client and search

The [Logging](../logging.md) category (batching `LogClient`, `Logger`, `RedactionProcessor`,
framework adapters, `LogQueryClient`) bound to OpenObserve.

| Function | What it returns |
| - | - |
| `logClient({stream, protocol, batch, resource, redaction})` | a `BatchingLogClient` that exports to one stream; closed and flushed by `client.close()` |
| `logExporter(stream, protocol)` | the `LogExporter` behind it: `json` → `_json` ingestion (default), `otlp` → OTLP/HTTP JSON to `/v1/logs` (Go: `NewJSONLogExporter(stream)` / `NewOTLPLogExporter(stream)`) |
| `logQuery(stream)` | a `LogQueryClient` on the stream (default: the config's `stream`) |

Records written by the `json` exporter are flat rows: `_timestamp`, `body`, `severity`,
`severity_number`, `trace_id`, `span_id`, `trace_flags`, the resource keys (`service_name`,
`service_version`, `deployment_environment`, `host_name`) and the attributes with dots turned into
underscores. The `otlp` exporter stores the OTLP column names; OpenObserve does not store trace
flags for OTLP logs.

## `LogQueryClient`

| Function | Behaviour |
| - | - |
| `search(query, {start, end, limit, order, cursor})` | a query that starts with `SELECT` (any case) is **raw SQL**; anything else is a full-text search `match_all('<query>')` on the stream; empty matches everything. Default last 15 min, limit 100, newest first. The cursor is the row offset of the next page. `total` is set only with `trackTotalHits` (a second count query). |
| `tail(query, onRecord, {start, interval})` | the polling tail of [Search and tail](search.md#tail), delivering normalized `LogRecord`s |
| `listStreams()` | names of the log streams |
| `listLabels()` | field names of the stream's schema |
| `waitForLog(query, {timeout, interval, predicate})` | polls `search` until a record matches; `TIMEOUT` otherwise |

Rows are normalized into `LogRecord`: `_timestamp`, then `body` or `message`, `severity` or
`level` (+ `severity_number`), `trace_id`, `span_id`, `trace_flags`, the resource keys above;
every other column becomes an attribute.

=== "TypeScript"

    ```ts
    const logs = client.logClient({ stream: "app", resource: { "service.name": "api" } });
    const log = logs.logger("checkout", { region: "eu" });
    log.warn("card declined", { orderId: 42, token: "never-sent" }); // token → [REDACTED]
    await logs.flush();

    const query = client.logQuery("app");
    const rec = await query.waitForLog("card declined", { timeoutMs: 10_000 });
    const page = await query.search("card declined", { limit: 20, trackTotalHits: true });
    const sqlPage = await query.search(`SELECT * FROM "app" WHERE region = 'eu'`);
    const otlp = client.logClient({ stream: "app_otlp", protocol: "otlp" });
    ```

=== "Rust"

    ```rust
    use grow_openobserve::logging::{LogClient, LogQueryClient, SearchOptions, WaitOptions};
    use grow_openobserve::{attrs, LogClientOptions, LogProtocol};

    let logs = client.log_client(
        LogClientOptions::default().stream("app").resource_attribute("service.name", "api"),
    )?;
    logs.logger(Some("checkout"), Default::default())
        .warn("card declined", attrs! { "order_id" => 42, "token" => "never-sent" });
    logs.flush(None).await?;

    let query = client.log_query(Some("app"))?;
    let rec = query.wait_for_log("card declined", WaitOptions::default()).await?;
    let page = query.search("card declined", SearchOptions::default().limit(20)).await?;
    let otlp = client.log_client(LogClientOptions::default().stream("app_otlp").protocol(LogProtocol::Otlp))?;
    ```

=== "Go"

    ```go
    logs, err := client.NewLogClient("app", openobserve.LogClientOptions{
        Resource: map[string]openobserve.AttrValue{"service.name": "api"},
    })
    logs.Logger("checkout", map[string]openobserve.AttrValue{"region": "eu"}).
        Warn(ctx, "card declined", "order_id", 42, "token", "never-sent")
    _ = logs.Flush(ctx)
    slog.New(openobserve.NewSlogHandler(logs, nil)).Info("via slog")

    q := client.LogQuery("app")
    rec, err := q.WaitForLog(ctx, "card declined", openobserve.WaitForLogOptions{Timeout: 10 * time.Second})
    page, err := q.Search(ctx, "card declined", openobserve.SearchOptions{Limit: 20})
    otlp, err := client.NewLogClientWith(client.NewOTLPLogExporter("app_otlp"), openobserve.LogClientOptions{})
    ```

=== "Python"

    ```python
    import logging
    from grow_openobserve import LogHandler

    logs = client.log_client("app", resource={"service.name": "api"})
    logs.logger("checkout", {"region": "eu"}).warn(
        "card declined", {"order_id": 42, "token": "never-sent"}
    )
    logging.getLogger("app").addHandler(LogHandler(logs))
    logs.flush(10)

    query = client.log_query("app")
    rec = query.wait_for_log("card declined", timeout_s=10)
    page = query.search("card declined", limit=20, track_total_hits=True)
    otlp = client.log_client("app_otlp", protocol="otlp")
    ```

=== "Java"

    ```java
    BatchingLogClient logs = client.logClient(LogClientOptions.builder()
        .stream("app").resource(Map.of("service.name", "api")).build());
    logs.logger("checkout", Map.of("region", "eu"))
        .warn("card declined", Map.of("orderId", 42, "token", "never-sent"));
    logs.flush(Duration.ofSeconds(5));

    OpenObserveLogQuery query = client.logQuery("app");
    LogRecord rec = query.waitForLog("card declined",
        WaitForLogOptions.of(Duration.ofSeconds(10), Duration.ofMillis(250)));
    LogSearchResult page = query.search("card declined", SearchOptions.builder().limit(20).build());
    BatchingLogClient otlp = client.logClient(LogClientOptions.builder()
        .stream("app_otlp").protocol(LogClientOptions.Protocol.OTLP).build());
    ```
