# Traces

## Sending spans: `initTracing`

`initTracing` sets up the official OpenTelemetry SDK of each language with an exporter for
OpenObserve; everything else (sampler, propagators, batch settings, span helpers, framework
adapters) is the shared [Tracing](../tracing.md) setup.

| Protocol | Endpoint | Headers |
| - | - | - |
| `http/protobuf` (default) | `<url>/api/{org}/v1/traces` | `Authorization: Basic …`, `stream-name: <tracesStream>` |
| `grpc` | `<host>:5081` (the URL's host; port 5080 → 5081) | `authorization`, `organization`, `stream-name` metadata |
| `native` | `<url>/api/{org}/v1/traces` | as `http/protobuf`; the body is **OTLP/HTTP JSON** in TypeScript and Rust and OTLP/HTTP protobuf in Go, Python and Java (their official exporters have no JSON encoding) |

Values you set in the tracing config (endpoint, headers, TLS) win over the defaults. The
client-bound form `client.initTracing(config)` uses the client's URL, org, credentials and
`tracesStream`; the free function takes an OpenObserve config (default: from the environment).

=== "TypeScript"

    ```ts
    import { initTracing, withSpan } from "@grow-codes/openobserve";

    const tracing = client.initTracing({ serviceName: "checkout", serviceVersion: "1.4.0" });
    // or: initTracing({ serviceName: "checkout", openobserve: config, stream: "traces_prod" })
    await withSpan("charge card", async (span) => span.setAttribute("order.id", 42));
    await tracing.shutdown();
    ```

=== "Rust"

    ```rust
    use grow_openobserve::tracing::{traced, TracingConfig};

    let tracing = client.init_tracing(TracingConfig::new("checkout"), Some("traces_prod"))?;
    traced("charge card", async { Ok::<_, std::io::Error>(()) }).await?;
    tracing.shutdown().await?;
    ```

=== "Go"

    ```go
    tracing, err := client.InitTracing(ctx, openobserve.TracingConfig{ServiceName: "checkout"})
    // or: openobserve.InitTracing(ctx, cfg, openobserve.TracingConfig{...})
    if err != nil {
        return err
    }
    defer tracing.Shutdown(context.Background())
    ctx, span := tracing.Tracer("checkout").Start(ctx, "charge card")
    span.End()
    ```

=== "Python"

    ```python
    from grow_openobserve import TracingConfig, with_span

    tracing = client.init_tracing(TracingConfig(service_name="checkout"), stream="traces_prod")
    # or: init_tracing(TracingConfig(...), openobserve=config)
    with with_span("charge card"):
        ...
    tracing.shutdown()
    ```

=== "Java"

    ```java
    TracingHandle tracing = client.initTracing(TracingConfig.builder("checkout").build());
    // or: OpenObserveTracing.initTracing(config, TracingConfig.builder("checkout").build())
    Spans.withSpan("charge card", () -> null);
    tracing.shutdown();
    ```

## Reading traces: `TraceQueryClient`

`client.traces` queries the configured `tracesStream`; `traceQuery(stream)` (Go `TraceQuery`)
queries another one.

| Function | Request | Defaults and behaviour |
| - | - | - |
| `getTrace(traceId)` | `GET /api/{org}/{stream}/traces/{id}/details` (paginated) | window: last 7 days (TypeScript, Python and Rust accept `start`/`end`); `null` for an unknown trace or missing stream |
| `searchTraces(query)` | `GET /api/{org}/{stream}/traces/latest` with a SQL filter | last 1 h, limit 20; `service`, `operation`, `tags`, `minDuration`/`maxDuration`, `start`/`end` |
| `listServices()` | `SELECT DISTINCT service_name …` | last 24 h |
| `listOperations(service)` | `SELECT DISTINCT operation_name …` | last 24 h |
| `getDependencies(range)` | — | always `UNSUPPORTED`: the service graph is an enterprise feature (404 in OSS) |
| `waitForTrace(traceId, {timeout, interval, minSpans})` | polls `getTrace` | 30 s, 1 s, 1 span; `TIMEOUT` when it does not appear |
| `traceTimeRange(traceId)` | `GET /api/{org}/traces/time_range?trace_id=` | `{found, stream, start, end}` |

Span mapping: `start_time`/`end_time` are nanoseconds on the wire and become `startMicros` /
`endMicros`; `duration` is already microseconds. Attributes come back flattened and stringified
(`http.status_code` → `http_status_code: "200"`), events and links are decoded from their JSON
strings. A tag filter matches **single spans**: `searchTraces` returns traces that contain one span
matching every condition, and the summary covers the matching spans only.

=== "TypeScript"

    ```ts
    const t = await client.traces.waitForTrace(traceId, { timeoutMs: 30_000 });
    const trace = await client.traces.getTrace(traceId);
    const recent = await client.traces.searchTraces({
      service: "checkout",
      tags: { "db.system": "postgresql" },
      minDurationMicros: 1_000,
      limit: 10,
    });
    const services = await client.traces.listServices();
    const ops = await client.traces.listOperations("checkout");
    const range = await client.traceQuery("traces_prod").traceTimeRange(traceId);
    ```

=== "Rust"

    ```rust
    use grow_openobserve::tracing::{TraceQueryClient, TraceSearchQuery, WaitForTraceOptions};

    let traces = client.traces();
    let t = traces.wait_for_trace(&trace_id, WaitForTraceOptions::default().min_spans(2)).await?;
    let trace = traces.get_trace(&trace_id).await?; // Option<Trace>
    let recent = traces.search_traces(&TraceSearchQuery::service("checkout").operation("db.query")).await?;
    let services = traces.list_services().await?;
    let ops = traces.list_operations("checkout").await?;
    let range = client.trace_query(Some("traces_prod")).trace_time_range(&trace_id).await?;
    ```

=== "Go"

    ```go
    traces := client.Traces()
    t, err := traces.WaitForTrace(ctx, traceID, openobserve.WaitForTraceOptions{MinSpans: 2})
    trace, err := traces.GetTrace(ctx, traceID) // nil for an unknown trace
    recent, err := traces.SearchTraces(ctx, openobserve.TraceSearchQuery{
        Service: "checkout", Tags: map[string]string{"db.system": "postgresql"}, MinDuration: time.Millisecond,
    })
    services, err := traces.ListServices(ctx)
    ops, err := traces.ListOperations(ctx, "checkout")
    rng, err := client.TraceQuery("traces_prod").TraceTimeRange(ctx, traceID)
    ```

=== "Python"

    ```python
    from grow_openobserve import TraceSearchQuery

    t = client.traces.wait_for_trace(trace_id, timeout_s=30)
    trace = client.traces.get_trace(trace_id)  # None for an unknown trace
    recent = client.traces.search_traces(
        TraceSearchQuery(service="checkout", tags={"db.system": "postgresql"}, limit=10)
    )
    services = client.traces.list_services()
    ops = client.traces.list_operations("checkout")
    time_range = client.trace_query("traces_prod").trace_time_range(trace_id)
    ```

=== "Java"

    ```java
    OpenObserveTraceQuery traces = client.traces();
    Trace t = traces.waitForTrace(traceId);
    Optional<Trace> trace = traces.getTrace(traceId);
    List<TraceSummary> recent = traces.searchTraces(
        TraceSearchQuery.builder().service("checkout").tag("db.system", "postgresql").limit(10).build());
    List<String> services = traces.listServices();
    List<String> ops = traces.listOperations("checkout");
    TraceTimeRange range = client.traceQuery("traces_prod").traceTimeRange(traceId);
    ```
