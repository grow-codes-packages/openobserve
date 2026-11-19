# grow-openobserve (Rust)

Rust SDK for [OpenObserve](https://openobserve.ai) (logs and traces) by
[Grow Codes](https://grow.codes). Async on tokio, with an optional blocking client. Tested
against `public.ecr.aws/zinclabs/openobserve:v1.1.2`.

```toml
[dependencies]
grow-openobserve = "0.1.0"
# blocking client:   features = ["blocking"]
# test fakes/runner: features = ["testing"] (dev-dependency)
```

```rust,no_run
use grow_openobserve::logs::{IngestOptions, SearchOptions};
use grow_openobserve::OpenObserveClient;
use serde_json::json;

#[tokio::main]
async fn main() -> grow_openobserve::Result<()> {
    // explicit > GROW_OPENOBSERVE_* > OPENOBSERVE_* > defaults
    let client = OpenObserveClient::from_env()?;
    println!("{} {}", client.health().await?.status, client.server_version().await?);
    client
        .logs()
        .ingest_json("app", &[json!({"message": "hello", "level": "info"})], IngestOptions::default())
        .await?;
    let page = client
        .logs()
        .search("SELECT * FROM \"app\" WHERE match_all('hello')", SearchOptions::default())
        .await?;
    println!("{} rows", page.hits.len());
    client.close().await;
    Ok(())
}
```

## Configuration

| Field | Variables | Default |
| - | - | - |
| url | `GROW_OPENOBSERVE_URL`, `OPENOBSERVE_URL` | `http://127.0.0.1:5080` |
| org | `GROW_OPENOBSERVE_ORG`, `OPENOBSERVE_ORG` | `default` |
| user | `GROW_OPENOBSERVE_USER`, `OPENOBSERVE_USER` | none |
| password | `GROW_OPENOBSERVE_PASSWORD`, `OPENOBSERVE_PASSWORD` | none |
| token | `GROW_OPENOBSERVE_TOKEN`, `OPENOBSERVE_TOKEN` | none |
| stream | `GROW_OPENOBSERVE_STREAM`, `OPENOBSERVE_STREAM` | `default` |
| traces stream | `GROW_OPENOBSERVE_TRACES_STREAM`, `OPENOBSERVE_TRACES_STREAM` | `default` |

OpenObserve only accepts HTTP Basic auth: `user` + `password`, or a service-account e-mail
(`user`) + its `token`. A token starting with `Basic ` is a pre-encoded header and needs no user.

## Modules

| Module | Functions |
| - | - |
| client | `health`, `ping`, `server_version`, `close`, `log_client`, `log_query`, `trace_query`, `init_tracing` |
| `client.logs()` | `ingest_json`, `ingest_multi`, `ingest_multi_records`, `ingest_bulk`, `ingest_bulk_raw`, `ingest_otlp`, `search`, `search_around`, `values_of`, `histogram`, `tail` |
| `client.streams()` | `list_streams`, `get_stream_schema`, `update_stream_settings`, `create_stream`, `delete_stream` |
| `client.traces()` | `get_trace`, `get_trace_in`, `search_traces`, `list_services(_in)`, `list_operations(_in)`, `trace_time_range`, `wait_for_trace` (`get_dependencies` → `UNSUPPORTED`, enterprise only) |
| `client.admin()` | `list_organizations`, `create_service_account`, `list_service_accounts`, `rotate_service_account_token`, `delete_service_account`, `get_passcode` |
| `traces::init_tracing` | OpenTelemetry pipeline exporting OTLP/HTTP protobuf (default), OTLP/JSON (`Protocol::Native`) or gRPC to `/api/{org}/v1/traces` |

```rust,no_run
# async fn demo(client: grow_openobserve::OpenObserveClient) -> grow_openobserve::Result<()> {
use grow_openobserve::logging::{LogClient, LogQueryClient, WaitOptions};
use grow_openobserve::tracing::{traced, TraceQueryClient, TracingConfig};
use grow_openobserve::LogClientOptions;

let logs = client.log_client(LogClientOptions::default().stream("app"))?;
logs.logger(Some("checkout"), Default::default()).info("order placed", Default::default());
logs.flush(None).await?;
client.log_query(Some("app"))?.wait_for_log("order placed", WaitOptions::default()).await?;

let tracing = client.init_tracing(TracingConfig::new("checkout"), None)?;
traced("charge", async { Ok::<_, std::io::Error>(()) }).await.ok();
tracing.force_flush(None).await?;
println!("{:?}", client.traces().list_services().await?);
# Ok(()) }
```

- Errors: one `Error` enum with `code()`, `http_status()`, `request_id()`, `is_retryable()`,
  `retry_after()`; messages never contain credentials. "Stream not found" (HTTP 400, vendor code
  20002) maps to `NOT_FOUND`, "already exists" to `CONFLICT`.
- Times: options take `Timestamp` (a `SystemTime` or epoch microseconds); the SDK always sends
  microseconds (milliseconds silently match nothing on the server).
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation (drop the future), request hooks, `tracing` debug events.
- MSRV 1.88. Licence Apache-2.0. Documentation: https://grow-codes-packages.github.io/openobserve/

Development: `make deps build lint test coverage license pack docs` (see `Makefile`).
