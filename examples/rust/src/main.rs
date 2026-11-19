// Created by sdk-kit 0.1.0; owned by this repository.
//! The OpenObserve Rust SDK end to end: configuration from the environment, health and version,
//! log ingestion and SQL search, a batching log client with a logger, tracing with
//! `init_tracing`, and reading the trace back.
//!
//! ```sh
//! OPENOBSERVE_URL=http://127.0.0.1:5080 OPENOBSERVE_USER=root@example.com \
//!   OPENOBSERVE_PASSWORD=... cargo run
//! ```

use std::sync::Arc;
use std::time::Duration;

use grow_openobserve as sdk;
use sdk::core::{ErrorInfo, Hooks};
use sdk::logging::{LogClient, LogQueryClient, SearchOptions as QueryOptions, WaitOptions};
use sdk::logs::{IngestOptions, SearchOptions};
use sdk::tracing::{
    current_trace_id, traced, TraceQueryClient, TracingConfig, WaitForTraceOptions,
};
use sdk::{ErrorCode, LogClientOptions, OpenObserveClient as Client, OpenObserveConfig as Config};
use serde_json::json;

/// Prints every retry.
struct RetryLogger;

impl Hooks for RetryLogger {
    fn on_error(&self, info: &ErrorInfo) {
        if info.will_retry {
            eprintln!(
                "retrying {} {} (attempt {})",
                info.method, info.url, info.attempt
            );
        }
    }
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error [{}]: {err}", err.code());
        std::process::exit(1);
    }
}

async fn run() -> sdk::Result<()> {
    // explicit > GROW_OPENOBSERVE_* > OPENOBSERVE_* > defaults; credentials print as [REDACTED]
    let config = Config::builder()
        .hooks(Arc::new(RetryLogger))
        .stream("example_logs")
        .traces_stream("example_traces")
        .build_from_env()?;
    println!("config: {config:?}");
    let client = Client::new(config)?;

    let health = client.health().await?; // never fails for an unreachable service
    println!("health: {} ({})", health.healthy, health.status);
    match client.server_version().await {
        Ok(version) => println!("server version: {version}"),
        Err(e) if e.code() == ErrorCode::Authentication => {
            println!("check OPENOBSERVE_USER / OPENOBSERVE_PASSWORD ({e})");
            return Ok(());
        }
        Err(e) => return Err(e),
    }

    // 1. ingest JSON records and read them back with SQL
    let logs = client.logs();
    let res = logs
        .ingest_json(
            "example_logs",
            &[json!({"message": "hello from rust", "level": "info", "user": {"id": 7}})],
            IngestOptions::default(),
        )
        .await?;
    println!("ingested {} record(s) into {}", res.successful, res.stream);
    let page = logs
        .search(
            "SELECT * FROM \"example_logs\" WHERE match_all('hello') ORDER BY _timestamp DESC",
            SearchOptions::default().track_total_hits(true),
        )
        .await?;
    println!(
        "search: {} row(s), {:?} total",
        page.hits.len(),
        page.total_hits
    );

    // 2. the logging category: a batching client, a logger and the query client
    let log_client = client.log_client(
        LogClientOptions::default().resource_attribute("service.name", "rust-example"),
    )?;
    let logger = log_client.logger(Some("example"), Default::default());
    logger.info(
        "order placed",
        sdk::attrs! { "order_id" => 42, "password" => "redacted" },
    );
    log_client.flush(Some(Duration::from_secs(5))).await?;
    let query = client.log_query(None)?;
    let record = query
        .wait_for_log(
            "order placed",
            WaitOptions::default().timeout(Duration::from_secs(10)),
        )
        .await?;
    println!(
        "found log: {} {:?}",
        record.body,
        record.attributes.get("order_id")
    );
    let recent = query
        .search("order", QueryOptions::default().limit(5))
        .await?;
    println!("recent orders: {}", recent.records.len());

    // 3. tracing: spans go to /api/{org}/v1/traces with Basic auth and the stream-name header
    let handle = client.init_tracing(
        TracingConfig::new("rust-example").with_set_global(true),
        None,
    )?;
    let trace_id = traced("checkout", async {
        traced("charge-card", async { Ok::<_, std::io::Error>(()) }).await?;
        Ok::<_, std::io::Error>(current_trace_id().unwrap_or_default())
    })
    .await
    .map_err(|e| sdk::Error::unavailable(e.to_string()))?;
    handle.force_flush(Some(Duration::from_secs(5))).await?;
    let trace = client
        .traces()
        .wait_for_trace(&trace_id, WaitForTraceOptions::default().min_spans(2))
        .await?;
    println!("trace {} has {} spans", trace.trace_id, trace.spans.len());
    println!("services: {:?}", client.traces().list_services().await?);
    handle.shutdown().await?;

    client.close().await; // flushes the log client too
    Ok(())
}
