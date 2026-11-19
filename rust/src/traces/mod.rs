// Service-owned. The OpenObserve traces module (API.md §5).
//! Traces: [`init_tracing`] (OTLP exporters to `/api/{org}/v1/traces` with Basic auth and the
//! `stream-name` header) and [`OpenObserveTraceQuery`], the
//! [`TraceQueryClient`](crate::tracing::TraceQueryClient) for OpenObserve.
//!
//! ```no_run
//! # async fn demo() -> grow_openobserve::Result<()> {
//! use grow_openobserve::tracing::{TraceQueryClient, TracingConfig};
//! use grow_openobserve::OpenObserveClient;
//!
//! let client = OpenObserveClient::from_env()?;
//! let handle = client.init_tracing(TracingConfig::new("checkout").with_set_global(false), None)?;
//! // … create spans with handle.tracer(…) …
//! handle.force_flush(None).await?;
//! let services = client.traces().list_services().await?;
//! println!("{services:?}");
//! handle.shutdown().await?;
//! # Ok(()) }
//! ```

mod init;
mod query;

pub use init::{
    create_span_exporter, grpc_endpoint, init_tracing, openobserve_tracing_defaults,
    traces_endpoint, OpenObserveTracingConfig, OPENOBSERVE_GRPC_PORT,
};
pub use query::{
    hit_to_summary, row_to_span, trace_filter, OpenObserveTraceQuery, TraceTimeRange,
    TraceWindowOptions, DEFAULT_GET_TRACE_WINDOW, DEFAULT_SERVICES_WINDOW,
    DEFAULT_TRACE_SEARCH_WINDOW,
};
