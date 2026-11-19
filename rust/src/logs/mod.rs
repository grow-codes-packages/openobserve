// Service-owned. The OpenObserve logs module (API.md §2–§4).
//! Logs: ingestion (`_json`, `_multi`, `_bulk`, OTLP), search (`_search`, `_around`,
//! `_values`, histogram, polling tail), stream management and the category adapters
//! ([`OpenObserveLogExporter`], [`OpenObserveLogQuery`]).
//!
//! ```no_run
//! # async fn demo() -> grow_openobserve::Result<()> {
//! use grow_openobserve::logs::{IngestOptions, SearchOptions};
//! use grow_openobserve::OpenObserveClient;
//! use serde_json::json;
//!
//! let client = OpenObserveClient::from_env()?;
//! client
//!     .logs()
//!     .ingest_json("app", &[json!({"message": "hello", "level": "info"})], IngestOptions::default())
//!     .await?;
//! let page = client
//!     .logs()
//!     .search("SELECT * FROM \"app\" WHERE match_all('hello')", SearchOptions::default())
//!     .await?;
//! println!("{} rows", page.hits.len());
//! # Ok(()) }
//! ```

mod api;
mod exporter;
mod query;
mod records;
mod streams;

pub use api::{
    BulkAction, BulkItem, BulkItemError, BulkOp, BulkResult, FieldValue, FieldValues, Histogram,
    HistogramBucket, HistogramOptions, IngestOptions, IngestResult, LogTailOptions,
    OpenObserveLogs, OtlpIngestOptions, OtlpIngestResult, RowCallback, SearchAroundOptions,
    SearchOptions, SearchResult, TailErrorCallback, ValuesOptions, DEFAULT_SEARCH_WINDOW,
};
pub use exporter::{OpenObserveLogExporter, OpenObserveOtlpLogExporter};
pub use query::{is_sql_query, text_query_sql, OpenObserveLogQuery};
pub use records::{
    log_record_to_row, otlp_attributes, otlp_value, row_to_log_record, to_otlp_logs_request,
};
pub use streams::{
    CreateStreamOptions, ListStreamsOptions, ListUpdate, OpenObserveStreams, PartitionKey,
    PartitionKeyInput, SchemaField, StreamInfo, StreamSettings, StreamSettingsUpdate, StreamStats,
};
