// Service-owned. Logs ingestion (`_json`, `_multi`, `_bulk`, OTLP) and search (`_search`,
// `_around`, `_values`, histogram, polling tail) — API.md §2–§3.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use serde_json::{json, Value};

use super::records::to_otlp_logs_request;
use crate::core::transport::{RequestBuilder, SDK_VERSION};
use crate::core::{Error, Method, Result};
use crate::http::{
    field, gzip, int, items, number, send_json, sql_ident, stream_name, system_time_to_micros,
    text, uint, window, JsonObject, StreamType, Timestamp,
};
use crate::logging::{LogRecord, TailHandle};
use crate::OpenObserveClient;

/// Default search window when `start` is not given: the last 15 minutes.
pub const DEFAULT_SEARCH_WINDOW: Duration = Duration::from_secs(15 * 60);

/// Options of the ingest calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestOptions {
    /// Send the body with `Content-Encoding: gzip` (default false).
    pub gzip: bool,
}

impl IngestOptions {
    /// Options with gzip on or off.
    pub fn gzip(gzip: bool) -> Self {
        IngestOptions { gzip }
    }
}

/// Result of `ingest_json` / `ingest_multi`: per-stream counters (HTTP 200 even on partial
/// failure).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestResult {
    /// Stream name as normalized by the server.
    pub stream: String,
    /// Records stored.
    pub successful: u64,
    /// Records rejected.
    pub failed: u64,
    /// Last per-record error reported by the server (e.g. "Too old data …").
    pub error: Option<String>,
}

/// `_bulk` action. Only `index` and `create` exist: OpenObserve silently ignores `delete` and
/// `update`, so the SDK cannot send them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BulkOp {
    /// `index` (default).
    #[default]
    Index,
    /// `create`.
    Create,
}

impl BulkOp {
    /// Wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            BulkOp::Index => "index",
            BulkOp::Create => "create",
        }
    }
}

/// One `_bulk` action.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkAction {
    /// Action (default `index`).
    pub action: BulkOp,
    /// Target stream (`_index`).
    pub stream: String,
    /// The document.
    pub document: Value,
}

impl BulkAction {
    /// An `index` action.
    pub fn index(stream: impl Into<String>, document: Value) -> Self {
        BulkAction {
            action: BulkOp::Index,
            stream: stream.into(),
            document,
        }
    }
    /// A `create` action.
    pub fn create(stream: impl Into<String>, document: Value) -> Self {
        BulkAction {
            action: BulkOp::Create,
            stream: stream.into(),
            document,
        }
    }
}

/// Error of one `_bulk` item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkItemError {
    /// Error type (e.g. `timestamp_parsing_failed`).
    pub error_type: String,
    /// Reason text.
    pub reason: String,
}

/// One `_bulk` result item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BulkItem {
    /// Action key reported by the server (`index` also for `create`).
    pub action: String,
    /// Stream.
    pub stream: String,
    /// HTTP-like status of the item (200 = created).
    pub status: u16,
    /// Item error, when any.
    pub error: Option<BulkItemError>,
}

/// Result of `ingest_bulk`. Always check `errors`: item failures keep HTTP 200.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BulkResult {
    /// Server time in ms.
    pub took: u64,
    /// Whether any item failed.
    pub errors: bool,
    /// Per-item results.
    pub items: Vec<BulkItem>,
}

/// Options of `ingest_otlp`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OtlpIngestOptions {
    /// Gzip the body.
    pub gzip: bool,
    /// Target stream (header `stream-name`; default: the config's `stream`).
    pub stream: Option<String>,
}

/// Result of `ingest_otlp`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OtlpIngestResult {
    /// Records accepted.
    pub accepted: usize,
    /// Records rejected (OTLP partial success).
    pub rejected: usize,
}

/// Options of `search`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOptions {
    /// Default: `end` − 15 minutes.
    pub start: Option<Timestamp>,
    /// Default: now.
    pub end: Option<Timestamp>,
    /// Row offset (default 0).
    pub from: u64,
    /// Page size (default 100; a SQL `LIMIT` overrides it; 0 or -1 = server default).
    pub size: i64,
    /// Also fetch the exact number of matching rows (one extra request) into `total_hits`.
    pub track_total_hits: bool,
    /// Stream type of the query (default logs).
    pub stream_type: StreamType,
}

impl Default for SearchOptions {
    fn default() -> Self {
        SearchOptions {
            start: None,
            end: None,
            from: 0,
            size: 100,
            track_total_hits: false,
            stream_type: StreamType::Logs,
        }
    }
}

impl SearchOptions {
    /// Sets the time range.
    pub fn range(mut self, start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        self.start = start;
        self.end = end;
        self
    }
    /// Sets the offset and page size.
    pub fn page(mut self, from: u64, size: i64) -> Self {
        self.from = from;
        self.size = size;
        self
    }
    /// Enables the extra count query.
    pub fn track_total_hits(mut self, on: bool) -> Self {
        self.track_total_hits = on;
        self
    }
    /// Sets the stream type.
    pub fn stream_type(mut self, t: StreamType) -> Self {
        self.stream_type = t;
        self
    }
}

/// Result of `search` and `search_around`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchResult {
    /// Flat rows; each has `_timestamp` (µs).
    pub hits: Vec<JsonObject>,
    /// Rows in this page (the server's `total`).
    pub total: u64,
    /// Exact number of matching rows, only with `track_total_hits`.
    pub total_hits: Option<u64>,
    /// Offset of the page.
    pub from: u64,
    /// Page size.
    pub size: i64,
    /// Server time in ms.
    pub took: u64,
    /// Records scanned.
    pub scan_records: u64,
    /// Whether the result is partial.
    pub is_partial: bool,
    /// Bucket width in seconds (histogram queries).
    pub histogram_interval: Option<u64>,
    /// The server's search trace id.
    pub request_id: Option<String>,
}

/// Options of `search_around`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchAroundOptions {
    /// Rows around the anchor, anchor included (default 10).
    pub size: u64,
    /// Optional `SELECT …` that filters the rows.
    pub sql: Option<String>,
    /// Stream type (default logs).
    pub stream_type: StreamType,
}

impl Default for SearchAroundOptions {
    fn default() -> Self {
        SearchAroundOptions {
            size: 10,
            sql: None,
            stream_type: StreamType::Logs,
        }
    }
}

/// Options of `values_of`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValuesOptions {
    /// Default: `end` − 15 minutes.
    pub start: Option<Timestamp>,
    /// Default: now.
    pub end: Option<Timestamp>,
    /// Values per field (default 10).
    pub size: u64,
    /// Stream type (default logs).
    pub stream_type: StreamType,
}

impl Default for ValuesOptions {
    fn default() -> Self {
        ValuesOptions {
            start: None,
            end: None,
            size: 10,
            stream_type: StreamType::Logs,
        }
    }
}

/// One distinct value and how often it occurs.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldValue {
    /// The value (string, number or boolean).
    pub value: Value,
    /// Occurrences.
    pub count: u64,
}

/// Distinct values of one field, most frequent first.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldValues {
    /// Field name.
    pub field: String,
    /// Values.
    pub values: Vec<FieldValue>,
}

/// Options of `histogram`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HistogramOptions {
    /// Default: `end` − 15 minutes.
    pub start: Option<Timestamp>,
    /// Default: now.
    pub end: Option<Timestamp>,
    /// Bucket width such as `"1 minute"` or `"10 second"` (default: chosen by the server).
    pub interval: Option<String>,
    /// SQL `WHERE` condition, e.g. `level = 'error'`.
    pub filter: Option<String>,
    /// Stream type (default logs).
    pub stream_type: StreamType,
}

/// One histogram bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramBucket {
    /// Bucket start (UTC).
    pub start: SystemTime,
    /// The server's bucket key (naive UTC ISO string).
    pub key: String,
    /// Records in the bucket.
    pub count: u64,
}

/// Result of `histogram`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Histogram {
    /// Buckets in time order.
    pub buckets: Vec<HistogramBucket>,
    /// Bucket width in seconds.
    pub interval_seconds: Option<u64>,
}

/// Callback receiving tailed rows.
pub type RowCallback = Arc<dyn Fn(JsonObject) + Send + Sync>;
/// Callback receiving tail query errors.
pub type TailErrorCallback = Arc<dyn Fn(&Error) + Send + Sync>;

/// Options of `tail`.
#[derive(Clone)]
pub struct LogTailOptions {
    /// SQL `WHERE` condition, e.g. `match_all('error')`.
    pub filter: Option<String>,
    /// Only rows at or after this time (default now).
    pub start: Option<Timestamp>,
    /// Poll interval (default 1 s).
    pub interval: Duration,
    /// Re-read window that catches late rows; duplicates are removed (default 1 s).
    pub overlap: Duration,
    /// Rows per poll request (default 1000).
    pub page_size: usize,
    /// Called on query errors; tailing continues.
    pub on_error: Option<TailErrorCallback>,
    /// Stream type (default logs).
    pub stream_type: StreamType,
}

impl Default for LogTailOptions {
    fn default() -> Self {
        LogTailOptions {
            filter: None,
            start: None,
            interval: Duration::from_secs(1),
            overlap: Duration::from_secs(1),
            page_size: 1000,
            on_error: None,
            stream_type: StreamType::Logs,
        }
    }
}

impl fmt::Debug for LogTailOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogTailOptions")
            .field("filter", &self.filter)
            .field("start", &self.start)
            .field("interval", &self.interval)
            .field("overlap", &self.overlap)
            .field("page_size", &self.page_size)
            .field("on_error", &self.on_error.is_some())
            .field("stream_type", &self.stream_type)
            .finish()
    }
}

impl LogTailOptions {
    /// Sets the filter.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
    /// Sets the start.
    pub fn start(mut self, start: impl Into<Timestamp>) -> Self {
        self.start = Some(start.into());
        self
    }
    /// Sets the poll interval.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }
    /// Sets the error callback.
    pub fn on_error(mut self, f: impl Fn(&Error) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }
}

/// The OpenObserve logs module: `client.logs()`.
#[derive(Debug, Clone)]
pub struct OpenObserveLogs {
    client: OpenObserveClient,
}

impl OpenObserveLogs {
    pub(crate) fn new(client: OpenObserveClient) -> Self {
        OpenObserveLogs { client }
    }

    fn with_body<'a>(
        &self,
        req: RequestBuilder<'a>,
        body: Vec<u8>,
        content_type: &str,
        compress: bool,
    ) -> RequestBuilder<'a> {
        if compress {
            req.header("content-encoding", "gzip")
                .body(gzip(&body), content_type.to_string())
        } else {
            req.body(body, content_type.to_string())
        }
    }

    /// Writes records with `POST /api/{org}/{stream}/_json`. Give `_timestamp` in epoch µs
    /// (ms, ns and RFC 3339 are auto-detected by the server); without it the server uses its
    /// own time.
    pub async fn ingest_json(
        &self,
        stream: &str,
        records: &[Value],
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        let name = stream_name(stream, "logs.ingestJson")?;
        let body = serde_json::to_vec(records)?;
        self.ingest("_json", &name, body, "application/json", opts)
            .await
    }

    /// Writes newline-delimited JSON with `POST /api/{org}/{stream}/_multi`. One bad line
    /// rejects the whole request (`VALIDATION`).
    pub async fn ingest_multi(
        &self,
        stream: &str,
        ndjson: &str,
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        let name = stream_name(stream, "logs.ingestMulti")?;
        self.ingest(
            "_multi",
            &name,
            ndjson.as_bytes().to_vec(),
            "application/x-ndjson",
            opts,
        )
        .await
    }

    /// Like [`ingest_multi`](Self::ingest_multi) with records encoded one per line.
    pub async fn ingest_multi_records(
        &self,
        stream: &str,
        records: &[Value],
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        let mut body = String::new();
        for r in records {
            body.push_str(&r.to_string());
            body.push('\n');
        }
        self.ingest_multi(stream, &body, opts).await
    }

    async fn ingest(
        &self,
        endpoint: &str,
        stream: &str,
        body: Vec<u8>,
        content_type: &str,
        opts: IngestOptions,
    ) -> Result<IngestResult> {
        let op = format!("logs.ingest {stream}");
        let req = self
            .client
            .request(Method::POST, &self.client.org_path(&[stream, endpoint]))
            .operation(op.clone());
        let data = send_json(self.with_body(req, body, content_type, opts.gzip), &op).await?;
        let status = items(field(&data, "status"));
        let first = status
            .iter()
            .find(|s| text(field(s, "name")).as_deref() == Some(stream))
            .or_else(|| status.first())
            .unwrap_or(&Value::Null);
        Ok(IngestResult {
            stream: text(field(first, "name")).unwrap_or_else(|| stream.to_string()),
            successful: uint(field(first, "successful")),
            failed: uint(field(first, "failed")),
            error: text(field(first, "error")).map(|e| self.client.transport().redact(&e)),
        })
    }

    /// Elasticsearch-compatible bulk write: `POST /api/{org}/_bulk`.
    pub async fn ingest_bulk(
        &self,
        actions: &[BulkAction],
        opts: IngestOptions,
    ) -> Result<BulkResult> {
        if actions.is_empty() {
            return Err(Error::validation("logs.ingestBulk: no actions"));
        }
        let mut body = String::new();
        for a in actions {
            let stream = stream_name(&a.stream, "logs.ingestBulk")?;
            body.push_str(&json!({ a.action.as_str(): { "_index": stream } }).to_string());
            body.push('\n');
            body.push_str(&a.document.to_string());
            body.push('\n');
        }
        self.bulk(body, opts).await
    }

    /// Bulk write with a raw NDJSON body. `delete` and `update` actions are rejected
    /// client-side with `VALIDATION` because the server silently ignores them.
    pub async fn ingest_bulk_raw(&self, ndjson: &str, opts: IngestOptions) -> Result<BulkResult> {
        let lines: Vec<&str> = ndjson.lines().filter(|l| !l.trim().is_empty()).collect();
        if lines.is_empty() {
            return Err(Error::validation("logs.ingestBulk: no actions"));
        }
        for (i, line) in lines.iter().enumerate().step_by(2) {
            let action: Value = serde_json::from_str(line).map_err(|_| {
                Error::validation(format!("logs.ingestBulk: line {} is not JSON", i + 1))
            })?;
            let name = action
                .as_object()
                .and_then(|m| m.keys().next().cloned())
                .unwrap_or_default();
            if name != "index" && name != "create" {
                return Err(Error::validation(format!(
                    "logs.ingestBulk: unsupported action \"{name}\" on line {} (only index and create)",
                    i + 1
                )));
            }
        }
        let mut body = ndjson.to_string();
        if !body.ends_with('\n') {
            body.push('\n');
        }
        self.bulk(body, opts).await
    }

    async fn bulk(&self, body: String, opts: IngestOptions) -> Result<BulkResult> {
        let op = "logs.ingestBulk";
        let req = self
            .client
            .request(Method::POST, &self.client.org_path(&["_bulk"]))
            .operation(op);
        let data = send_json(
            self.with_body(req, body.into_bytes(), "application/x-ndjson", opts.gzip),
            op,
        )
        .await?;
        let items: Vec<BulkItem> = items(field(&data, "items"))
            .iter()
            .map(|raw| {
                let (action, item) = raw
                    .as_object()
                    .and_then(|m| m.iter().next())
                    .map_or(("index".to_string(), &Value::Null), |(k, v)| (k.clone(), v));
                let error = match field(item, "error") {
                    Value::Null => None,
                    err => Some(BulkItemError {
                        error_type: text(field(err, "type")).unwrap_or_else(|| "error".into()),
                        reason: self
                            .client
                            .transport()
                            .redact(&text(field(err, "reason")).unwrap_or_default()),
                    }),
                };
                let status = match field(item, "status") {
                    Value::Null => 200,
                    s => u16::try_from(uint(s)).unwrap_or(u16::MAX),
                };
                BulkItem {
                    action,
                    stream: text(field(item, "_index")).unwrap_or_default(),
                    status,
                    error,
                }
            })
            .collect();
        Ok(BulkResult {
            took: uint(field(&data, "took")),
            errors: field(&data, "errors") == &Value::Bool(true)
                || items.iter().any(|i| i.status >= 300),
            items,
        })
    }

    /// Writes SDK log records as OTLP/JSON (`POST /api/{org}/v1/logs`, header `stream-name`).
    /// The server stores `body`, `severity`, `severity_number`, `trace_id`, `span_id` and
    /// flattened attributes; trace flags are not stored.
    pub async fn ingest_otlp(
        &self,
        records: &[LogRecord],
        opts: OtlpIngestOptions,
    ) -> Result<OtlpIngestResult> {
        let stream = stream_name(
            opts.stream
                .as_deref()
                .unwrap_or_else(|| self.client.config().stream()),
            "logs.ingestOtlp",
        )?;
        if records.is_empty() {
            return Ok(OtlpIngestResult::default());
        }
        let body = serde_json::to_vec(&to_otlp_logs_request(records, SDK_VERSION))?;
        let op = format!("logs.ingestOtlp {stream}");
        let req = self
            .client
            .request(Method::POST, &self.client.org_path(&["v1", "logs"]))
            .header("stream-name", stream)
            .operation(op.clone());
        let data = send_json(
            self.with_body(req, body, "application/json", opts.gzip),
            &op,
        )
        .await?;
        let partial = match field(&data, "partialSuccess") {
            Value::Null => field(&data, "partial_success"),
            p => p,
        };
        let rejected = match field(partial, "rejectedLogRecords") {
            Value::Null => uint(field(partial, "rejected_log_records")),
            r => uint(r),
        };
        let rejected = usize::try_from(rejected)
            .unwrap_or(usize::MAX)
            .min(records.len());
        Ok(OtlpIngestResult {
            accepted: records.len() - rejected,
            rejected,
        })
    }

    /// Runs a SQL query: `POST /api/{org}/_search?type=logs`. Quote stream names with double
    /// quotes (`SELECT * FROM "default"`); without `ORDER BY` rows come newest first. Times are
    /// converted to microseconds (the server silently returns nothing for millisecond values).
    pub async fn search(&self, sql: &str, opts: SearchOptions) -> Result<SearchResult> {
        if sql.trim().is_empty() {
            return Err(Error::validation("logs.search: sql must not be empty"));
        }
        let (start, end) = window(opts.start, opts.end, DEFAULT_SEARCH_WINDOW, "logs.search")?;
        let mut query = json!({
            "sql": sql,
            "start_time": start,
            "end_time": end,
            "from": opts.from,
            "size": opts.size,
        });
        let mut result = self.raw_search(&query, opts.stream_type).await?;
        if opts.track_total_hits {
            // The count comes back as the only row, so it must not be skipped by `from`.
            query["from"] = json!(0);
            query["track_total_hits"] = json!(true);
            let counted = self.raw_search(&query, opts.stream_type).await?;
            let n = counted
                .hits
                .first()
                .and_then(|h| h.get("zo_sql_num"))
                .and_then(number)
                .map_or(counted.total, |f| f as u64);
            result.total_hits = Some(n);
        }
        Ok(result)
    }

    async fn raw_search(&self, query: &Value, stream_type: StreamType) -> Result<SearchResult> {
        let op = "logs.search";
        let data = send_json(
            self.client
                .request(Method::POST, &self.client.org_path(&["_search"]))
                .query("type", stream_type.as_str())
                .json(&json!({ "query": query }))
                .idempotent(true)
                .operation(op),
            op,
        )
        .await?;
        Ok(search_result(&data))
    }

    /// Rows around one timestamp: `GET /api/{org}/{stream}/_around`. The anchor is a
    /// [`SystemTime`] or the exact `_timestamp` (µs) of a row; the anchor row is included.
    pub async fn search_around(
        &self,
        stream: &str,
        timestamp: impl Into<Timestamp>,
        opts: SearchAroundOptions,
    ) -> Result<SearchResult> {
        let name = stream_name(stream, "logs.searchAround")?;
        let op = format!("logs.searchAround {name}");
        let mut req = self
            .client
            .request(Method::GET, &self.client.org_path(&[&name, "_around"]))
            .query("key", timestamp.into().as_micros().to_string())
            .query("size", opts.size.to_string())
            .query("type", opts.stream_type.as_str())
            .operation(op.clone());
        if let Some(sql) = &opts.sql {
            req = req.query(
                "sql",
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sql),
            );
        }
        Ok(search_result(&send_json(req, &op).await?))
    }

    /// Distinct values with counts: `GET /api/{org}/{stream}/_values?fields=a,b`.
    pub async fn values_of(
        &self,
        stream: &str,
        fields: &[&str],
        opts: ValuesOptions,
    ) -> Result<Vec<FieldValues>> {
        let name = stream_name(stream, "logs.valuesOf")?;
        if fields.is_empty() {
            return Err(Error::validation("logs.valuesOf: fields must not be empty"));
        }
        let (start, end) = window(opts.start, opts.end, DEFAULT_SEARCH_WINDOW, "logs.valuesOf")?;
        let op = format!("logs.valuesOf {name}");
        let data = send_json(
            self.client
                .request(Method::GET, &self.client.org_path(&[&name, "_values"]))
                .query("fields", fields.join(","))
                .query("start_time", start.to_string())
                .query("end_time", end.to_string())
                .query("size", opts.size.to_string())
                .query("type", opts.stream_type.as_str())
                .operation(op.clone()),
            &op,
        )
        .await?;
        Ok(items(field(&data, "hits"))
            .iter()
            .map(|hit| FieldValues {
                field: text(field(hit, "field")).unwrap_or_default(),
                values: items(field(hit, "values"))
                    .iter()
                    .map(|v| {
                        let key = field(v, "zo_sql_key");
                        FieldValue {
                            value: match key {
                                Value::String(_) | Value::Number(_) | Value::Bool(_) => key.clone(),
                                other => Value::String(other.to_string()),
                            },
                            count: uint(field(v, "zo_sql_num")),
                        }
                    })
                    .collect(),
            })
            .collect())
    }

    /// Record counts per time bucket (a `histogram(_timestamp, …)` query via `_search`).
    pub async fn histogram(&self, stream: &str, opts: HistogramOptions) -> Result<Histogram> {
        let name = stream_name(stream, "logs.histogram")?;
        let interval = match &opts.interval {
            Some(i) => format!(", '{}'", check_interval(i)?),
            None => String::new(),
        };
        let filter = opts
            .filter
            .as_ref()
            .map_or(String::new(), |f| format!(" WHERE {f}"));
        let sql = format!(
            "SELECT histogram(_timestamp{interval}) AS zo_sql_key, count(*) AS zo_sql_num FROM {}{filter} GROUP BY zo_sql_key ORDER BY zo_sql_key",
            sql_ident(&name)
        );
        let result = self
            .search(
                &sql,
                SearchOptions {
                    start: opts.start,
                    end: opts.end,
                    size: -1,
                    stream_type: opts.stream_type,
                    ..SearchOptions::default()
                },
            )
            .await?;
        Ok(Histogram {
            buckets: result
                .hits
                .iter()
                .map(|hit| {
                    let key = hit.get("zo_sql_key").and_then(text).unwrap_or_default();
                    HistogramBucket {
                        start: parse_naive_utc(&key),
                        count: hit.get("zo_sql_num").map_or(0, uint),
                        key,
                    }
                })
                .collect(),
            interval_seconds: result.histogram_interval,
        })
    }

    /// Follows a stream by polling `_search` with a `_timestamp` cursor (OpenObserve has no
    /// live tail). Every poll re-reads `overlap` before the cursor and drops rows it already
    /// delivered (by full-row equality), so rows that are exact duplicates within that window
    /// are delivered once. Must be called inside a tokio runtime; stop with
    /// [`TailHandle::stop`].
    pub fn tail(
        &self,
        stream: &str,
        on_row: RowCallback,
        opts: LogTailOptions,
    ) -> Result<TailHandle> {
        let name = stream_name(stream, "logs.tail")?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| Error::config("logs.tail must be called inside a tokio runtime"))?;
        let handle = TailHandle::new();
        let h = handle.clone();
        let logs = self.clone();
        tokio::spawn(async move {
            let overlap = i64::try_from(opts.overlap.as_micros()).unwrap_or(i64::MAX / 4);
            let page_size = opts.page_size.max(1);
            let mut cursor = opts.start.map_or_else(
                || system_time_to_micros(SystemTime::now()),
                |s| s.as_micros(),
            );
            let mut seen: HashMap<String, i64> = HashMap::new();
            loop {
                tokio::select! {
                    _ = h.stopped() => return,
                    _ = tokio::time::sleep(opts.interval) => {}
                }
                let polled = logs
                    .poll_tail(
                        &name,
                        &opts,
                        &on_row,
                        &h,
                        &mut cursor,
                        &mut seen,
                        overlap,
                        page_size,
                    )
                    .await;
                if let Err(e) = polled {
                    if h.is_stopped() {
                        return;
                    }
                    if let Some(cb) = &opts.on_error {
                        cb(&e);
                    }
                }
            }
        });
        Ok(handle)
    }

    #[allow(clippy::too_many_arguments)]
    async fn poll_tail(
        &self,
        name: &str,
        opts: &LogTailOptions,
        on_row: &RowCallback,
        handle: &TailHandle,
        cursor: &mut i64,
        seen: &mut HashMap<String, i64>,
        overlap: i64,
        page_size: usize,
    ) -> Result<()> {
        loop {
            let from = *cursor - overlap;
            let filter = opts
                .filter
                .as_ref()
                .map_or(String::new(), |f| format!(" AND ({f})"));
            let sql = format!(
                "SELECT * FROM {} WHERE _timestamp >= {from}{filter} ORDER BY _timestamp ASC",
                sql_ident(name)
            );
            let end = system_time_to_micros(SystemTime::now()) + 60_000_000;
            let page = self
                .search(
                    &sql,
                    SearchOptions {
                        start: Some(Timestamp::Micros(from)),
                        end: Some(Timestamp::Micros(end)),
                        size: i64::try_from(page_size).unwrap_or(i64::MAX),
                        stream_type: opts.stream_type,
                        ..SearchOptions::default()
                    },
                )
                .await?;
            let mut fresh = 0usize;
            for row in &page.hits {
                if handle.is_stopped() {
                    return Ok(());
                }
                let key = Value::Object(row.clone()).to_string();
                if seen.contains_key(&key) {
                    continue;
                }
                let ts = row.get("_timestamp").map_or(0, int);
                seen.insert(key, ts);
                fresh += 1;
                if ts > *cursor {
                    *cursor = ts;
                }
                on_row(row.clone());
            }
            let limit = *cursor - overlap;
            seen.retain(|_, ts| *ts >= limit);
            if page.hits.len() < page_size || fresh == 0 {
                return Ok(());
            }
        }
    }
}

fn search_result(data: &Value) -> SearchResult {
    SearchResult {
        hits: items(field(data, "hits"))
            .iter()
            .map(|h| h.as_object().cloned().unwrap_or_default())
            .collect(),
        total: uint(field(data, "total")),
        total_hits: None,
        from: uint(field(data, "from")),
        size: int(field(data, "size")),
        took: uint(field(data, "took")),
        scan_records: uint(field(data, "scan_records")),
        is_partial: field(data, "is_partial") == &Value::Bool(true),
        histogram_interval: field(data, "histogram_interval").as_u64(),
        request_id: text(field(data, "trace_id")),
    }
}

fn check_interval(interval: &str) -> Result<String> {
    let t = interval.trim();
    let digits = t.chars().take_while(char::is_ascii_digit).count();
    let unit = t[digits..].trim();
    if digits == 0 || unit.is_empty() || !unit.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(Error::validation(
            "logs.histogram: interval must look like \"1 minute\"",
        ));
    }
    Ok(t.to_string())
}

fn parse_naive_utc(key: &str) -> SystemTime {
    humantime::parse_rfc3339_weak(key.trim_end_matches(['Z', 'z'])).unwrap_or(UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intervals_and_keys() {
        assert_eq!(check_interval(" 10 second ").unwrap(), "10 second");
        assert!(check_interval("minute").is_err());
        assert!(check_interval("1").is_err());
        assert!(check_interval("1 min'; DROP").is_err());
        assert_eq!(
            parse_naive_utc("1970-01-01T00:01:00"),
            UNIX_EPOCH + Duration::from_secs(60)
        );
        assert_eq!(
            parse_naive_utc("1970-01-01T00:01:00Z"),
            UNIX_EPOCH + Duration::from_secs(60)
        );
        assert_eq!(parse_naive_utc("nope"), UNIX_EPOCH);
        assert_eq!(BulkOp::Create.as_str(), "create");
        assert!(format!("{:?}", LogTailOptions::default().on_error(|_| {})).contains("on_error"));
    }
}
