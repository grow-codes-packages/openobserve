// Service-owned. TraceQueryClient for OpenObserve (API.md §5, DECISIONS 4).

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::core::{Error, ErrorCode, Method, Result};
use crate::http::{
    field, flatten_key, items, ns_to_micros, scalar_text, send_json, sql_ident, sql_string,
    stream_name, text, uint, window, JsonObject, Timestamp,
};
use crate::tracing::{
    normalize_trace_id, DependencyLink, DependencyOptions, Span, SpanEvent, SpanKind, SpanLink,
    SpanStatus, StatusCode, Trace, TraceQueryClient, TraceSearchQuery, TraceSummary,
};
use crate::OpenObserveClient;

/// Default window of `search_traces`: the last hour.
pub const DEFAULT_TRACE_SEARCH_WINDOW: Duration = Duration::from_secs(60 * 60);
/// Default window of `get_trace`: the last 7 days.
pub const DEFAULT_GET_TRACE_WINDOW: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Default window of `list_services` / `list_operations`: the last 24 hours.
pub const DEFAULT_SERVICES_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// Time window of a trace query (`None` = the method's default window ending now).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceWindowOptions {
    /// Window start.
    pub start: Option<Timestamp>,
    /// Window end (default now).
    pub end: Option<Timestamp>,
}

impl TraceWindowOptions {
    /// A window.
    pub fn new(start: Option<Timestamp>, end: Option<Timestamp>) -> Self {
        TraceWindowOptions { start, end }
    }
}

/// Result of [`OpenObserveTraceQuery::trace_time_range`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceTimeRange {
    /// Whether the server's trace index knows the trace.
    pub found: bool,
    /// Stream that holds the trace.
    pub stream: Option<String>,
    /// First span start (µs).
    pub start_micros: Option<u64>,
    /// Last span end (µs).
    pub end_micros: Option<u64>,
}

/// Span row columns that are mapped to span fields or are internal to OpenObserve.
const SPAN_COLUMNS: &[&str] = &[
    "_timestamp",
    "_o2_ingest_ts",
    "trace_id",
    "span_id",
    "reference_parent_span_id",
    "reference_parent_trace_id",
    "reference_ref_type",
    "operation_name",
    "span_kind",
    "service_name",
    "start_time",
    "end_time",
    "duration",
    "span_status",
    "status_code",
    "status_message",
    "flags",
    "events",
    "links",
];

fn attr_value(value: &Value) -> Value {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => value.clone(),
        other => Value::String(other.to_string()),
    }
}

fn attributes_of(source: &Value, skip: impl Fn(&str) -> bool) -> BTreeMap<String, Value> {
    source
        .as_object()
        .map(|m| {
            m.iter()
                .filter(|(k, v)| !skip(k) && !v.is_null())
                .map(|(k, v)| (k.clone(), attr_value(v)))
                .collect()
        })
        .unwrap_or_default()
}

fn json_list(value: &Value) -> Vec<Value> {
    match value {
        Value::String(s) => serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default(),
        other => items(other).to_vec(),
    }
}

fn lower(value: &Value) -> String {
    text(value).unwrap_or_default().to_ascii_lowercase()
}

/// Maps an OpenObserve span row to the normalized [`Span`]: `start_time`/`end_time` are
/// nanoseconds, `span_kind` is a string number, `events` and `links` are JSON strings.
/// Internal columns (`_timestamp`, `infer_service_*`, `o2_db_*`, …) are dropped; the rest
/// become attributes with the server's flattened names.
pub fn row_to_span(row: &JsonObject) -> Span {
    let row = Value::Object(row.clone());
    let start = ns_to_micros(field(&row, "start_time"));
    let end = ns_to_micros(field(&row, "end_time"));
    let status_text = text(field(&row, "span_status"))
        .unwrap_or_default()
        .to_ascii_uppercase();
    let status_code = uint(field(&row, "status_code"));
    let code = if status_text == "ERROR" || status_code == 2 {
        StatusCode::Error
    } else if status_text == "OK" || status_code == 1 {
        StatusCode::Ok
    } else {
        StatusCode::Unset
    };
    let kind = match scalar_text(field(&row, "span_kind")).as_deref() {
        Some("0") | None => SpanKind::Unspecified,
        Some(k) => SpanKind::parse(k),
    };
    Span {
        trace_id: lower(field(&row, "trace_id")),
        span_id: lower(field(&row, "span_id")),
        parent_span_id: text(field(&row, "reference_parent_span_id"))
            .map(|p| p.to_ascii_lowercase()),
        name: text(field(&row, "operation_name")).unwrap_or_default(),
        kind,
        service_name: text(field(&row, "service_name")).unwrap_or_default(),
        start_micros: start,
        end_micros: end,
        duration_micros: if end >= start {
            end - start
        } else {
            uint(field(&row, "duration"))
        },
        status: SpanStatus {
            code,
            message: text(field(&row, "status_message")),
        },
        attributes: attributes_of(&row, |k| {
            SPAN_COLUMNS.contains(&k) || k.starts_with("infer_service_") || k.starts_with("o2_db_")
        }),
        events: json_list(field(&row, "events"))
            .iter()
            .map(|e| SpanEvent {
                name: text(field(e, "name")).unwrap_or_default(),
                time_micros: ns_to_micros(field(e, "_timestamp")),
                attributes: attributes_of(e, |k| k == "name" || k == "_timestamp"),
            })
            .collect(),
        links: json_list(field(&row, "links"))
            .iter()
            .map(|l| {
                let cx = field(l, "context");
                let id = |a: &str, b: &str| match field(cx, a) {
                    Value::Null => lower(field(cx, b)),
                    v => lower(v),
                };
                SpanLink {
                    trace_id: id("traceId", "trace_id"),
                    span_id: id("spanId", "span_id"),
                    attributes: attributes_of(l, |k| {
                        k == "context"
                            || k == "droppedAttributesCount"
                            || k == "dropped_attributes_count"
                    }),
                }
            })
            .collect(),
    }
}

/// Maps one `traces/latest` hit to a [`TraceSummary`].
pub fn hit_to_summary(hit: &Value) -> TraceSummary {
    let first = field(hit, "first_event");
    TraceSummary {
        trace_id: lower(field(hit, "trace_id")),
        root_service_name: text(field(first, "service_name")),
        root_name: text(field(first, "operation_name")),
        start_micros: ns_to_micros(field(hit, "start_time")),
        duration_micros: uint(field(hit, "duration")),
        span_count: usize::try_from(items(field(hit, "spans")).first().map_or(0, uint))
            .unwrap_or(usize::MAX),
    }
}

/// Builds the `traces/latest` filter (a SQL `WHERE` fragment) for a search query.
pub fn trace_filter(query: &TraceSearchQuery) -> Result<String> {
    let mut parts = Vec::new();
    if let Some(s) = &query.service {
        parts.push(format!("service_name = {}", sql_string(s)));
    }
    if let Some(o) = &query.operation {
        parts.push(format!("operation_name = {}", sql_string(o)));
    }
    for (key, value) in &query.tags {
        let column = flatten_key(key);
        if column.is_empty() {
            return Err(Error::validation("tracing.searchTraces: empty tag key"));
        }
        parts.push(format!("{column} = {}", sql_string(value)));
    }
    if let Some(d) = query.min_duration {
        parts.push(format!("duration >= {}", d.as_micros()));
    }
    if let Some(d) = query.max_duration {
        parts.push(format!("duration <= {}", d.as_micros()));
    }
    Ok(parts.join(" AND "))
}

/// [`TraceQueryClient`] on one traces stream (default: the config's `traces_stream`).
///
/// `search_traces` uses `traces/latest`; with a filter each summary covers only the matching
/// spans (start, duration and span count), which keeps the call cheap. `get_dependencies`
/// returns `UNSUPPORTED`: the service graph is an enterprise feature (404 in the OSS server).
#[derive(Debug, Clone)]
pub struct OpenObserveTraceQuery {
    client: OpenObserveClient,
    stream: String,
}

fn not_found_to<T>(r: Result<T>, fallback: T) -> Result<T> {
    match r {
        Err(e) if e.code() == ErrorCode::NotFound => Ok(fallback),
        other => other,
    }
}

impl OpenObserveTraceQuery {
    pub(crate) fn new(client: OpenObserveClient, stream: Option<&str>) -> Self {
        let configured = client.config().traces_stream().to_string();
        let stream = stream
            .and_then(|s| stream_name(s, "tracing.query").ok())
            .unwrap_or(configured);
        OpenObserveTraceQuery { client, stream }
    }

    /// The traces stream.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// All spans of a trace (`traces/{id}/details`, paginated), or `None` when the trace (or
    /// the stream) does not exist in the window (default: the last 7 days).
    pub async fn get_trace_in(
        &self,
        trace_id: &str,
        opts: TraceWindowOptions,
    ) -> Result<Option<Trace>> {
        let id = normalize_trace_id(trace_id)?;
        let (start, end) = window(
            opts.start,
            opts.end,
            DEFAULT_GET_TRACE_WINDOW,
            "tracing.getTrace",
        )?;
        let op = format!("tracing.getTrace {id}");
        let mut spans: BTreeMap<String, Span> = BTreeMap::new();
        let mut after: Option<(String, String)> = None;
        for _ in 0..1000 {
            let mut req = self
                .client
                .request(
                    Method::GET,
                    &self
                        .client
                        .org_path(&[&self.stream, "traces", &id, "details"]),
                )
                .query("start_time", start.to_string())
                .query("end_time", end.to_string())
                .query("size", "1000")
                .operation(op.clone());
            if let Some((s, span)) = &after {
                req = req
                    .query("after_start_time", s.clone())
                    .query("after_span_id", span.clone());
            }
            let data = match send_json(req, &op).await {
                Err(e) if e.code() == ErrorCode::NotFound => return Ok(None),
                other => other?,
            };
            for hit in items(field(&data, "hits")) {
                if let Some(row) = hit.as_object() {
                    let span = row_to_span(row);
                    spans.insert(span.span_id.clone(), span);
                }
            }
            let next = field(&data, "next_after");
            match (
                field(&data, "has_more"),
                scalar_text(field(next, "start_time")),
                text(field(next, "span_id")),
            ) {
                (Value::Bool(true), Some(s), Some(span)) => after = Some((s, span)),
                _ => break,
            }
        }
        if spans.is_empty() {
            return Ok(None);
        }
        Ok(Some(Trace::new(id, spans.into_values().collect())))
    }

    /// Distinct `service_name` values in the window (default: the last 24 hours), sorted.
    pub async fn list_services_in(&self, opts: TraceWindowOptions) -> Result<Vec<String>> {
        self.distinct("service_name", "", "tracing.listServices", opts)
            .await
    }

    /// Distinct operation names of one service in the window, sorted.
    pub async fn list_operations_in(
        &self,
        service: &str,
        opts: TraceWindowOptions,
    ) -> Result<Vec<String>> {
        self.distinct(
            "operation_name",
            &format!(" WHERE service_name = {}", sql_string(service)),
            "tracing.listOperations",
            opts,
        )
        .await
    }

    async fn distinct(
        &self,
        column: &str,
        filter: &str,
        op: &str,
        opts: TraceWindowOptions,
    ) -> Result<Vec<String>> {
        let (start, end) = window(opts.start, opts.end, DEFAULT_SERVICES_WINDOW, op)?;
        let sql = format!(
            "SELECT DISTINCT {column} FROM {}{filter} ORDER BY {column}",
            sql_ident(&self.stream)
        );
        let data = send_json(
            self.client
                .request(Method::POST, &self.client.org_path(&["_search"]))
                .query("type", "traces")
                .json(&json!({ "query": {
                    "sql": sql, "start_time": start, "end_time": end, "from": 0, "size": 10_000,
                }}))
                .idempotent(true)
                .operation(op),
            op,
        )
        .await;
        let data = not_found_to(data, Value::Null)?;
        let values: BTreeSet<String> = items(field(&data, "hits"))
            .iter()
            .filter_map(|h| text(field(h, column)))
            .collect();
        Ok(values.into_iter().collect())
    }

    /// Where and when a trace was stored (`traces/time_range`, uses the server's trace index).
    pub async fn trace_time_range(&self, trace_id: &str) -> Result<TraceTimeRange> {
        let id = normalize_trace_id(trace_id)?;
        let op = format!("tracing.traceTimeRange {id}");
        let data = send_json(
            self.client
                .request(
                    Method::GET,
                    &self.client.org_path(&["traces", "time_range"]),
                )
                .query("trace_id", id.clone())
                .operation(op.clone()),
            &op,
        )
        .await?;
        let found = items(field(&data, "results"))
            .iter()
            .find(|r| text(field(r, "status")).as_deref() == Some("found"));
        Ok(match found {
            None => TraceTimeRange::default(),
            Some(r) => {
                let range = field(r, "range");
                TraceTimeRange {
                    found: true,
                    stream: text(field(r, "stream")),
                    start_micros: Some(uint(field(range, "start_time"))),
                    end_micros: Some(uint(field(range, "end_time"))),
                }
            }
        })
    }
}

#[async_trait]
impl TraceQueryClient for OpenObserveTraceQuery {
    /// All spans of a trace in the last 7 days; `None` when unknown.
    async fn get_trace(&self, trace_id: &str) -> Result<Option<Trace>> {
        self.get_trace_in(trace_id, TraceWindowOptions::default())
            .await
    }

    /// Recent traces matching the query (`traces/latest`); default window: the last hour.
    async fn search_traces(&self, query: &TraceSearchQuery) -> Result<Vec<TraceSummary>> {
        if query.limit == 0 {
            return Err(Error::validation(
                "tracing.searchTraces: limit must be positive",
            ));
        }
        let (start, end) = window(
            query.start.map(Timestamp::from),
            query.end.map(Timestamp::from),
            DEFAULT_TRACE_SEARCH_WINDOW,
            "tracing.searchTraces",
        )?;
        let filter = trace_filter(query)?;
        let op = "tracing.searchTraces";
        let mut req = self
            .client
            .request(
                Method::GET,
                &self.client.org_path(&[&self.stream, "traces", "latest"]),
            )
            .query("start_time", start.to_string())
            .query("end_time", end.to_string())
            .query("from", "0")
            .query("size", query.limit.to_string())
            .operation(op);
        if !filter.is_empty() {
            req = req.query("filter", filter);
        }
        let data = not_found_to(send_json(req, op).await, Value::Null)?;
        Ok(items(field(&data, "hits"))
            .iter()
            .take(query.limit)
            .map(hit_to_summary)
            .collect())
    }

    /// Distinct services in the last 24 hours.
    async fn list_services(&self) -> Result<Vec<String>> {
        self.list_services_in(TraceWindowOptions::default()).await
    }

    /// Distinct operations of a service in the last 24 hours.
    async fn list_operations(&self, service: &str) -> Result<Vec<String>> {
        self.list_operations_in(service, TraceWindowOptions::default())
            .await
    }

    /// `UNSUPPORTED`: the service graph is an OpenObserve enterprise feature.
    async fn get_dependencies(&self, opts: DependencyOptions) -> Result<Vec<DependencyLink>> {
        let _ = opts;
        Err(Error::unsupported(
            "traces.getDependencies: the service graph is an OpenObserve enterprise feature",
        ))
    }
}
