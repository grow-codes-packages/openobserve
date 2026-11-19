// Service-owned. Mapping between SDK LogRecords and OpenObserve rows / OTLP JSON.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::http::{
    flatten_key, int, micros_to_system_time, number, system_time_to_micros, text, JsonObject,
};
use crate::logging::{AttrValue, Attributes, LogRecord, Severity};

/// Row columns that [`row_to_log_record`] maps to `LogRecord` fields instead of attributes.
const RECORD_COLUMNS: &[&str] = &[
    "_timestamp",
    "_o2_ingest_ts",
    "body",
    "message",
    "severity",
    "level",
    "severity_number",
    "severity_text",
    "trace_id",
    "span_id",
    "trace_flags",
    "flags",
    "observed_timestamp",
];

/// Flattened resource columns → resource attribute names.
const RESOURCE_COLUMNS: &[(&str, &str)] = &[
    ("service_name", "service.name"),
    ("service_version", "service.version"),
    ("service_namespace", "service.namespace"),
    ("service_instance_id", "service.instance.id"),
    ("deployment_environment", "deployment.environment"),
    ("deployment_environment_name", "deployment.environment.name"),
    ("host_name", "host.name"),
];

fn column_value(value: &AttrValue) -> AttrValue {
    match value {
        Value::Array(_) => Value::String(value.to_string()),
        other => other.clone(),
    }
}

/// Converts a [`LogRecord`] into the row written by `_json` ingestion: flattened attributes and
/// resource keys plus `_timestamp` (µs), `body`, `severity`, `severity_number`, `trace_id`,
/// `span_id`, `trace_flags`. Record fields win over attributes with the same column name.
pub fn log_record_to_row(record: &LogRecord) -> JsonObject {
    let mut row = JsonObject::new();
    for (k, v) in record.resource.iter().chain(record.attributes.iter()) {
        row.insert(flatten_key(k), column_value(v));
    }
    row.insert(
        "_timestamp".into(),
        json!(system_time_to_micros(record.timestamp)),
    );
    row.insert("body".into(), json!(record.body));
    row.insert("severity".into(), json!(record.severity_text));
    row.insert("severity_number".into(), json!(record.severity.number()));
    if let Some(t) = &record.trace_id {
        row.insert("trace_id".into(), json!(t));
    }
    if let Some(s) = &record.span_id {
        row.insert("span_id".into(), json!(s));
    }
    if let Some(f) = record.trace_flags {
        row.insert("trace_flags".into(), json!(f));
    }
    row
}

fn attr_value(value: &Value) -> AttrValue {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => value.clone(),
        other => Value::String(other.to_string()),
    }
}

fn body_of(row: &JsonObject) -> String {
    for key in ["body", "message", "log", "msg"] {
        match row.get(key) {
            Some(Value::String(s)) => return s.clone(),
            Some(Value::Null) | None => {}
            Some(other) => return other.to_string(),
        }
    }
    String::new()
}

/// Normalizes an OpenObserve row (from `_search`) into a [`LogRecord`]: `_timestamp` (µs),
/// `body`|`message`, `severity`|`level` (+ `severity_number`), `trace_id`, `span_id`,
/// `trace_flags`; well-known resource columns (`service_name`, `host_name`, …) go to
/// `resource`, every other column to `attributes` (flattened names as stored by the server).
pub fn row_to_log_record(row: &JsonObject) -> LogRecord {
    let get = |k: &str| row.get(k).unwrap_or(&Value::Null);
    let severity_text = text(get("severity"))
        .or_else(|| text(get("severity_text")))
        .or_else(|| text(get("level")));
    let severity = number(get("severity_number"))
        .filter(|n| n.fract() == 0.0 && (0.0..=255.0).contains(n))
        .map(|n| Severity::from_number(n as u8))
        .or_else(|| severity_text.as_deref().and_then(|t| t.parse().ok()))
        .unwrap_or(Severity::Info);
    let mut attributes = Attributes::new();
    let mut resource = Attributes::new();
    for (k, v) in row {
        if RECORD_COLUMNS.contains(&k.as_str()) || v.is_null() {
            continue;
        }
        match RESOURCE_COLUMNS.iter().find(|(col, _)| col == k) {
            Some((_, name)) => resource.insert((*name).to_string(), attr_value(v)),
            None => attributes.insert(k.clone(), attr_value(v)),
        };
    }
    let flags = number(get("trace_flags"))
        .filter(|n| n.fract() == 0.0 && (0.0..=255.0).contains(n))
        .map(|n| n as u8);
    LogRecord {
        timestamp: micros_to_system_time(int(get("_timestamp"))),
        observed_timestamp: None,
        severity,
        severity_text: severity_text.unwrap_or_else(|| severity.text().to_string()),
        body: body_of(row),
        attributes,
        resource,
        trace_id: text(get("trace_id")).map(|s| s.to_ascii_lowercase()),
        span_id: text(get("span_id")).map(|s| s.to_ascii_lowercase()),
        trace_flags: flags,
    }
}

/// Converts an attribute value into an OTLP/JSON `AnyValue`.
pub fn otlp_value(value: &AttrValue) -> Value {
    match value {
        Value::String(s) => json!({ "stringValue": s }),
        Value::Bool(b) => json!({ "boolValue": b }),
        Value::Number(n) => match n.as_i64() {
            Some(i) => json!({ "intValue": i.to_string() }),
            None => json!({ "doubleValue": n.as_f64() }),
        },
        Value::Array(items) => {
            json!({ "arrayValue": { "values": items.iter().map(otlp_value).collect::<Vec<_>>() } })
        }
        Value::Object(map) => json!({ "kvlistValue": { "values": key_values(map.iter()) } }),
        Value::Null => json!({}),
    }
}

fn key_values<'a>(attrs: impl Iterator<Item = (&'a String, &'a Value)>) -> Vec<Value> {
    attrs
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| json!({ "key": k, "value": otlp_value(v) }))
        .collect()
}

/// Converts an attribute map into OTLP/JSON `KeyValue`s (`null` values are dropped).
pub fn otlp_attributes(attributes: &Attributes) -> Vec<Value> {
    key_values(attributes.iter())
}

fn nanos(t: SystemTime) -> String {
    t.duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos())
        .to_string()
}

/// Builds an OTLP/JSON `ExportLogsServiceRequest` from records (grouped by resource), with the
/// instrumentation scope `grow-codes-openobserve`.
pub fn to_otlp_logs_request(records: &[LogRecord], scope_version: &str) -> Value {
    let mut groups: Vec<(String, &Attributes, Vec<Value>)> = Vec::new();
    for record in records {
        let key = serde_json::to_string(&record.resource).unwrap_or_default();
        let index = match groups.iter().position(|(k, _, _)| *k == key) {
            Some(i) => i,
            None => {
                groups.push((key, &record.resource, Vec::new()));
                groups.len() - 1
            }
        };
        let mut log = json!({
            "timeUnixNano": nanos(record.timestamp),
            "severityNumber": record.severity.number(),
            "severityText": record.severity_text,
            "body": { "stringValue": record.body },
            "attributes": otlp_attributes(&record.attributes),
        });
        if let Some(t) = record.observed_timestamp {
            log["observedTimeUnixNano"] = json!(nanos(t));
        }
        if let Some(t) = &record.trace_id {
            log["traceId"] = json!(t);
        }
        if let Some(s) = &record.span_id {
            log["spanId"] = json!(s);
        }
        if let Some(f) = record.trace_flags {
            log["flags"] = json!(f);
        }
        groups[index].2.push(log);
    }
    json!({
        "resourceLogs": groups.into_iter().map(|(_, resource, logs)| json!({
            "resource": { "attributes": otlp_attributes(resource) },
            "scopeLogs": [{
                "scope": { "name": "grow-codes-openobserve", "version": scope_version },
                "logRecords": logs,
            }],
        })).collect::<Vec<_>>(),
    })
}
