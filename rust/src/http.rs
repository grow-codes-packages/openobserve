// Service-owned. Shared helpers of the OpenObserve service modules: JSON decoding, error
// remapping of vendor codes, time-unit conversion and SQL quoting.
//! Helpers shared by the OpenObserve modules: time units, SQL quoting and JSON access.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde_json::Value;

use crate::config::normalize_stream_name;
use crate::core::transport::RequestBuilder;
use crate::core::{Error, ErrorCode, Response, Result};

/// A JSON object (one search row, one stream description, …).
pub type JsonObject = serde_json::Map<String, Value>;

/// Characters `encodeURIComponent` leaves alone.
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'!')
    .remove(b'~')
    .remove(b'*')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')');

/// Stream types of OpenObserve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum StreamType {
    /// Log streams (default).
    #[default]
    Logs,
    /// Trace streams.
    Traces,
    /// Metric streams.
    Metrics,
}

impl StreamType {
    /// Wire value: `logs`, `traces` or `metrics`.
    pub fn as_str(self) -> &'static str {
        match self {
            StreamType::Logs => "logs",
            StreamType::Traces => "traces",
            StreamType::Metrics => "metrics",
        }
    }
}

impl std::fmt::Display for StreamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A point in time: a [`SystemTime`] or epoch **microseconds** (the exact `_timestamp` of a row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// A wall-clock time.
    Time(SystemTime),
    /// Epoch microseconds.
    Micros(i64),
}

impl Timestamp {
    /// Epoch microseconds (negative before 1970).
    pub fn as_micros(self) -> i64 {
        match self {
            Timestamp::Micros(m) => m,
            Timestamp::Time(t) => system_time_to_micros(t),
        }
    }
}

impl From<SystemTime> for Timestamp {
    fn from(t: SystemTime) -> Self {
        Timestamp::Time(t)
    }
}

impl From<i64> for Timestamp {
    fn from(m: i64) -> Self {
        Timestamp::Micros(m)
    }
}

impl From<u64> for Timestamp {
    fn from(m: u64) -> Self {
        Timestamp::Micros(i64::try_from(m).unwrap_or(i64::MAX))
    }
}

/// Converts a [`SystemTime`] to epoch microseconds.
pub fn system_time_to_micros(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_micros()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_micros()).unwrap_or(i64::MAX),
    }
}

/// Converts epoch microseconds to a [`SystemTime`].
pub fn micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        UNIX_EPOCH - Duration::from_micros(micros.unsigned_abs())
    }
}

/// Converts a nanosecond value (number or exact decimal string) to microseconds; `0` for
/// anything unparsable.
pub fn ns_to_micros(value: &Value) -> u64 {
    match value {
        Value::Number(n) => match n.as_u64() {
            Some(u) => u / 1000,
            None => n
                .as_f64()
                .filter(|f| f.is_finite() && *f > 0.0)
                .map_or(0, |f| (f / 1000.0).floor() as u64),
        },
        Value::String(s) => s
            .trim()
            .parse::<u128>()
            .map_or(0, |ns| u64::try_from(ns / 1000).unwrap_or(u64::MAX)),
        _ => 0,
    }
}

/// SQL string literal with single quotes doubled: `'it''s'`.
pub fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// SQL identifier in double quotes (quotes doubled), e.g. `"default"`.
pub fn sql_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Flattens an attribute key like the server does (`http.method` → `http_method`).
pub fn flatten_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `/api/{org}/…` with every segment URL-encoded.
pub(crate) fn org_path(org: &str, segments: &[&str]) -> String {
    let mut out = String::from("/api/");
    out.push_str(&encode(org));
    for s in segments {
        out.push('/');
        out.push_str(&encode(s));
    }
    out
}

/// Percent-encodes one path segment (`encodeURIComponent` rules).
pub(crate) fn encode(segment: &str) -> String {
    utf8_percent_encode(segment, SEGMENT).to_string()
}

/// Normalizes and validates a stream name (`VALIDATION` when blank).
pub(crate) fn stream_name(name: &str, operation: &str) -> Result<String> {
    let normalized = normalize_stream_name(name);
    if normalized.is_empty() {
        return Err(Error::validation(format!(
            "{operation}: stream name must not be blank"
        )));
    }
    Ok(normalized)
}

/// Remaps a transport error: vendor code 20002 / "stream not found" (HTTP 400) → `NOT_FOUND`,
/// "already exists" (HTTP 400) → `CONFLICT`.
pub(crate) fn remap_error(err: Error) -> Error {
    if err.http_status() != Some(400) {
        return err;
    }
    let lower = err.message().to_ascii_lowercase();
    let code = if lower.contains("stream not found") {
        ErrorCode::NotFound
    } else if lower.contains("already exists") {
        ErrorCode::Conflict
    } else {
        return err;
    };
    let mut out = Error::new(code, err.message().to_string()).with_status(400);
    if let Some(id) = err.request_id() {
        out = out.with_request_id(id.to_string());
    }
    out.with_source(err)
}

/// Sends a request, remaps errors and decodes the JSON body (`Null` for an empty body).
pub(crate) async fn send_json(req: RequestBuilder<'_>, operation: &str) -> Result<Value> {
    let resp = req.send().await.map_err(remap_error)?;
    decode(&resp, operation)
}

/// Decodes a response body as JSON (`Null` when empty).
pub(crate) fn decode(resp: &Response, operation: &str) -> Result<Value> {
    let body = resp.bytes();
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body).map_err(|_| {
        Error::validation(format!(
            "{operation}: invalid JSON in response (HTTP {})",
            resp.status()
        ))
        .with_status(resp.status())
    })
}

/// Default search window: `[end - default - 60 s, now + 60 s]` in microseconds.
pub(crate) fn window(
    start: Option<Timestamp>,
    end: Option<Timestamp>,
    default: Duration,
    operation: &str,
) -> Result<(i64, i64)> {
    let end = match end {
        Some(e) => e.as_micros(),
        None => system_time_to_micros(SystemTime::now()) + 60_000_000,
    };
    let start = match start {
        Some(s) => s.as_micros(),
        None => end - i64::try_from(default.as_micros()).unwrap_or(i64::MAX / 2) - 60_000_000,
    };
    if start > end {
        return Err(Error::validation(format!(
            "{operation}: start is after end"
        )));
    }
    Ok((start, end))
}

/// A field of a JSON object (`Null` when absent or not an object).
pub(crate) fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value.get(key).unwrap_or(&Value::Null)
}

/// A non-empty string.
pub(crate) fn text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// A string or number as text.
pub(crate) fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// A number from a JSON number or numeric string; `None` otherwise.
pub(crate) fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok().filter(|f| f.is_finite()),
        _ => None,
    }
}

/// An unsigned integer (negative and missing values → 0).
pub(crate) fn uint(value: &Value) -> u64 {
    match value {
        Value::Number(n) => n
            .as_u64()
            .unwrap_or_else(|| n.as_f64().filter(|f| *f > 0.0).map_or(0, |f| f as u64)),
        other => number(other).filter(|f| *f > 0.0).map_or(0, |f| f as u64),
    }
}

/// A signed integer (missing values → 0).
pub(crate) fn int(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n
            .as_i64()
            .unwrap_or_else(|| n.as_f64().map_or(0, |f| f as i64)),
        other => number(other).map_or(0, |f| f as i64),
    }
}

/// The items of a JSON array (empty for anything else).
pub(crate) fn items(value: &Value) -> &[Value] {
    value.as_array().map_or(&[], Vec::as_slice)
}

/// Gzips a payload.
pub(crate) fn gzip(body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    // Writing to a Vec cannot fail.
    let _ = enc.write_all(body);
    enc.finish().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn helpers() {
        assert_eq!(ns_to_micros(&json!(1_500_000)), 1500);
        assert_eq!(
            ns_to_micros(&json!("1763496183123456789")),
            1_763_496_183_123_456
        );
        assert_eq!(ns_to_micros(&json!(2500.0)), 2);
        assert_eq!(ns_to_micros(&json!(-1.0)), 0);
        assert_eq!(ns_to_micros(&json!(null)), 0);
        assert_eq!(ns_to_micros(&json!("x")), 0);
        assert_eq!(sql_string("it's"), "'it''s'");
        assert_eq!(sql_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(flatten_key("HTTP.Method-x"), "http_method_x");
        assert_eq!(org_path("d f", &["a@b", "x"]), "/api/d%20f/a%40b/x");
        assert_eq!(Timestamp::from(5u64).as_micros(), 5);
        assert_eq!(Timestamp::from(u64::MAX).as_micros(), i64::MAX);
        assert_eq!(Timestamp::from(-3i64).as_micros(), -3);
        let before = UNIX_EPOCH - Duration::from_micros(7);
        assert_eq!(Timestamp::from(before).as_micros(), -7);
        assert_eq!(micros_to_system_time(-7), before);
        assert_eq!(StreamType::Traces.to_string(), "traces");
        assert_eq!(StreamType::Metrics.as_str(), "metrics");
        assert_eq!(uint(&json!(-4)), 0);
        assert_eq!(uint(&json!(4.7)), 4);
        assert_eq!(uint(&json!("12")), 12);
        assert_eq!(int(&json!(-4.2)), -4);
        assert_eq!(int(&json!("-9")), -9);
        assert_eq!(int(&json!(u64::MAX)), i64::MAX);
        assert_eq!(number(&json!("nan")), None);
        assert_eq!(number(&json!(true)), None);
        assert_eq!(scalar_text(&json!(3)), Some("3".into()));
        assert_eq!(scalar_text(&json!(true)), None);
        assert_eq!(text(&json!("")), None);
        assert!(items(&json!({})).is_empty());
        assert!(window(
            Some(Timestamp::Micros(5)),
            Some(Timestamp::Micros(1)),
            Duration::ZERO,
            "op"
        )
        .is_err());
        let (s, e) = window(
            None,
            Some(Timestamp::Micros(100_000_000)),
            Duration::from_secs(1),
            "op",
        )
        .unwrap();
        assert_eq!((s, e), (100_000_000 - 61_000_000, 100_000_000));
    }

    #[test]
    fn remap() {
        let e = remap_error(
            Error::from_status(400, "x: Search stream not found: s").with_request_id("r"),
        );
        assert_eq!(e.code(), ErrorCode::NotFound);
        assert_eq!(e.request_id(), Some("r"));
        let e = remap_error(Error::from_status(400, "User already exists"));
        assert_eq!(e.code(), ErrorCode::Conflict);
        let e = remap_error(Error::from_status(400, "bad sql"));
        assert_eq!(e.code(), ErrorCode::Validation);
        let e = remap_error(Error::from_status(404, "stream not found"));
        assert_eq!(e.code(), ErrorCode::NotFound);
        assert_eq!(e.http_status(), Some(404));
    }
}
