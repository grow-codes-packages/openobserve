// Service-owned.
//! Logs module against the real service: every ingest call is read back with search, plus
//! around, values, histogram, tail, the log query client and the log client.

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use grow_openobserve as sdk;
use grow_openobserve_integration as support;
use sdk::logging::{
    LogClient, LogEntry, LogInput, LogQueryClient, LogRecord, SearchOptions as QueryOptions,
    Severity, TailOptions, WaitOptions,
};
use sdk::logs::*;
use sdk::{ErrorCode, JsonObject, LogClientOptions, LogProtocol, OpenObserveClient, StreamType};
use serde_json::{json, Value};
use support::*;

async fn rows(c: &OpenObserveClient, stream: &str, filter: &str, min: usize) -> Vec<JsonObject> {
    let sql = format!("SELECT * FROM \"{stream}\" WHERE {filter} ORDER BY _timestamp ASC");
    eventually(
        || async {
            c.logs()
                .search(&sql, SearchOptions::default())
                .await
                .unwrap()
                .hits
        },
        |hits| hits.len() >= min,
    )
    .await
}

fn now_micros() -> i64 {
    sdk::system_time_to_micros(SystemTime::now())
}

#[tokio::test]
async fn ingest_round_trips() {
    let c = root_client();
    let stream = unique("logs");
    let logs = c.logs();
    let now = now_micros();

    // _json: flattened fields, µs timestamps
    let res = logs
        .ingest_json(
            &stream,
            &[
                json!({"message": "json alpha", "level": "info", "src": "json", "n": 1, "_timestamp": now}),
                json!({"message": "json beta", "level": "error", "src": "json", "n": 2, "_timestamp": now + 1, "user": {"id": 7}}),
            ],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        (res.stream.as_str(), res.successful, res.failed),
        (stream.as_str(), 2, 0)
    );
    let hits = rows(&c, &stream, "src = 'json'", 2).await;
    assert_eq!(hits[0]["message"], "json alpha");
    assert_eq!(hits[0]["_timestamp"], now);
    assert_eq!(hits[1]["user_id"], 7);

    // partial failure keeps HTTP 200
    let too_old = now - 10 * 3600 * 1_000_000;
    let res = logs
        .ingest_json(
            &stream,
            &[
                json!({"message": "fresh", "src": "partial"}),
                json!({"message": "old", "src": "partial", "_timestamp": too_old}),
            ],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.successful, res.failed), (1, 1));
    assert!(res.error.unwrap().to_lowercase().contains("old"));

    // _multi (gzip and records)
    let res = logs
        .ingest_multi(
            &stream,
            "{\"message\":\"multi 0\",\"src\":\"multi\"}\n{\"message\":\"multi 1\",\"src\":\"multi\"}\n",
            IngestOptions::gzip(true),
        )
        .await
        .unwrap();
    assert_eq!(res.successful, 2);
    logs.ingest_multi_records(
        &stream,
        &[json!({"message": "multi 2", "src": "multi"})],
        IngestOptions::default(),
    )
    .await
    .unwrap();
    let hits = rows(&c, &stream, "src = 'multi'", 3).await;
    let mut messages: Vec<&str> = hits
        .iter()
        .map(|h| h["message"].as_str().unwrap())
        .collect();
    messages.sort_unstable();
    assert_eq!(messages, ["multi 0", "multi 1", "multi 2"]);
    let err = logs
        .ingest_multi(&stream, "{\"ok\":1}\n{broken\n", IngestOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);

    // _bulk (typed and raw); delete actions are rejected client-side
    let res = logs
        .ingest_bulk(
            &[
                BulkAction::index(&stream, json!({"message": "bulk a", "src": "bulk"})),
                BulkAction::create(&stream, json!({"message": "bulk b", "src": "bulk"})),
            ],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert!(!res.errors, "{res:?}");
    assert_eq!(res.items.len(), 2);
    let raw = format!(
        "{{\"index\":{{\"_index\":\"{stream}\"}}}}\n{{\"message\":\"bulk c\",\"src\":\"bulk\"}}\n"
    );
    assert!(
        !logs
            .ingest_bulk_raw(&raw, IngestOptions::gzip(true))
            .await
            .unwrap()
            .errors
    );
    let bad = logs
        .ingest_bulk(
            &[BulkAction::index(
                &stream,
                json!({"src": "bulk", "_timestamp": too_old}),
            )],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert!(bad.errors);
    assert!(bad.items[0].error.is_some());
    let err = logs
        .ingest_bulk_raw(
            &format!("{{\"delete\":{{\"_index\":\"{stream}\",\"_id\":\"1\"}}}}\n"),
            IngestOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert_eq!(rows(&c, &stream, "src = 'bulk'", 3).await.len(), 3);

    // OTLP logs with trace ids
    let mut record = LogRecord::new(Severity::Warn, "otlp hello")
        .with_attribute("src", "otlp")
        .with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    record
        .resource
        .insert("service.name".into(), json!("rs-otlp"));
    let res = logs
        .ingest_otlp(
            &[record],
            OtlpIngestOptions {
                gzip: true,
                stream: Some(stream.clone()),
            },
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (1, 0));
    let hits = rows(&c, &stream, "src = 'otlp'", 1).await;
    assert_eq!(hits[0]["body"], "otlp hello");
    assert_eq!(hits[0]["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(hits[0]["service_name"], "rs-otlp");
    let back = row_to_log_record(&hits[0]);
    assert_eq!(back.severity, Severity::Warn);
    assert_eq!(back.span_id.as_deref(), Some("00f067aa0ba902b7"));

    c.streams()
        .delete_stream(&stream, StreamType::Logs)
        .await
        .unwrap();
}

#[tokio::test]
async fn search_around_values_histogram() {
    let c = root_client();
    let stream = unique("search");
    let logs = c.logs();
    let now = now_micros();
    let records: Vec<Value> = (0..5)
        .map(|i| json!({"message": format!("row {i}"), "level": if i % 2 == 0 { "info" } else { "error" }, "_timestamp": now + i}))
        .collect();
    logs.ingest_json(&stream, &records, IngestOptions::default())
        .await
        .unwrap();
    rows(&c, &stream, "level IS NOT NULL", 5).await;
    let sql = format!("SELECT * FROM \"{stream}\" ORDER BY _timestamp DESC");
    let page = logs
        .search(
            &sql,
            SearchOptions::default().page(1, 2).track_total_hits(true),
        )
        .await
        .unwrap();
    assert_eq!(page.hits.len(), 2);
    assert_eq!(page.total_hits, Some(5));
    assert_eq!(page.hits[0]["message"], "row 3");
    // milliseconds would silently find nothing; Timestamp conversion keeps µs
    let window = logs
        .search(
            &sql,
            SearchOptions::default().range(
                Some((SystemTime::now() - Duration::from_secs(600)).into()),
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(window.hits.len(), 5);
    let err = logs
        .search(
            "SELECT * FROM \"rs_missing_stream_x\"",
            SearchOptions::default(),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);

    let around = logs
        .search_around(
            &stream,
            now + 2,
            SearchAroundOptions {
                size: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(
        around.hits.iter().any(|h| h["_timestamp"] == now + 2),
        "{around:?}"
    );
    let filtered = logs
        .search_around(
            &stream,
            now + 2,
            SearchAroundOptions {
                size: 10,
                sql: Some(format!("SELECT * FROM \"{stream}\" WHERE level = 'error'")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // v1.1.2 accepts the `sql` filter but does not reliably narrow the rows (verified only for
    // HTTP 200 by the prep suite), so only the call itself is asserted here.
    assert!(!filtered.hits.is_empty(), "{filtered:?}");

    let values = logs
        .values_of(&stream, &["level"], ValuesOptions::default())
        .await
        .unwrap();
    let level = values.iter().find(|v| v.field == "level").unwrap();
    let info = level.values.iter().find(|v| v.value == "info").unwrap();
    assert_eq!(info.count, 3);

    let hist = logs
        .histogram(
            &stream,
            HistogramOptions {
                interval: Some("1 minute".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(hist.buckets.iter().map(|b| b.count).sum::<u64>(), 5);
    let errors = logs
        .histogram(
            &stream,
            HistogramOptions {
                filter: Some("level = 'error'".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(errors.buckets.iter().map(|b| b.count).sum::<u64>(), 2);

    c.streams()
        .delete_stream(&stream, StreamType::Logs)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tail_query_client_and_log_client() {
    let c = root_client();
    let stream = unique("tail");
    let logs = c.logs();
    logs.ingest_json(
        &stream,
        &[json!({"message": "seed"})],
        IngestOptions::default(),
    )
    .await
    .unwrap();

    // logs.tail
    let rows_seen = Arc::new(Mutex::new(Vec::<JsonObject>::new()));
    let r = rows_seen.clone();
    let handle = logs
        .tail(
            &stream,
            Arc::new(move |row| r.lock().unwrap().push(row)),
            LogTailOptions::default()
                .interval(Duration::from_millis(200))
                .filter("message = 'tailed'"),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    logs.ingest_json(
        &stream,
        &[json!({"message": "tailed"}), json!({"message": "other"})],
        IngestOptions::default(),
    )
    .await
    .unwrap();
    let seen = eventually(|| async { rows_seen.lock().unwrap().len() }, |n| *n >= 1).await;
    handle.stop();
    assert_eq!(seen, 1);

    // the LogQueryClient + LogClient pair (both protocols)
    let query = c.log_query(Some(&stream)).unwrap();
    assert_eq!(query.stream(), stream);
    let marker = sdk::testing::run_id();
    let got = Arc::new(Mutex::new(Vec::<LogRecord>::new()));
    let g = got.clone();
    let tail = query
        .tail(
            &marker,
            Arc::new(move |r| g.lock().unwrap().push(r)),
            TailOptions {
                interval: Duration::from_millis(200),
                start: None,
            },
        )
        .await
        .unwrap();
    let json_client = c
        .log_client(
            LogClientOptions::default()
                .stream(&stream)
                .resource_attribute("service.name", "rs-logs"),
        )
        .unwrap();
    let otlp_client = c
        .log_client(
            LogClientOptions::default()
                .stream(&stream)
                .protocol(LogProtocol::Otlp),
        )
        .unwrap();
    json_client.send(LogEntry::Input(
        LogInput::new(format!("json {marker}"))
            .severity(Severity::Error)
            .attribute("password", "hunter2-secret")
            .trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"),
    ));
    let logger = otlp_client.logger(Some("rs"), Default::default());
    logger.warn(format!("otlp {marker}"), Default::default());
    json_client
        .flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    otlp_client
        .flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    assert_eq!(json_client.stats().failed, 0);

    let found = query
        .wait_for_log(
            &marker,
            WaitOptions::default()
                .timeout(Duration::from_secs(20))
                .predicate(|r| r.severity == Severity::Error),
        )
        .await
        .unwrap();
    assert_eq!(
        found.trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert_eq!(found.resource["service.name"], "rs-logs");
    assert!(!format!("{found:?}").contains("hunter2-secret"));
    let both = eventually(
        || async {
            query
                .search(&marker, QueryOptions::default())
                .await
                .unwrap()
        },
        |p| p.records.len() >= 2,
    )
    .await;
    assert_eq!(both.records.len(), 2);
    let paged = query
        .search_with(&marker, QueryOptions::default().limit(1), true)
        .await
        .unwrap();
    assert_eq!(paged.records.len(), 1);
    assert_eq!(paged.total, Some(2));
    assert_eq!(paged.next_cursor.as_deref(), Some("1"));
    let sql = query
        .search(
            &format!("SELECT * FROM \"{stream}\" WHERE message = 'seed'"),
            QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(sql.records[0].body, "seed");
    let tailed = eventually(|| async { got.lock().unwrap().len() }, |n| *n >= 2).await;
    tail.stop();
    assert!(tailed >= 2);
    assert!(query.list_streams().await.unwrap().contains(&stream));
    assert!(query
        .list_labels()
        .await
        .unwrap()
        .contains(&"body".to_string()));

    // the exporter used by the log client, directly
    let exporter = c.log_exporter(&stream, LogProtocol::Json);
    let res = exporter
        .export(
            &[LogRecord::new(Severity::Info, format!("exporter {marker}"))],
            Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(res.accepted, 1);

    c.close().await;
    root_client()
        .streams()
        .delete_stream(&stream, StreamType::Logs)
        .await
        .unwrap();
}
