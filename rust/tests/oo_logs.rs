// Service-owned.
//! Logs module (ingest, search, around, values, histogram, tail) against recorded
//! OpenObserve responses.

mod support;

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_openobserve as sdk;
use sdk::core::ErrorCode;
use sdk::logging::{LogRecord, Severity};
use sdk::logs::*;
use sdk::{StreamType, Timestamp};
use serde_json::{json, Value};
use support::*;
use wiremock::matchers::{header as h, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn gunzip(bytes: &[u8]) -> String {
    let mut out = String::new();
    flate2::read::GzDecoder::new(bytes)
        .read_to_string(&mut out)
        .unwrap();
    out
}

#[tokio::test]
async fn ingest_json_round_trip_and_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/default/app/_json"))
        .and(h("authorization", ROOT_AUTH))
        .and(h("content-type", "application/json"))
        .respond_with(respond("ingestJson.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.200_partial_failure"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.400_invalid_json"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.401"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let ok = logs
        .ingest_json("App", &[json!({"message": "a"})], IngestOptions::default())
        .await
        .unwrap();
    let recorded = body("ingestJson.ok");
    assert_eq!(
        ok.successful,
        recorded["status"][0]["successful"].as_u64().unwrap()
    );
    assert_eq!(ok.failed, 0);
    assert!(ok.error.is_none());
    let partial = logs
        .ingest_json("app", &[json!({}), json!({})], IngestOptions::default())
        .await
        .unwrap();
    assert_eq!((partial.successful, partial.failed), (1, 1));
    assert!(partial.error.unwrap().to_lowercase().contains("old"));
    let err = logs
        .ingest_json("app", &[json!({})], IngestOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let err = logs
        .ingest_json("app", &[json!({})], IngestOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert_eq!(
        logs.ingest_json(" ", &[], IngestOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let reqs = requests(&server).await;
    assert_eq!(json_body(&reqs[0]), json!([{"message": "a"}]));
}

#[tokio::test]
async fn ingest_gzip_and_multi() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.gzip"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_multi"))
        .and(h("content-type", "application/x-ndjson"))
        .respond_with(respond("ingestMulti.ok"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_multi"))
        .respond_with(respond("ingestMulti.400_invalid_line"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    logs.ingest_json("app", &[json!({"m": 1})], IngestOptions::gzip(true))
        .await
        .unwrap();
    let multi = logs
        .ingest_multi_records(
            "app",
            &[json!({"m": 1}), json!({"m": 2})],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert!(multi.successful > 0);
    logs.ingest_multi("app", "{\"m\":3}\n", IngestOptions::gzip(true))
        .await
        .unwrap();
    let err = logs
        .ingest_multi("app", "{bad\n", IngestOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let reqs = requests(&server).await;
    assert_eq!(
        header(&reqs[0], "content-encoding").as_deref(),
        Some("gzip")
    );
    assert_eq!(gunzip(&reqs[0].body), "[{\"m\":1}]");
    assert_eq!(
        String::from_utf8(reqs[1].body.clone()).unwrap(),
        "{\"m\":1}\n{\"m\":2}\n"
    );
    assert_eq!(gunzip(&reqs[2].body), "{\"m\":3}\n");
}

#[tokio::test]
async fn ingest_bulk_actions_and_item_errors() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/_bulk"))
        .respond_with(respond("ingestBulk.ok"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_bulk"))
        .respond_with(respond("ingestBulk.200_item_error"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let ok = logs
        .ingest_bulk(
            &[
                BulkAction::index("App", json!({"message": "a"})),
                BulkAction::create("app", json!({"message": "b"})),
            ],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert!(!ok.errors);
    assert!(!ok.items.is_empty());
    assert_eq!(ok.items[0].action, "index");
    assert_eq!(ok.items[0].status, 200);
    let raw = logs
        .ingest_bulk_raw(
            "{\"create\":{\"_index\":\"app\"}}\n{\"m\":1}",
            IngestOptions::gzip(true),
        )
        .await
        .unwrap();
    assert!(!raw.errors);
    let failed = logs
        .ingest_bulk(
            &[BulkAction::index("app", json!({}))],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert!(failed.errors);
    let item = &failed.items[0];
    assert_eq!(item.status, 422);
    let err = item.error.as_ref().unwrap();
    assert_eq!(err.error_type, "timestamp_parsing_failed");
    assert!(!err.reason.is_empty());

    for (body, what) in [
        (
            "{\"delete\":{\"_index\":\"app\",\"_id\":\"1\"}}\n",
            "delete",
        ),
        ("{\"update\":{\"_index\":\"app\"}}\n{}\n", "update"),
        ("not json\n{}\n", "not JSON"),
        ("\n\n", "no actions"),
    ] {
        let e = logs
            .ingest_bulk_raw(body, IngestOptions::default())
            .await
            .unwrap_err();
        assert_eq!(e.code(), ErrorCode::Validation, "{what}");
    }
    assert_eq!(
        logs.ingest_bulk(&[], IngestOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    assert_eq!(
        logs.ingest_bulk(
            &[BulkAction::index(" ", json!({}))],
            IngestOptions::default()
        )
        .await
        .unwrap_err()
        .code(),
        ErrorCode::Validation
    );
    let reqs = requests(&server).await;
    assert_eq!(
        String::from_utf8(reqs[0].body.clone()).unwrap(),
        "{\"index\":{\"_index\":\"app\"}}\n{\"message\":\"a\"}\n{\"create\":{\"_index\":\"app\"}}\n{\"message\":\"b\"}\n"
    );
    assert!(gunzip(&reqs[1].body).ends_with("{\"m\":1}\n"));
}

#[tokio::test]
async fn bulk_item_shapes() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "took": 3,
            "errors": false,
            "items": [{"create": {"_index": "a"}}, "junk", {"index": {"_index": "b", "status": 500, "error": {}}}],
        })))
        .mount(&server)
        .await;
    let res = client(&server)
        .logs()
        .ingest_bulk(
            &[BulkAction::index("a", json!({}))],
            IngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(res.took, 3);
    assert!(res.errors);
    assert_eq!(res.items[0].action, "create");
    assert_eq!(res.items[1].stream, "");
    let e = res.items[2].error.as_ref().unwrap();
    assert_eq!((e.error_type.as_str(), e.reason.as_str()), ("error", ""));
}

#[tokio::test]
async fn ingest_otlp_records() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/v1/logs"))
        .and(h("content-type", "application/json"))
        .respond_with(respond("otlpLogs.json"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/v1/logs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"partialSuccess": {"rejectedLogRecords": "5"}})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/v1/logs"))
        .respond_with(respond("otlpLogs.401"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let mut record = LogRecord::new(Severity::Error, "boom")
        .with_attribute("http.method", "GET")
        .with_trace("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
    record.trace_flags = Some(1);
    record.observed_timestamp = Some(UNIX_EPOCH + Duration::from_secs(2));
    record.timestamp = UNIX_EPOCH + Duration::from_secs(1);
    record.resource.insert("service.name".into(), json!("svc"));
    let res = logs
        .ingest_otlp(
            std::slice::from_ref(&record),
            OtlpIngestOptions {
                gzip: false,
                stream: Some("Otlp-Stream".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (1, 0));
    let res = logs
        .ingest_otlp(
            &[record.clone(), record.clone()],
            OtlpIngestOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!((res.accepted, res.rejected), (0, 2));
    assert_eq!(
        logs.ingest_otlp(&[record], OtlpIngestOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Authentication
    );
    assert_eq!(
        logs.ingest_otlp(&[], OtlpIngestOptions::default())
            .await
            .unwrap(),
        OtlpIngestResult::default()
    );
    let reqs = requests(&server).await;
    assert_eq!(
        header(&reqs[0], "stream-name").as_deref(),
        Some("otlp_stream")
    );
    let b = json_body(&reqs[0]);
    let rl = &b["resourceLogs"][0];
    assert_eq!(
        rl["resource"]["attributes"][0],
        json!({"key": "service.name", "value": {"stringValue": "svc"}})
    );
    let lr = &rl["scopeLogs"][0]["logRecords"][0];
    assert_eq!(
        rl["scopeLogs"][0]["scope"]["name"],
        "grow-codes-openobserve"
    );
    assert_eq!(lr["timeUnixNano"], "1000000000");
    assert_eq!(lr["observedTimeUnixNano"], "2000000000");
    assert_eq!(lr["severityNumber"], 17);
    assert_eq!(lr["body"]["stringValue"], "boom");
    assert_eq!(lr["traceId"], "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(lr["spanId"], "00f067aa0ba902b7");
    assert_eq!(lr["flags"], 1);
    assert_eq!(header(&reqs[1], "stream-name").as_deref(), Some("default"));
}

#[tokio::test]
async fn search_paging_total_and_errors() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/_search"))
        .and(query_param("type", "logs"))
        .respond_with(respond("search.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.paging"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.track_total_hits"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.400_stream_not_found"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.400_unknown_field"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.422_missing_start_time"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let start = SystemTime::now() - Duration::from_secs(60);
    let res = logs
        .search(
            "SELECT * FROM \"app\"",
            SearchOptions::default().range(Some(start.into()), None),
        )
        .await
        .unwrap();
    assert_eq!(res.hits.len(), 5);
    assert_eq!(res.total, 5);
    assert_eq!(res.scan_records, 5);
    assert!(!res.is_partial);
    assert!(res.total_hits.is_none());
    assert_eq!(
        res.request_id.as_deref(),
        Some("01a1f050f76b74d09079000b8030b68f")
    );
    assert!(res.hits[0].contains_key("_timestamp"));
    let paged = logs
        .search(
            "SELECT * FROM \"app\"",
            SearchOptions::default().page(1, 2).track_total_hits(true),
        )
        .await
        .unwrap();
    assert_eq!(paged.hits.len(), 2);
    assert!(paged.total_hits.unwrap() >= 2);
    let err = logs
        .search("SELECT * FROM x", SearchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::NotFound);
    assert_eq!(err.http_status(), Some(400));
    let err = logs
        .search("SELECT nocol FROM x", SearchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    assert!(err.message().contains("unknown field"), "{err}");
    let err = logs
        .search("SELECT 1", SearchOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.http_status(), Some(422));
    assert_eq!(
        logs.search("  ", SearchOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let bad_window = SearchOptions::default().range(Some(Timestamp::Micros(10)), Some(1i64.into()));
    assert_eq!(
        logs.search("SELECT 1", bad_window)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let reqs = requests(&server).await;
    let q = &json_body(&reqs[0])["query"];
    assert_eq!(q["sql"], "SELECT * FROM \"app\"");
    let s = q["start_time"].as_i64().unwrap();
    assert!(s > 1_700_000_000_000_000, "µs expected, got {s}");
    assert_eq!(q["size"], 100);
    let count = json_body(&reqs[2]);
    assert_eq!(count["query"]["track_total_hits"], true);
    assert_eq!(count["query"]["from"], 0);
    assert_eq!(json_body(&reqs[1])["query"]["from"], 1);
}

#[tokio::test]
async fn search_track_total_hits_fallback() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"hits": [], "total": 7})))
        .mount(&server)
        .await;
    let res = client(&server)
        .logs()
        .search(
            "SELECT 1",
            SearchOptions::default()
                .track_total_hits(true)
                .stream_type(StreamType::Traces),
        )
        .await
        .unwrap();
    assert_eq!(res.total_hits, Some(7));
    assert_eq!(
        query(&requests(&server).await[0], "type").as_deref(),
        Some("traces")
    );
}

#[tokio::test]
async fn around_values_histogram() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/app/_around"))
        .respond_with(respond("searchAround.ok"))
        .up_to_n_times(2)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/nope/_around"))
        .respond_with(respond("searchAround.400_stream_not_found"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_values"))
        .respond_with(respond("valuesOf.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_values"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"field": "x", "values": [{"zo_sql_key": null, "zo_sql_num": 1}]}]
        })))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("histogram.ok"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let around = logs
        .search_around(
            "app",
            1_795_033_200_242_779i64,
            SearchAroundOptions::default(),
        )
        .await
        .unwrap();
    assert!(!around.hits.is_empty());
    logs.search_around(
        "app",
        SystemTime::now(),
        SearchAroundOptions {
            size: 4,
            sql: Some("SELECT * FROM \"app\" WHERE n > 1".into()),
            stream_type: StreamType::Logs,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        logs.search_around("nope", 1i64, SearchAroundOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    let values = logs
        .values_of("app", &["level"], ValuesOptions::default())
        .await
        .unwrap();
    assert_eq!(values[0].field, "level");
    assert!(values[0].values[0].count > 0);
    assert!(values[0].values[0].value.is_string());
    let odd = logs
        .values_of("app", &["x"], ValuesOptions::default())
        .await
        .unwrap();
    assert_eq!(odd[0].values[0].value, json!("null"));
    assert_eq!(
        logs.values_of("app", &[], ValuesOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let hist = logs
        .histogram(
            "app",
            HistogramOptions {
                interval: Some("1 minute".into()),
                filter: Some("level = 'error'".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!hist.buckets.is_empty());
    let b = &hist.buckets[0];
    assert!(b.count > 0);
    assert!(b.start > UNIX_EPOCH);
    assert!(b.key.contains('T'));
    assert!(hist.interval_seconds.is_some());
    logs.histogram("app", HistogramOptions::default())
        .await
        .unwrap();
    assert_eq!(
        logs.histogram(
            "app",
            HistogramOptions {
                interval: Some("1; DROP".into()),
                ..Default::default()
            }
        )
        .await
        .unwrap_err()
        .code(),
        ErrorCode::Validation
    );
    let reqs = requests(&server).await;
    assert_eq!(query(&reqs[0], "key").as_deref(), Some("1795033200242779"));
    assert_eq!(query(&reqs[0], "size").as_deref(), Some("10"));
    let sql = query(&reqs[1], "sql").unwrap();
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sql)
        .unwrap();
    assert_eq!(decoded, b"SELECT * FROM \"app\" WHERE n > 1");
    assert_eq!(query(&reqs[3], "fields").as_deref(), Some("level"));
    assert!(query(&reqs[3], "start_time").is_some());
    let hist_sql = json_body(&reqs[5])["query"]["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        hist_sql,
        "SELECT histogram(_timestamp, '1 minute') AS zo_sql_key, count(*) AS zo_sql_num FROM \"app\" WHERE level = 'error' GROUP BY zo_sql_key ORDER BY zo_sql_key"
    );
    assert_eq!(json_body(&reqs[5])["query"]["size"], -1);
}

#[tokio::test]
async fn tail_polls_with_cursor_and_dedupes() {
    let server = MockServer::start().await;
    let now = sdk::system_time_to_micros(SystemTime::now());
    Mock::given(path("/api/default/app/_search"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [
                {"_timestamp": now + 1, "message": "a"},
                {"_timestamp": now + 2, "message": "b"},
            ]
        })))
        .up_to_n_times(3)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.400_bad_sql"))
        .mount(&server)
        .await;
    let logs = client(&server).logs();
    let rows = Arc::new(Mutex::new(Vec::<Value>::new()));
    let errors = Arc::new(Mutex::new(0usize));
    let (r, e) = (rows.clone(), errors.clone());
    let handle = logs
        .tail(
            "app",
            Arc::new(move |row| r.lock().unwrap().push(Value::Object(row))),
            LogTailOptions {
                page_size: 2,
                ..LogTailOptions::default()
                    .filter("match_all('x')")
                    .start(now)
                    .interval(Duration::from_millis(20))
                    .on_error(move |_| *e.lock().unwrap() += 1)
            },
        )
        .unwrap();
    for _ in 0..200 {
        if *errors.lock().unwrap() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    handle.stop();
    let rows = rows.lock().unwrap().clone();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0]["message"], "a");
    assert!(*errors.lock().unwrap() > 0);
    let reqs = requests(&server).await;
    let sql = json_body(&reqs[0])["query"]["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(sql.contains("AND (match_all('x'))"), "{sql}");
    assert!(sql.contains("ORDER BY _timestamp ASC"));
    // second page request starts again from the advanced cursor minus the overlap
    let sql2 = json_body(&reqs[1])["query"]["sql"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        sql2.contains(&format!(">= {}", now + 2 - 1_000_000)),
        "{sql2}"
    );
    assert!(logs
        .tail("  ", Arc::new(|_| {}), LogTailOptions::default())
        .is_err());
    // stopping right away ends the task before the first poll
    let h = logs
        .tail("app", Arc::new(|_| {}), LogTailOptions::default())
        .unwrap();
    h.stop();
}

#[test]
fn tail_outside_runtime_is_config_error() {
    let server_uri = "http://127.0.0.1:9";
    let c = sdk::OpenObserveClient::new(
        sdk::OpenObserveConfig::builder()
            .url(server_uri)
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = c
        .logs()
        .tail("app", Arc::new(|_| {}), LogTailOptions::default())
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Config);
}
