// Service-owned.
//! Logging category adapters: `OpenObserveLogQuery` (text and SQL queries, cursor, tail,
//! streams, labels), the `_json` / OTLP exporters and the record mappings.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use grow_openobserve as sdk;
use sdk::core::ErrorCode;
use sdk::logging::{
    ExportOptions, LogExporter, LogQueryClient, LogRecord, SearchOptions, Severity, SortOrder,
    TailOptions, WaitOptions,
};
use sdk::logs::*;
use serde_json::json;
use support::*;
use wiremock::matchers::path;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn query_helpers() {
    assert!(is_sql_query("  select * from x"));
    assert!(is_sql_query("SELECT\n1"));
    assert!(!is_sql_query("selected items"));
    assert!(!is_sql_query("select"));
    assert_eq!(
        text_query_sql("app", "it's", SortOrder::Asc),
        "SELECT * FROM \"app\" WHERE match_all('it''s') ORDER BY _timestamp ASC"
    );
    assert_eq!(
        text_query_sql("app", " ", SortOrder::Desc),
        "SELECT * FROM \"app\" ORDER BY _timestamp DESC"
    );
}

#[tokio::test]
async fn search_text_sql_cursor_and_total() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.match_all"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.paging"))
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
        .mount(&server)
        .await;
    let q = client(&server).log_query(Some("app")).unwrap();
    let page = q.search("needle", SearchOptions::default()).await.unwrap();
    assert!(!page.records.is_empty());
    assert!(page.next_cursor.is_none());
    assert!(page.total.is_none());
    let rec = &page.records[0];
    assert!(rec.body.contains("needle"), "{rec:?}");
    let page = q
        .search(
            "SELECT * FROM \"app\"",
            SearchOptions::default()
                .limit(2)
                .cursor("1")
                .range(Some(SystemTime::now() - Duration::from_secs(5)), None),
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.next_cursor.as_deref(), Some("3"));
    let page = q
        .search_with("x", SearchOptions::default().limit(2), true)
        .await
        .unwrap();
    assert_eq!(page.total, Some(5));
    for (opts, what) in [
        (SearchOptions::default().limit(0), "limit"),
        (SearchOptions::default().cursor("abc"), "cursor"),
    ] {
        let err = q.search("x", opts).await.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Validation, "{what}");
    }
    let reqs = requests(&server).await;
    let first = json_body(&reqs[0]);
    assert_eq!(
        first["query"]["sql"],
        "SELECT * FROM \"app\" WHERE match_all('needle') ORDER BY _timestamp DESC"
    );
    assert_eq!(first["query"]["size"], 100);
    let second = json_body(&reqs[1]);
    assert_eq!(second["query"]["sql"], "SELECT * FROM \"app\"");
    assert_eq!(second["query"]["from"], 1);
    assert_eq!(second["query"]["size"], 2);
}

#[tokio::test]
async fn streams_labels_and_wait() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/streams"))
        .respond_with(respond("listStreams.logs"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/streams/default/schema"))
        .respond_with(respond("getStreamSchema.ok"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.ok"))
        .mount(&server)
        .await;
    let q = client(&server).log_query(None).unwrap();
    assert!(!q.list_streams().await.unwrap().is_empty());
    let labels = q.list_labels().await.unwrap();
    assert!(labels.contains(&"_timestamp".to_string()));
    let rec = q
        .wait_for_log(
            "hello",
            WaitOptions::default()
                .timeout(Duration::from_secs(5))
                .predicate(|r| r.severity == Severity::Error),
        )
        .await
        .unwrap();
    assert_eq!(rec.body, "hello beta");
    assert_eq!(rec.attributes["user_id"], 7);
}

#[tokio::test]
async fn tail_text_and_sql() {
    let server = MockServer::start().await;
    let now = sdk::system_time_to_micros(SystemTime::now()) + 5_000_000;
    Mock::given(path("/api/default/_search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "hits": [{"_timestamp": now, "message": "tailed", "level": "warn"}]
        })))
        .mount(&server)
        .await;
    let q = client(&server).log_query(Some("app")).unwrap();
    let got = Arc::new(Mutex::new(Vec::<LogRecord>::new()));
    let opts = TailOptions {
        interval: Duration::from_millis(20),
        start: Some(SystemTime::now()),
    };
    let g = got.clone();
    let text = q
        .tail(
            "tailed",
            Arc::new(move |r| g.lock().unwrap().push(r)),
            opts.clone(),
        )
        .await
        .unwrap();
    let g = got.clone();
    let sql = q
        .tail(
            "SELECT * FROM \"app\"",
            Arc::new(move |r| g.lock().unwrap().push(r)),
            opts,
        )
        .await
        .unwrap();
    for _ in 0..200 {
        if got.lock().unwrap().len() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    text.stop();
    sql.stop();
    let got = got.lock().unwrap().clone();
    assert!(got.len() >= 2, "{got:?}");
    assert!(got
        .iter()
        .all(|r| r.body == "tailed" && r.severity == Severity::Warn));
    let reqs = requests(&server).await;
    assert!(reqs.iter().any(|r| json_body(r)["query"]["sql"]
        .as_str()
        .unwrap()
        .contains("AND (match_all('tailed'))")));
    // an empty text query tails everything
    let all = q
        .tail(" ", Arc::new(|_| {}), TailOptions::default())
        .await
        .unwrap();
    all.stop();
}

#[test]
fn sql_tail_outside_runtime() {
    let c =
        sdk::OpenObserveClient::new(sdk::OpenObserveConfig::builder().build().unwrap()).unwrap();
    let q = c.log_query(None).unwrap();
    let mut fut = q.tail("SELECT 1", Arc::new(|_| {}), TailOptions::default());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(r) => assert_eq!(r.unwrap_err().code(), ErrorCode::Config),
        std::task::Poll::Pending => panic!("tail should fail at once"),
    }
}

#[tokio::test]
async fn json_exporter_counts_rejections() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.200_partial_failure"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/app/_json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": [{"name": "app", "successful": 0, "failed": 9}]
        })))
        .mount(&server)
        .await;
    let c = client(&server);
    let exporter = OpenObserveLogExporter::new(c.logs(), "app");
    assert_eq!(exporter.stream(), "app");
    let record = LogRecord::new(Severity::Info, "one");
    let ok = exporter
        .export(std::slice::from_ref(&record), ExportOptions::new(false))
        .await
        .unwrap();
    assert_eq!((ok.accepted, ok.rejected), (1, 0));
    let partial = exporter
        .export(&[record.clone(), record.clone()], ExportOptions::new(true))
        .await
        .unwrap();
    assert_eq!((partial.accepted, partial.rejected), (1, 1));
    let e = partial.error.unwrap();
    assert_eq!(e.code(), ErrorCode::Validation);
    assert!(e.message().contains("old"), "{e}");
    let none = exporter
        .export(std::slice::from_ref(&record), ExportOptions::default())
        .await
        .unwrap();
    assert_eq!((none.accepted, none.rejected), (0, 1));
    assert_eq!(
        exporter
            .export(&[], ExportOptions::default())
            .await
            .unwrap()
            .accepted,
        0
    );
    exporter.shutdown().await.unwrap();
}

#[tokio::test]
async fn otlp_exporter_counts_rejections() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/v1/logs"))
        .respond_with(respond("otlpLogs.json"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/v1/logs"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"partial_success": {"rejected_log_records": 1}})),
        )
        .mount(&server)
        .await;
    let exporter = OpenObserveOtlpLogExporter::new(client(&server).logs(), "otel");
    assert_eq!(exporter.stream(), "otel");
    let record = LogRecord::new(Severity::Warn, "w");
    let ok = exporter
        .export(std::slice::from_ref(&record), ExportOptions::new(false))
        .await
        .unwrap();
    assert_eq!((ok.accepted, ok.rejected), (1, 0));
    assert!(ok.error.is_none());
    let partial = exporter
        .export(&[record.clone(), record], ExportOptions::new(false))
        .await
        .unwrap();
    assert_eq!((partial.accepted, partial.rejected), (1, 1));
    assert!(partial.error.is_some());
}

#[test]
fn record_mappings() {
    let mut record = LogRecord::new(Severity::Error, "boom")
        .with_attribute("http.method", "GET")
        .with_attribute("tags", json!(["a", "b"]))
        .with_attribute("user", json!({"id": 7}))
        .with_trace("4BF92F3577B34DA6A3CE929D0E0E4736", "00F067AA0BA902B7");
    record.trace_flags = Some(1);
    record.timestamp = UNIX_EPOCH + Duration::from_micros(1_795_033_200_000_001);
    record.resource.insert("service.name".into(), json!("svc"));
    let row = log_record_to_row(&record);
    assert_eq!(row["_timestamp"], 1_795_033_200_000_001i64);
    assert_eq!(row["body"], "boom");
    assert_eq!(row["severity"], "ERROR");
    assert_eq!(row["severity_number"], 17);
    assert_eq!(row["http_method"], "GET");
    assert_eq!(row["tags"], "[\"a\",\"b\"]");
    assert_eq!(row["user"], json!({"id": 7}));
    assert_eq!(row["service_name"], "svc");
    assert_eq!(row["trace_flags"], 1);

    let back = row_to_log_record(&row);
    assert_eq!(back.timestamp, record.timestamp);
    assert_eq!(back.severity, Severity::Error);
    assert_eq!(back.body, "boom");
    assert_eq!(
        back.trace_id.as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    assert_eq!(back.span_id.as_deref(), Some("00f067aa0ba902b7"));
    assert_eq!(back.trace_flags, Some(1));
    assert_eq!(back.resource["service.name"], "svc");
    assert_eq!(back.attributes["user"], "{\"id\":7}");

    // OTLP rows (`search.otlp_logs`) normalize the same way
    let otlp = body("search.otlp_logs");
    let r = row_to_log_record(otlp["hits"][0].as_object().unwrap());
    assert!(!r.body.is_empty());
    assert!(r.trace_id.is_some());

    let plain = json!({"_timestamp": 5, "message": {"k": 1}, "level": "warning", "host_name": "h"});
    let r = row_to_log_record(plain.as_object().unwrap());
    assert_eq!(r.severity, Severity::Warn);
    assert_eq!(r.severity_text, "warning");
    assert_eq!(r.body, "{\"k\":1}");
    assert_eq!(r.resource["host.name"], "h");
    let empty = row_to_log_record(
        json!({"severity_number": 300, "log": null})
            .as_object()
            .unwrap(),
    );
    assert_eq!(empty.severity, Severity::Info);
    assert_eq!(empty.severity_text, "INFO");
    assert_eq!(empty.body, "");
    assert_eq!(
        row_to_log_record(
            json!({"severity_number": "21", "msg": "m"})
                .as_object()
                .unwrap()
        )
        .severity,
        Severity::Fatal
    );

    assert_eq!(otlp_value(&json!(1.5)), json!({"doubleValue": 1.5}));
    assert_eq!(otlp_value(&json!(3)), json!({"intValue": "3"}));
    assert_eq!(otlp_value(&json!(true)), json!({"boolValue": true}));
    assert_eq!(otlp_value(&json!(null)), json!({}));
    assert_eq!(
        otlp_value(&json!([1, "a"])),
        json!({"arrayValue": {"values": [{"intValue": "1"}, {"stringValue": "a"}]}})
    );
    assert_eq!(
        otlp_value(&json!({"k": "v", "n": null})),
        json!({"kvlistValue": {"values": [{"key": "k", "value": {"stringValue": "v"}}]}})
    );
    let mut attrs = sdk::logging::Attributes::new();
    attrs.insert("a".into(), json!(1));
    assert_eq!(otlp_attributes(&attrs).len(), 1);

    let mut other = LogRecord::new(Severity::Info, "two");
    other.resource.insert("service.name".into(), json!("other"));
    let req = to_otlp_logs_request(&[record.clone(), other, record], "0.1.0");
    let groups = req["resourceLogs"].as_array().unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!(
        groups[0]["scopeLogs"][0]["logRecords"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
