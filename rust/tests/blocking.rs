// Taken over from sdk-kit 0.1.0 (header removed): OpenObserve tokens need a `user`, and the
// blocking twins of the service calls are exercised too.
//! Blocking client (feature `blocking`).
#![cfg(feature = "blocking")]

use std::io::{Read, Write};
use std::net::TcpListener;

use grow_openobserve as sdk;
use sdk::core::ErrorCode;

use sdk::blocking::OpenObserveClient as Client;

use sdk::OpenObserveConfig as Config;

/// Minimal HTTP server on a thread (no async runtime involved).
fn serve(responses: Vec<&'static str>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for body in responses {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let (status, json) = body.split_once(' ').unwrap();
            let resp = format!(
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{json}",
                json.len()
            );
            stream.write_all(resp.as_bytes()).unwrap();
        }
    });
    format!("http://{addr}")
}

#[test]
fn blocking_client() {
    let url = serve(vec![
        r#"200 {"status":"ok"}"#,
        r#"200 {"pong":true}"#,
        r#"200 {"version":"2.0.0"}"#,
        r#"200 {"code":200,"status":[{"name":"s","successful":1,"failed":0}]}"#,
        r#"200 {"code":200,"status":[{"name":"s","successful":2,"failed":0}]}"#,
        r#"200 {"hits":[{"_timestamp":1,"message":"m"}],"total":1}"#,
        r#"200 {"list":[{"name":"s","stream_type":"logs"}],"total":1}"#,
        r#"200 {"name":"s","stream_type":"logs","schema":[{"name":"message","type":"Utf8"}]}"#,
        r#"200 {"code":200,"message":"stream deleted"}"#,
        r#"200 {"hits":[]}"#,
        r#"200 {"hits":[{"trace_id":"0af7651916cd43dd8448eb211c80319c","spans":[2,0]}]}"#,
        r#"200 {"data":[{"id":1,"identifier":"default","name":"default","type":"default"}]}"#,
    ]);
    let cfg = Config::builder()
        .url(url)
        .user("sa@example.com")
        .token("t-1234")
        .build()
        .unwrap();
    let c = Client::new(cfg).unwrap();
    assert!(c.health().unwrap().healthy);
    c.ping().unwrap();
    assert_eq!(c.server_version().unwrap(), "2.0.0");
    let rec = [serde_json::json!({"message": "m"})];
    assert_eq!(
        c.ingest_json("s", &rec, Default::default())
            .unwrap()
            .successful,
        1
    );
    assert_eq!(
        c.ingest_multi("s", "{}\n{}\n", Default::default())
            .unwrap()
            .successful,
        2
    );
    assert_eq!(
        c.search("SELECT 1", Default::default()).unwrap().hits.len(),
        1
    );
    assert_eq!(
        c.list_streams(sdk::StreamType::Logs, Default::default())
            .unwrap()[0]
            .name,
        "s"
    );
    assert_eq!(
        c.get_stream_schema("s", sdk::StreamType::Logs)
            .unwrap()
            .schema
            .len(),
        1
    );
    c.delete_stream("s", sdk::StreamType::Logs).unwrap();
    assert!(c
        .get_trace("0af7651916cd43dd8448eb211c80319c")
        .unwrap()
        .is_none());
    assert_eq!(
        c.search_traces(&Default::default()).unwrap()[0].span_count,
        2
    );
    assert_eq!(c.list_organizations().unwrap()[0].identifier, "default");
    assert!(!c.async_client().is_closed());
    assert_eq!(c.block_on(async { 7 }), 7);
    c.close();
    c.close();
    assert_eq!(c.ping().unwrap_err().code(), ErrorCode::Cancelled);
    let _ = Client::from_env();
    let bad = Config::builder()
        .ca_cert_path("/nonexistent")
        .build()
        .unwrap();
    assert_eq!(Client::new(bad).unwrap_err().code(), ErrorCode::Config);
}
