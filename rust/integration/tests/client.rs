// Service-owned.
//! Client, config and blocking twins against the real OpenObserve (Docker).

use base64::Engine as _;
use grow_openobserve as sdk;
use grow_openobserve_integration as support;
use sdk::blocking::OpenObserveClient as BlockingClient;
use sdk::logs::{IngestOptions, SearchOptions};
use sdk::tracing::TraceSearchQuery;
use sdk::{ErrorCode, OpenObserveClient as Client, OpenObserveConfig as Config, StreamType};
use serde_json::json;
use support::*;

#[tokio::test]
async fn client_round_trip() {
    let c = root_client();
    let health = c.health().await.expect("health");
    assert!(health.healthy, "service not healthy: {health:?}");
    assert_eq!(health.status, "ok");
    c.ping().await.expect("authenticated ping");
    let version = c.server_version().await.expect("version");
    if let Ok(expected) = std::env::var("OPENOBSERVE_SERVER_VERSION") {
        assert_eq!(version, expected);
    }
    assert!(version.starts_with('v'), "{version}");
    c.close().await;
    c.close().await;
    assert!(c.is_closed());
}

#[tokio::test]
async fn service_account_and_pre_encoded_credentials() {
    let sa = service_account_config();
    let c = Client::new(sa.clone()).unwrap();
    c.ping().await.expect("service-account ping");
    let header = base64::engine::general_purpose::STANDARD.encode(format!(
        "{}:{}",
        sa.user().unwrap(),
        sa.token().unwrap().expose_secret()
    ));
    let pre = Client::new(
        Config::builder()
            .url(sa.url().as_str())
            .token(format!("Basic {header}"))
            .build()
            .unwrap(),
    )
    .unwrap();
    pre.ping().await.expect("pre-encoded ping");
    let bad = Client::new(
        Config::builder()
            .url(sa.url().as_str())
            .user("nobody@example.com")
            .password("wrong-password-1")
            .build()
            .unwrap(),
    )
    .unwrap();
    let err = bad.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(!err.to_string().contains("wrong-password-1"));
    let anon = Client::new(Config::builder().url(sa.url().as_str()).build().unwrap()).unwrap();
    assert!(anon.health().await.unwrap().healthy);
    assert_eq!(
        anon.server_version().await.unwrap_err().code(),
        ErrorCode::Authentication
    );
}

#[test]
fn blocking_round_trip() {
    load_bootstrap_env();
    let c = BlockingClient::from_env().expect("client");
    assert!(c.health().expect("health").healthy);
    c.ping().expect("ping");
    assert!(c.server_version().unwrap().starts_with('v'));
    let stream = unique("blocking");
    let marker = sdk::testing::run_id();
    let res = c
        .ingest_json(
            &stream,
            &[json!({"message": format!("blocking {marker}")})],
            IngestOptions::default(),
        )
        .unwrap();
    assert_eq!(res.successful, 1);
    let res = c
        .ingest_multi(
            &stream,
            &format!("{{\"message\":\"multi {marker}\"}}\n"),
            IngestOptions::default(),
        )
        .unwrap();
    assert_eq!(res.successful, 1);
    let sql = format!("SELECT * FROM \"{stream}\" WHERE match_all('{marker}')");
    let mut hits = 0;
    for _ in 0..40 {
        hits = c.search(&sql, SearchOptions::default()).unwrap().hits.len();
        if hits >= 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    assert_eq!(hits, 2);
    assert!(c
        .list_streams(StreamType::Logs, Default::default())
        .unwrap()
        .iter()
        .any(|s| s.name == stream));
    assert!(c
        .get_stream_schema(&stream, StreamType::Logs)
        .unwrap()
        .schema
        .iter()
        .any(|f| f.name == "message"));
    assert!(c
        .get_trace("0af7651916cd43dd8448eb211c80319c")
        .unwrap()
        .is_none());
    let _ = c.search_traces(&TraceSearchQuery::default()).unwrap();
    assert!(c
        .list_organizations()
        .unwrap()
        .iter()
        .any(|o| o.identifier == "default"));
    c.delete_stream(&stream, StreamType::Logs).unwrap();
    assert_eq!(
        c.get_stream_schema(&stream, StreamType::Logs)
            .unwrap_err()
            .code(),
        ErrorCode::NotFound
    );
    c.close();
}
