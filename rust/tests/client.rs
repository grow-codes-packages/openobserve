// Taken over from sdk-kit 0.1.0 (header removed): the skeleton tests targeted the demo
// service; these test the OpenObserve config and client surface against recorded responses.
//! `OpenObserveConfig` and `OpenObserveClient`: env precedence, validation, Basic auth,
//! health, ping, server version, log clients and close.

mod support;

use std::time::Duration;

use grow_openobserve as sdk;
use sdk::core::{EnvReader, ErrorCode};
use sdk::logging::{LogClient, LogEntry, LogQueryClient};
use sdk::tracing::TraceQueryClient;
use sdk::{
    LogClientOptions, LogProtocol, OpenObserveClient as Client, OpenObserveConfig as Config,
};
use serde_json::json;
use support::*;
use wiremock::matchers::{header as h, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn env(pairs: &[(&str, &str)]) -> EnvReader {
    EnvReader::from_map(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())))
}

#[test]
fn config_defaults_and_env_precedence() {
    let c = Config::builder().build().unwrap();
    assert_eq!(c.url().as_str(), "http://127.0.0.1:5080/");
    assert_eq!(c.org(), "default");
    assert_eq!(c.stream(), "default");
    assert_eq!(c.traces_stream(), "default");
    assert!(c.user().is_none() && !c.has_credentials() && c.authorization().is_none());
    assert!(c.known_secrets().is_empty());

    let e = env(&[
        ("OPENOBSERVE_URL", "http://vendor:5080"),
        ("GROW_OPENOBSERVE_URL", "http://grow:5080"),
        ("OPENOBSERVE_ORG", "acme"),
        ("OPENOBSERVE_USER", "root@example.com"),
        ("OPENOBSERVE_PASSWORD", "pw-from-env"),
        ("OPENOBSERVE_STREAM", "App-Logs.x"),
        ("GROW_OPENOBSERVE_TRACES_STREAM", "Traces"),
    ]);
    let c = Config::builder().build_with_env(&e).unwrap();
    assert_eq!(c.url().as_str(), "http://grow:5080/");
    assert_eq!(c.org(), "acme");
    assert_eq!(c.user(), Some("root@example.com"));
    assert_eq!(c.password().unwrap().expose_secret(), "pw-from-env");
    assert_eq!(c.stream(), "app_logs_x");
    assert_eq!(c.traces_stream(), "traces");
    // explicit values win; an explicit token drops the password from the environment
    let c = Config::builder()
        .org("explicit")
        .user("sa@example.com")
        .token("tok")
        .stream("s")
        .traces_stream("t")
        .build_with_env(&e)
        .unwrap();
    assert_eq!(c.org(), "explicit");
    assert!(c.password().is_none());
    assert_eq!(c.token().unwrap().expose_secret(), "tok");
    // and an explicit password drops a token from the environment
    let e2 = env(&[("OPENOBSERVE_USER", "u@x"), ("OPENOBSERVE_TOKEN", "t-env")]);
    let c = Config::builder()
        .password("pw")
        .build_with_env(&e2)
        .unwrap();
    assert!(c.token().is_none());
    assert_eq!(c.password().unwrap().expose_secret(), "pw");
    let c = Config::builder().build_with_env(&e2).unwrap();
    assert_eq!(c.token().unwrap().expose_secret(), "t-env");
    let _ = Config::from_env();
    let _ = Client::from_env();
}

#[test]
fn config_validation() {
    let cases: Vec<(&str, sdk::Result<Config>)> = vec![
        ("blank org", Config::builder().org(" ").build()),
        ("org with slash", Config::builder().org("a/b").build()),
        ("blank stream", Config::builder().stream("  ").build()),
        (
            "blank traces stream",
            Config::builder().traces_stream("").build(),
        ),
        (
            "password and token",
            Config::builder().user("u").password("p").token("t").build(),
        ),
        (
            "blank token",
            Config::builder().user("u").token("  ").build(),
        ),
        (
            "password without user",
            Config::builder().password("p").build(),
        ),
        ("token without user", Config::builder().token("t").build()),
        (
            "user without credentials",
            Config::builder().user("u").build(),
        ),
    ];
    for (name, result) in cases {
        let err = result.unwrap_err();
        assert_eq!(err.code(), ErrorCode::Config, "{name}");
        assert!(
            err.message().starts_with("invalid configuration"),
            "{name}: {err}"
        );
    }
    // empty secrets count as unset
    assert!(Config::builder().password("").build().is_ok());
}

#[test]
fn config_authorization_forms_and_redaction() {
    let c = Config::builder()
        .user("root@example.com")
        .password("Complexpass#123")
        .build()
        .unwrap();
    assert_eq!(c.authorization().unwrap().expose_secret(), ROOT_AUTH);
    let c = Config::builder()
        .user("sa@example.com")
        .token("sa-token-1234")
        .build()
        .unwrap();
    assert_eq!(
        c.authorization().unwrap().expose_secret(),
        "Basic c2FAZXhhbXBsZS5jb206c2EtdG9rZW4tMTIzNA=="
    );
    // a pre-encoded header needs no user
    let c = Config::builder().token("basic  QUJDRA== ").build().unwrap();
    assert_eq!(c.authorization().unwrap().expose_secret(), "Basic QUJDRA==");
    assert_eq!(c.known_secrets().len(), 2);
    let text = format!(
        "{:?}",
        Config::builder()
            .user("u@x")
            .password("hunter2-secret")
            .build()
            .unwrap()
    );
    assert!(!text.contains("hunter2-secret"), "{text}");
    assert!(text.contains("[REDACTED]"));
    assert_eq!(sdk::normalize_stream_name(" Sdk-Mixed.X "), "sdk_mixed_x");
}

#[tokio::test]
async fn health_ping_version() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/healthz"))
        .respond_with(respond("health.ok"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/config"))
        .and(h("authorization", ROOT_AUTH))
        .respond_with(respond("ping.ok"))
        .mount(&server)
        .await;
    let c = client(&server);
    let health = c.health().await.unwrap();
    assert!(health.healthy);
    assert_eq!(health.status, "ok");
    assert!(c.ping().await.unwrap() < Duration::from_secs(5));
    assert_eq!(c.server_version().await.unwrap(), "v1.1.2");
    let reqs = requests(&server).await;
    assert!(
        header(&reqs[0], "authorization").is_none(),
        "health is anonymous"
    );
    assert!(header(&reqs[1], "user-agent")
        .unwrap()
        .starts_with("grow-codes-openobserve/"));
    assert!(!c.is_closed());
    c.close().await;
    c.close().await;
    assert!(c.is_closed());
    assert_eq!(c.ping().await.unwrap_err().code(), ErrorCode::Cancelled);
}

#[tokio::test]
async fn auth_errors_and_odd_responses() {
    let server = MockServer::start().await;
    Mock::given(path("/healthz"))
        .respond_with(ResponseTemplate::new(503).set_body_json(json!({"status": "starting"})))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/config"))
        .respond_with(respond("ping.401"))
        .mount(&server)
        .await;
    let c = client(&server);
    let health = c.health().await.unwrap();
    assert!(!health.healthy);
    assert_eq!(health.status, "starting");
    let err = c.ping().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Authentication);
    assert!(!err.to_string().contains("Complexpass"), "{err}");
    assert_eq!(
        c.server_version().await.unwrap_err().code(),
        ErrorCode::Authentication
    );

    let server = MockServer::start().await;
    Mock::given(path("/healthz"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(path("/api/acme/config"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"build_type": "oss"})))
        .mount(&server)
        .await;
    let c = Client::new(
        Config::builder()
            .url(server.uri())
            .org("acme")
            .build()
            .unwrap(),
    )
    .unwrap();
    let health = c.health().await.unwrap();
    assert_eq!(health.status, "unhealthy");
    assert_eq!(
        c.server_version().await.unwrap_err().code(),
        ErrorCode::Unsupported
    );

    let server = MockServer::start().await;
    Mock::given(path("/api/default/config"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>"))
        .mount(&server)
        .await;
    let err = client(&server).server_version().await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);

    // unreachable service: health reports it, never fails
    let c = Client::new(
        Config::builder()
            .url(format!("http://127.0.0.1:{}", closed_port()))
            .max_retries(0)
            .build()
            .unwrap(),
    )
    .unwrap();
    assert!(!c.health().await.unwrap().healthy);
}

fn closed_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

#[tokio::test]
async fn log_clients_are_flushed_on_close() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/default/app/_json"))
        .respond_with(respond("ingestJson.ok"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/default/v1/logs"))
        .respond_with(respond("otlpLogs.json"))
        .mount(&server)
        .await;
    let c = client(&server);
    let json_client = c
        .log_client(
            LogClientOptions::default()
                .stream("app")
                .resource_attribute("service.name", "svc")
                .min_severity(sdk::logging::Severity::Debug)
                .redaction(None)
                .batch(sdk::logging::BatchOptions::default().gzip(false)),
        )
        .unwrap();
    let otlp_client = c
        .log_client(LogClientOptions::default().protocol(LogProtocol::Otlp))
        .unwrap();
    json_client.send(LogEntry::from("hello json"));
    otlp_client.send(LogEntry::from("hello otlp"));
    c.close().await;
    assert_eq!(json_client.stats().sent, 1);
    assert_eq!(otlp_client.stats().sent, 1);
    let reqs = requests(&server).await;
    let json_req = reqs
        .iter()
        .find(|r| r.url.path().ends_with("_json"))
        .unwrap();
    let rows = json_body(json_req);
    assert_eq!(rows[0]["body"], "hello json");
    assert_eq!(rows[0]["service_name"], "svc");
    let otlp_req = reqs
        .iter()
        .find(|r| r.url.path().ends_with("v1/logs"))
        .unwrap();
    assert_eq!(header(otlp_req, "stream-name").as_deref(), Some("default"));
    assert_eq!(
        header(otlp_req, "content-encoding").as_deref(),
        Some("gzip")
    );
}

#[tokio::test]
async fn module_accessors() {
    let server = MockServer::start().await;
    let c = client(&server);
    assert_eq!(c.log_query(None).unwrap().stream(), "default");
    assert_eq!(c.log_query(Some("My-App")).unwrap().stream(), "my_app");
    assert!(c.log_query(Some(" ")).is_err());
    assert_eq!(c.trace_query(None).stream(), "default");
    assert_eq!(c.trace_query(Some("Tr")).stream(), "tr");
    assert_eq!(c.traces().stream(), "default");
    let _ = c.log_exporter("s", LogProtocol::Json);
    let q: &dyn LogQueryClient = &c.log_query(None).unwrap();
    let _ = q;
    let t: &dyn TraceQueryClient = &c.traces();
    let _ = t;
    assert!(format!("{:?}", c.admin()).contains("OpenObserveAdmin"));
    assert!(format!("{:?}", c.streams()).contains("OpenObserveStreams"));
    assert!(format!("{:?}", c.logs()).contains("OpenObserveLogs"));
    assert!(!format!("{c:?}").contains("Complexpass"));
}
