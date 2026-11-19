// Service-owned.
//! Traces module: trace queries against recorded OpenObserve responses, span mapping and the
//! `init_tracing` exporter setup (spans exported to a mock OTLP endpoint).

mod support;

use std::time::Duration;

use grow_openobserve as sdk;
use opentelemetry::trace::{Span as _, Tracer as _};
use sdk::core::{ErrorCode, TlsConfig};
use sdk::traces::*;
use sdk::tracing::{
    DependencyOptions, Protocol, SpanKind, StatusCode, TraceQueryClient, TraceSearchQuery,
    TracingConfig,
};
use sdk::{OpenObserveConfig as Config, Timestamp};
use serde_json::{json, Value};
use support::*;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TRACE: &str = "038a1ed5af73678e47809f854dabaae9";
const DETAILS: &str = "/api/default/default/traces/038a1ed5af73678e47809f854dabaae9/details";

#[tokio::test]
async fn get_trace_maps_spans() {
    let server = MockServer::start().await;
    Mock::given(path(DETAILS))
        .respond_with(respond("getTrace.details"))
        .mount(&server)
        .await;
    let traces = client(&server).traces();
    let trace = traces
        .get_trace(&TRACE.to_uppercase())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trace.trace_id, TRACE);
    assert_eq!(trace.spans.len(), 3);
    let root = trace
        .spans
        .iter()
        .find(|s| s.parent_span_id.is_none())
        .unwrap();
    assert_eq!(root.name, "GET /checkout");
    assert_eq!(root.kind, SpanKind::Server);
    assert_eq!(root.status.code, StatusCode::Ok);
    assert_eq!(root.start_micros, 1_795_033_200_603_151);
    assert_eq!(root.duration_micros, 50_000);
    assert_eq!(root.attributes["http_status_code"], "200");
    assert!(!root.attributes.contains_key("_timestamp"));
    let db = trace.spans.iter().find(|s| s.name == "db.query").unwrap();
    assert_eq!(db.parent_span_id.as_deref(), Some("b7ad6b7169203331"));
    assert_eq!(db.events[0].name, "cache.miss");
    assert_eq!(db.events[0].attributes["key"], "u1");
    assert_eq!(db.events[0].time_micros, 1_795_033_200_623_151);
    assert_eq!(db.links[0].trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(db.links[0].attributes["kind"], "follows");
    assert!(!db
        .attributes
        .keys()
        .any(|k| k.starts_with("o2_db_") || k.starts_with("infer_service_")));
    let internal = trace.spans.iter().find(|s| s.name == "row.decode").unwrap();
    assert_eq!(internal.kind, SpanKind::Internal);
    assert_eq!(internal.status.code, StatusCode::Unset);
    let req = &requests(&server).await[0];
    assert_eq!(query(req, "size").as_deref(), Some("1000"));
    let start: i64 = query(req, "start_time").unwrap().parse().unwrap();
    let end: i64 = query(req, "end_time").unwrap().parse().unwrap();
    assert!(end - start >= 7 * 24 * 3600 * 1_000_000);
}

#[tokio::test]
async fn get_trace_paginates_and_handles_missing() {
    let server = MockServer::start().await;
    Mock::given(path(DETAILS))
        .and(query_param_is_missing("after_span_id"))
        .respond_with(respond("getTrace.page1"))
        .mount(&server)
        .await;
    let mut last = body("getTrace.details");
    last["has_more"] = json!(false);
    Mock::given(path(DETAILS))
        .and(query_param("after_span_id", "b7ad6b7169203331"))
        .and(query_param("after_start_time", "1795033200603151382"))
        .respond_with(ResponseTemplate::new(200).set_body_json(last))
        .mount(&server)
        .await;
    let other = "/api/default/default/traces/0af7651916cd43dd8448eb211c80319c/details";
    Mock::given(path(other))
        .respond_with(respond("getTrace.not_found"))
        .mount(&server)
        .await;
    Mock::given(path(
        "/api/default/nostream/traces/038a1ed5af73678e47809f854dabaae9/details",
    ))
    .respond_with(respond("getTrace.400_stream_not_found"))
    .mount(&server)
    .await;
    let c = client(&server);
    let trace = c
        .traces()
        .get_trace_in(
            TRACE,
            TraceWindowOptions::new(Some(Timestamp::Micros(1)), Some(Timestamp::Micros(2))),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(trace.spans.len(), 3, "deduplicated across pages");
    assert!(c
        .traces()
        .get_trace("0af7651916cd43dd8448eb211c80319c")
        .await
        .unwrap()
        .is_none());
    assert!(c
        .trace_query(Some("nostream"))
        .get_trace(TRACE)
        .await
        .unwrap()
        .is_none());
    let err = c.traces().get_trace("not-hex").await.unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
    let err = c
        .traces()
        .get_trace_in(
            TRACE,
            TraceWindowOptions::new(Some(Timestamp::Micros(5)), Some(Timestamp::Micros(1))),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Validation);
}

#[tokio::test]
async fn search_list_time_range() {
    let server = MockServer::start().await;
    Mock::given(path("/api/default/default/traces/latest"))
        .and(query_param_is_missing("filter"))
        .respond_with(respond("searchTraces.latest"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/default/traces/latest"))
        .and(query_param("filter", "service_name = 'svc'"))
        .respond_with(respond("searchTraces.filter_service"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/default/traces/latest"))
        .respond_with(respond("searchTraces.filter_no_match"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/gone/traces/latest"))
        .respond_with(respond("search.400_stream_not_found"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("listServices.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("listOperations.ok"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(path("/api/default/_search"))
        .respond_with(respond("search.400_stream_not_found"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/traces/time_range"))
        .and(query_param("trace_id", TRACE))
        .respond_with(respond("traceTimeRange.ok"))
        .mount(&server)
        .await;
    Mock::given(path("/api/default/traces/time_range"))
        .respond_with(respond("traceTimeRange.not_found"))
        .mount(&server)
        .await;
    let c = client(&server);
    let traces = c.traces();
    let all = traces
        .search_traces(&TraceSearchQuery::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    let s = all.iter().find(|s| s.trace_id == TRACE).unwrap();
    assert_eq!(s.duration_micros, 50_000);
    assert!(s.span_count >= 1);
    assert!(s.root_service_name.is_some() && s.root_name.is_some());
    assert_eq!(s.start_micros, 1_795_033_200_603_151);
    let filtered = traces
        .search_traces(&TraceSearchQuery::service("svc"))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    let mut q = TraceSearchQuery::service("x")
        .operation("op")
        .tag("db.system", "pg")
        .limit(1);
    q.min_duration = Some(Duration::from_millis(40));
    q.max_duration = Some(Duration::from_micros(90_000));
    assert!(traces.search_traces(&q).await.unwrap().is_empty());
    assert!(c
        .trace_query(Some("gone"))
        .search_traces(&TraceSearchQuery::default())
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        traces
            .search_traces(&TraceSearchQuery::default().limit(0))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Validation
    );
    let services = traces.list_services().await.unwrap();
    assert_eq!(services.len(), 2);
    assert!(services.windows(2).all(|w| w[0] <= w[1]));
    let ops = traces.list_operations("sdk-a").await.unwrap();
    assert_eq!(ops.len(), 3);
    assert!(
        traces.list_services().await.unwrap().is_empty(),
        "missing stream → empty"
    );
    let range = traces.trace_time_range(TRACE).await.unwrap();
    assert!(range.found);
    assert_eq!(range.stream.as_deref(), Some("sdk_traces_6moiazbq"));
    assert_eq!(range.start_micros, Some(1_795_033_200_603_151));
    let missing = traces
        .trace_time_range("0af7651916cd43dd8448eb211c80319c")
        .await
        .unwrap();
    assert_eq!(missing, TraceTimeRange::default());
    let err = traces
        .get_dependencies(DependencyOptions::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), ErrorCode::Unsupported);
    let reqs = requests(&server).await;
    let filter = reqs
        .iter()
        .filter_map(|r| query(r, "filter"))
        .find(|f| f.contains("operation_name"))
        .unwrap();
    assert_eq!(
        filter,
        "service_name = 'x' AND operation_name = 'op' AND db_system = 'pg' AND duration >= 40000 AND duration <= 90000"
    );
    let list_sql = reqs
        .iter()
        .filter(|r| r.url.path() == "/api/default/_search")
        .map(|r| json_body(r)["query"]["sql"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        list_sql[0],
        "SELECT DISTINCT service_name FROM \"default\" ORDER BY service_name"
    );
    assert_eq!(
        list_sql[1],
        "SELECT DISTINCT operation_name FROM \"default\" WHERE service_name = 'sdk-a' ORDER BY operation_name"
    );
}

#[test]
fn mapping_edge_cases() {
    let row = json!({
        "trace_id": "AB", "span_id": "CD", "span_kind": 4, "status_code": 2,
        "start_time": 5_000_000, "end_time": 1_000, "duration": 77,
        "events": [{"name": "e", "_timestamp": "3000", "n": {"a": 1}}],
        "links": "not json", "extra": null, "list": [1, 2],
    });
    let span = row_to_span(row.as_object().unwrap());
    assert_eq!(span.kind, SpanKind::Producer);
    assert_eq!(span.status.code, StatusCode::Error);
    assert_eq!(
        span.duration_micros, 77,
        "end before start falls back to duration"
    );
    assert_eq!(span.events[0].time_micros, 3);
    assert_eq!(span.events[0].attributes["n"], "{\"a\":1}");
    assert!(span.links.is_empty());
    assert_eq!(span.attributes["list"], "[1,2]");
    assert!(!span.attributes.contains_key("extra"));
    let span = row_to_span(
        json!({"links": [{"context": {"trace_id": "AA", "span_id": "BB"}}]})
            .as_object()
            .unwrap(),
    );
    assert_eq!(span.kind, SpanKind::Unspecified);
    assert_eq!(span.links[0].trace_id, "aa");
    assert_eq!(span.links[0].span_id, "bb");
    let span = row_to_span(
        json!({"span_status": "ok", "span_kind": "0"})
            .as_object()
            .unwrap(),
    );
    assert_eq!(span.status.code, StatusCode::Ok);
    assert_eq!(span.kind, SpanKind::Unspecified);
    let s = hit_to_summary(&json!({"trace_id": "X", "spans": []}));
    assert_eq!((s.trace_id.as_str(), s.span_count), ("x", 0));
    let mut q = TraceSearchQuery::default();
    q.tags.insert(String::new(), "v".into());
    assert_eq!(trace_filter(&q).unwrap_err().code(), ErrorCode::Validation);
    assert_eq!(trace_filter(&TraceSearchQuery::default()).unwrap(), "");
    assert_eq!(
        trace_filter(&TraceSearchQuery::service("o'neil")).unwrap(),
        "service_name = 'o''neil'"
    );
}

#[test]
fn endpoints_and_defaults() {
    let c = Config::builder()
        .url("http://oo:5080/base/")
        .org("acme.org")
        .user("root@example.com")
        .password("Complexpass#123")
        .traces_stream("Tr")
        .build()
        .unwrap();
    assert_eq!(
        traces_endpoint(&c),
        "http://oo:5080/base/api/acme.org/v1/traces"
    );
    assert_eq!(grpc_endpoint(&c), "http://oo:5081");
    let other = Config::builder().url("https://oo:9000").build().unwrap();
    assert_eq!(grpc_endpoint(&other), "https://oo:9000");
    let v6 = Config::builder().url("http://[::1]/").build().unwrap();
    assert_eq!(grpc_endpoint(&v6), "http://[::1]:5081");
    assert_eq!(OPENOBSERVE_GRPC_PORT, 5081);

    let t = openobserve_tracing_defaults(TracingConfig::new("svc"), &c, None).unwrap();
    assert_eq!(
        t.endpoint.as_deref(),
        Some("http://oo:5080/base/api/acme.org/v1/traces")
    );
    assert_eq!(t.headers["stream-name"], "tr");
    assert_eq!(t.headers["authorization"], ROOT_AUTH);
    assert!(!t.headers.contains_key("organization"));
    let t = openobserve_tracing_defaults(
        TracingConfig::new("svc")
            .with_protocol(Protocol::Grpc)
            .with_header("Stream-Name", "mine")
            .with_header("Authorization", "Basic custom"),
        &c,
        Some("ignored"),
    )
    .unwrap();
    assert_eq!(t.endpoint.as_deref(), Some("http://oo:5081"));
    assert_eq!(t.headers["Stream-Name"], "mine");
    assert_eq!(t.headers["Authorization"], "Basic custom");
    assert!(!t.headers.contains_key("stream-name"));
    assert_eq!(t.headers["organization"], "acme.org");
    let anon = Config::builder().build().unwrap();
    let t = openobserve_tracing_defaults(
        TracingConfig::new("svc").with_endpoint("http://x/v1/traces"),
        &anon,
        Some("S"),
    )
    .unwrap();
    assert_eq!(t.endpoint.as_deref(), Some("http://x/v1/traces"));
    assert_eq!(t.headers["stream-name"], "s");
    assert!(!t.headers.contains_key("authorization"));
    assert!(openobserve_tracing_defaults(TracingConfig::new("svc"), &anon, Some(" ")).is_err());
}

#[tokio::test]
async fn exporters_for_every_protocol() {
    let ca = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/ca.pem");
    let cert = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/client.pem");
    let key = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/client.key");
    let c = Config::builder()
        .url("https://oo:5080")
        .proxy("http://proxy:3128")
        .build()
        .unwrap();
    let base = openobserve_tracing_defaults(TracingConfig::new("svc"), &c, None).unwrap();
    assert!(create_span_exporter(&base, &c).is_ok());
    let native = base
        .clone()
        .with_protocol(Protocol::Native)
        .with_compression(sdk::tracing::Compression::None)
        .with_tls(TlsConfig {
            ca_cert_path: Some(ca.into()),
            ca_cert_pem: Some(std::fs::read_to_string(ca).unwrap()),
            client_cert_path: Some(cert.into()),
            client_key_path: Some(key.into()),
            insecure_skip_verify: true,
        });
    assert!(create_span_exporter(&native, &c).is_ok());
    let grpc = openobserve_tracing_defaults(
        TracingConfig::new("svc").with_protocol(Protocol::Grpc),
        &Config::builder().build().unwrap(),
        None,
    )
    .unwrap();
    assert!(create_span_exporter(&grpc, &c).is_ok());
    let mut no_endpoint = base.clone();
    no_endpoint.endpoint = None;
    assert!(create_span_exporter(&no_endpoint, &c).is_ok());
    for tls in [
        TlsConfig {
            ca_cert_path: Some("/nonexistent/ca.pem".into()),
            ..Default::default()
        },
        TlsConfig {
            ca_cert_pem: Some("not pem".into()),
            ..Default::default()
        },
        TlsConfig {
            client_cert_path: Some("/nonexistent/c.pem".into()),
            client_key_path: Some(key.into()),
            ..Default::default()
        },
        TlsConfig {
            client_cert_path: Some(ca.into()),
            client_key_path: Some(ca.into()),
            ..Default::default()
        },
    ] {
        let cfg = base.clone().with_tls(tls);
        let err = create_span_exporter(&cfg, &c).unwrap_err();
        assert_eq!(err.code(), ErrorCode::Config, "{err}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_tracing_exports_to_openobserve() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/default/v1/traces"))
        .respond_with(
            ResponseTemplate::new(200).insert_header("content-type", "application/x-protobuf"),
        )
        .mount(&server)
        .await;
    let c = client(&server);
    let handle = c
        .init_tracing(
            TracingConfig::new("svc").with_set_global(false),
            Some("My-Traces"),
        )
        .unwrap();
    let tracer = handle.tracer("test", None);
    let mut span = tracer.start("work");
    span.end();
    handle
        .force_flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    let native = init_tracing(
        OpenObserveTracingConfig::new(
            TracingConfig::new("svc-json")
                .with_set_global(false)
                .with_protocol(Protocol::Native),
        )
        .openobserve(config(&server))
        .stream("json"),
    )
    .unwrap();
    native.tracer("test", Some("1.0")).start("json-span").end();
    native
        .force_flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    handle.shutdown().await.unwrap();
    native.shutdown().await.unwrap();
    let reqs = requests(&server).await;
    let proto = reqs
        .iter()
        .find(|r| header(r, "stream-name").as_deref() == Some("my_traces"))
        .expect("protobuf export");
    assert_eq!(header(proto, "authorization").as_deref(), Some(ROOT_AUTH));
    assert_eq!(
        header(proto, "content-type").as_deref(),
        Some("application/x-protobuf")
    );
    assert!(header(proto, "user-agent")
        .unwrap()
        .starts_with("grow-codes-openobserve/"));
    let json_req = reqs
        .iter()
        .find(|r| header(r, "stream-name").as_deref() == Some("json"))
        .expect("json export");
    assert_eq!(
        header(json_req, "content-type").as_deref(),
        Some("application/json")
    );
    let body: Value = if header(json_req, "content-encoding").as_deref() == Some("gzip") {
        use std::io::Read;
        let mut s = String::new();
        flate2::read::GzDecoder::new(&json_req.body[..])
            .read_to_string(&mut s)
            .unwrap();
        serde_json::from_str(&s).unwrap()
    } else {
        json_body(json_req)
    };
    assert!(body.to_string().contains("json-span"));
    // from TracingConfig directly (connection from the environment)
    let _ = init_tracing(TracingConfig::new("env").with_set_global(false));
}
