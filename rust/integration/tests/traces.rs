// Service-owned.
//! Traces against the real service: spans exported by `init_tracing` (OTLP/HTTP protobuf and
//! JSON) are read back with every `TraceQueryClient` call.

use std::time::{Duration, SystemTime};

use grow_openobserve as sdk;
use grow_openobserve_integration as support;
use sdk::logging::Attributes;
use sdk::traces::{init_tracing, OpenObserveTracingConfig, TraceWindowOptions};
use sdk::tracing::{
    current_trace_id, with_span, DependencyOptions, Protocol, SpanKind as Kind, SpanOptions,
    StatusCode, TraceQueryClient, TraceSearchQuery, TracingConfig, WaitForTraceOptions,
};
use sdk::{ErrorCode, StreamType, Timestamp};
use serde_json::json;
use support::*;

#[derive(Debug)]
struct Boom;

impl std::fmt::Display for Boom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("boom")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn traces_round_trip() {
    let c = root_client();
    let stream = unique("traces");
    let service = format!("rs-svc-{}", sdk::testing::run_id());
    let handle = c
        .init_tracing(
            TracingConfig::new(&service)
                .with_service_version("1.2.3")
                .with_environment("test")
                .with_set_global(false),
            Some(&stream),
        )
        .unwrap();
    let tracer = handle.tracer("rs-integration", None);
    let mut attrs = Attributes::new();
    attrs.insert("http.method".into(), json!("GET"));
    let trace_id = with_span(
        "GET /checkout",
        SpanOptions::default()
            .tracer(tracer.clone())
            .kind(sdk::opentelemetry::trace::SpanKind::Server)
            .attributes(&attrs),
        async {
            let id = current_trace_id().unwrap();
            let mut db = Attributes::new();
            db.insert("db.system".into(), json!("postgresql"));
            with_span(
                "db.query",
                SpanOptions::default()
                    .tracer(tracer.clone())
                    .attributes(&db),
                async {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    Ok::<_, Boom>(())
                },
            )
            .await?;
            let _ = with_span(
                "fails",
                SpanOptions::default().tracer(tracer.clone()),
                async { Err::<(), _>(Boom) },
            )
            .await;
            Ok::<_, Boom>(id)
        },
    )
    .await
    .unwrap();
    handle
        .force_flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();

    let traces = c.trace_query(Some(&stream));
    let trace = traces
        .wait_for_trace(
            &trace_id,
            WaitForTraceOptions::default()
                .min_spans(3)
                .timeout(Duration::from_secs(30)),
        )
        .await
        .unwrap();
    assert_eq!(trace.trace_id, trace_id);
    assert_eq!(trace.spans.len(), 3);
    let root = trace
        .spans
        .iter()
        .find(|s| s.name == "GET /checkout")
        .unwrap();
    assert!(root.parent_span_id.is_none());
    assert_eq!(root.kind, Kind::Server);
    assert_eq!(root.service_name, service);
    assert_eq!(root.attributes["http_method"], "GET");
    let db = trace.spans.iter().find(|s| s.name == "db.query").unwrap();
    assert_eq!(db.parent_span_id.as_deref(), Some(root.span_id.as_str()));
    assert!(db.duration_micros >= 30_000);
    let failed = trace.spans.iter().find(|s| s.name == "fails").unwrap();
    assert_eq!(failed.status.code, StatusCode::Error);
    assert!(failed.events.iter().any(|e| e.name == "exception"));
    assert_eq!(trace.children_of("GET /checkout").len(), 2);

    let window = TraceWindowOptions::new(
        Some((SystemTime::now() - Duration::from_secs(600)).into()),
        None,
    );
    assert!(traces
        .get_trace_in(&trace_id, window)
        .await
        .unwrap()
        .is_some());
    // v1.1.2 resolves `details` through its trace index, so an old window still finds the
    // trace; only the call with an explicit window is asserted.
    let _ = traces
        .get_trace_in(
            &trace_id,
            TraceWindowOptions::new(
                Some(Timestamp::Micros(1_000_000)),
                Some(Timestamp::Micros(2_000_000)),
            ),
        )
        .await
        .unwrap();
    assert!(traces
        .get_trace("0af7651916cd43dd8448eb211c80319c")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        traces.get_trace("xyz").await.unwrap_err().code(),
        ErrorCode::Validation
    );

    let all = traces
        .search_traces(&TraceSearchQuery::default())
        .await
        .unwrap();
    let summary = all
        .iter()
        .find(|s| s.trace_id == trace_id)
        .expect("summary");
    assert_eq!(summary.root_name.as_deref(), Some("GET /checkout"));
    assert_eq!(summary.span_count, 3);
    let by_service = traces
        .search_traces(&TraceSearchQuery::service(&service).operation("db.query"))
        .await
        .unwrap();
    assert!(by_service.iter().any(|s| s.trace_id == trace_id));
    let by_tag = traces
        .search_traces(&TraceSearchQuery::default().tag("db.system", "postgresql"))
        .await
        .unwrap();
    assert!(by_tag.iter().any(|s| s.trace_id == trace_id));
    let mut slow = TraceSearchQuery::service(&service);
    slow.min_duration = Some(Duration::from_secs(3600));
    assert!(traces.search_traces(&slow).await.unwrap().is_empty());

    assert_eq!(traces.list_services().await.unwrap(), vec![service.clone()]);
    assert_eq!(
        traces.list_services_in(window).await.unwrap(),
        vec![service.clone()]
    );
    let ops = traces.list_operations(&service).await.unwrap();
    assert_eq!(ops, ["GET /checkout", "db.query", "fails"]);
    assert_eq!(
        traces.list_operations_in(&service, window).await.unwrap(),
        ops
    );
    let range = traces.trace_time_range(&trace_id).await.unwrap();
    assert!(range.found);
    assert_eq!(range.stream.as_deref(), Some(stream.as_str()));
    assert!(
        !traces
            .trace_time_range("0af7651916cd43dd8448eb211c80319c")
            .await
            .unwrap()
            .found
    );
    assert_eq!(
        traces
            .get_dependencies(DependencyOptions::default())
            .await
            .unwrap_err()
            .code(),
        ErrorCode::Unsupported
    );
    // a missing traces stream reads as empty
    let missing = c.trace_query(Some("rs_no_such_traces"));
    assert!(missing.list_services().await.unwrap().is_empty());
    assert!(missing
        .search_traces(&TraceSearchQuery::default())
        .await
        .unwrap()
        .is_empty());
    assert!(missing.get_trace(&trace_id).await.unwrap().is_none());
    handle.shutdown().await.unwrap();

    // OTLP/JSON ("native") through the free init_tracing function
    let json_service = format!("{service}-json");
    let native = init_tracing(
        OpenObserveTracingConfig::new(
            TracingConfig::new(&json_service)
                .with_set_global(false)
                .with_protocol(Protocol::Native),
        )
        .openobserve(c.config().clone())
        .stream(&stream),
    )
    .unwrap();
    let json_tracer = native.tracer("rs-json", Some("1.0"));
    let json_trace = with_span(
        "json-span",
        SpanOptions::default().tracer(json_tracer),
        async { Ok::<_, Boom>(current_trace_id().unwrap()) },
    )
    .await
    .unwrap();
    native
        .force_flush(Some(Duration::from_secs(10)))
        .await
        .unwrap();
    let t = c.traces();
    assert_eq!(
        t.stream(),
        std::env::var("OPENOBSERVE_TRACES_STREAM").unwrap_or_else(|_| "default".into())
    );
    let t = traces
        .wait_for_trace(&json_trace, WaitForTraceOptions::default())
        .await
        .unwrap();
    assert_eq!(t.spans[0].service_name, json_service);
    native.shutdown().await.unwrap();

    c.streams()
        .delete_stream(&stream, StreamType::Traces)
        .await
        .unwrap();
}
