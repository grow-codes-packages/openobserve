// Created by sdk-kit 0.1.0; owned by this repository.
//! Conformance scenarios (conformance/scenarios) against the real service with the core,
//! logging and tracing factories. Declared capabilities:
//!   core: version · logging: query · tracing: query, services
//! Not declared: `dependencies` (enterprise-only in OpenObserve, see docs/compatibility.md).

use std::sync::Arc;

use grow_openobserve as sdk;
use grow_openobserve_integration as support;
use sdk::testing::logging::{LoggingHandler, LoggingTarget};
use sdk::testing::tracing::{TracingHandler, TracingTarget};
use sdk::testing::{run_id, ConformanceRunner, CoreHandler};
use sdk::tracing::TracingConfig;
use sdk::{LogClientOptions, OpenObserveClient as Client};
use support::{conformance_dir, load_bootstrap_env};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance() {
    load_bootstrap_env();
    let run = run_id();
    let service = format!("rs-conformance-{run}");
    let log_stream = format!("rs_conf_{run}");
    let trace_stream = format!("rs_conf_traces_{run}");

    let logging = {
        let stream = log_stream.clone();
        LoggingHandler::new(move |batch| {
            let stream = stream.clone();
            async move {
                let client = Client::from_env()?;
                let log_client =
                    client.log_client(LogClientOptions::default().stream(&stream).batch(batch))?;
                Ok(LoggingTarget {
                    client: Arc::new(log_client),
                    query: Some(Arc::new(client.log_query(Some(&stream))?)),
                })
            }
        })
    };
    let tracing = {
        let (service, stream) = (service.clone(), trace_stream.clone());
        TracingHandler::new(move || {
            let (service, stream) = (service.clone(), stream.clone());
            async move {
                let client = Client::from_env()?;
                let handle = client.init_tracing(
                    TracingConfig::new(service).with_set_global(false),
                    Some(&stream),
                )?;
                Ok(TracingTarget {
                    handle,
                    query: Some(Arc::new(client.trace_query(Some(&stream)))),
                })
            }
        })
    };
    let report = ConformanceRunner::new(conformance_dir())
        .capabilities(["version", "query", "services"])
        .var("serviceName", service)
        .handler(CoreHandler::new(Client::from_env))
        .handler(logging)
        .handler(tracing)
        .run()
        .await
        .expect("scenarios load");
    println!("{report}");
    report.assert_success();
    assert_eq!(report.skipped(), 0, "{report}");
    assert!(report.passed() >= 6, "{report}");

    let cleanup = Client::from_env().unwrap();
    let _ = cleanup
        .streams()
        .delete_stream(&log_stream, sdk::StreamType::Logs)
        .await;
    let _ = cleanup
        .streams()
        .delete_stream(&trace_stream, sdk::StreamType::Traces)
        .await;
}
