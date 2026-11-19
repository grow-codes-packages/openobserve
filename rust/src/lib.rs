// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! # grow-openobserve
//!
//! Rust SDK for [OpenObserve](https://grow-codes-packages.github.io/openobserve/) by Grow Codes.
//!
//! ```no_run
//! # async fn demo() -> grow_openobserve::Result<()> {
//! use grow_openobserve::{OpenObserveClient, OpenObserveConfig};
//!
//! let client = OpenObserveClient::new(OpenObserveConfig::from_env()?)?;
//! let health = client.health().await?;
//! println!("healthy: {}", health.healthy);
//! println!("server: {}", client.server_version().await?);
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! Modules: [`logs`] (ingest, search, streams and the logging adapters), [`traces`]
//! ([`traces::init_tracing`] and the trace query client), [`admin`] (organizations, service
//! accounts, passcode), plus the category libraries [`logging`] and [`tracing`].
//!
//! Features: `blocking` (a blocking client), `testing` (in-memory fakes and the conformance
//! runner)
//! , `tracing-layer` and `log-bridge` (logging adapters, on by default)
//! , `tower` and `reqwest-middleware` (tracing instrumentation)
//! .
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod core;

pub mod admin;
mod client;
mod config;
mod http;
pub mod logs;
pub mod traces;

#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;

#[cfg(feature = "testing")]
#[cfg_attr(docsrs, doc(cfg(feature = "testing")))]
pub mod testing;

pub mod logging;
pub mod tracing;

pub use crate::core::{Error, ErrorCode, HealthStatus, Result, RetryPolicy, Secret};

pub use crate::config::DEFAULT_URL;

pub use crate::client::{LogClientOptions, LogProtocol, OpenObserveClient};

pub use crate::config::{normalize_stream_name, DEFAULT_ORG, DEFAULT_STREAM, ENV_PREFIX};

pub use crate::http::{
    flatten_key, micros_to_system_time, ns_to_micros, sql_ident, sql_string, system_time_to_micros,
    JsonObject, StreamType, Timestamp,
};

pub use crate::traces::init_tracing;

/// The OpenTelemetry API crate this SDK is built on (use it for version-matched types such as
/// `opentelemetry::trace::SpanKind`).
pub use opentelemetry;
/// The OpenTelemetry SDK crate this SDK is built on.
pub use opentelemetry_sdk;

pub use crate::config::OpenObserveConfig;

pub use crate::config::OpenObserveConfigBuilder;

/// Service-prefixed alias of [`Error`].
pub type OpenObserveError = Error;
