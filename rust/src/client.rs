// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! `OpenObserveClient`: entry point of the SDK (SPEC §2.4) and its service modules.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use serde::Deserialize;

use crate::admin::OpenObserveAdmin;
use crate::config::OpenObserveConfig;
use crate::core::lifecycle::CloseGroup;
use crate::core::transport::RequestBuilder;
use crate::core::{Error, HealthStatus, HttpTransport, Method, Result, Secret};
use crate::http::{decode, field, org_path, remap_error, text};
use crate::logging::{
    Attributes, BatchOptions, BatchingLogClient, LogClient, LogExporter, RedactionProcessor,
    Severity,
};
use crate::logs::{
    OpenObserveLogExporter, OpenObserveLogQuery, OpenObserveLogs, OpenObserveOtlpLogExporter,
    OpenObserveStreams,
};
use crate::traces::OpenObserveTraceQuery;

/// Async client for OpenObserve. Cheap to clone (clones share one connection pool).
///
/// Service modules: [`logs`](Self::logs) (ingest and search), [`streams`](Self::streams),
/// [`traces`](Self::traces) (trace queries) and [`admin`](Self::admin); category clients:
/// [`log_client`](Self::log_client), [`log_query`](Self::log_query) and
/// [`trace_query`](Self::trace_query). Cancel a call by dropping its future.
#[derive(Debug, Clone)]
pub struct OpenObserveClient {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: OpenObserveConfig,
    transport: HttpTransport,
    authorization: Option<Secret>,
    on_close: CloseGroup,
}

#[derive(Deserialize)]
struct StatusBody {
    status: Option<String>,
}

/// Wire protocol of a [`OpenObserveClient::log_client`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogProtocol {
    /// `_json` ingestion (default): exact per-stream accepted/rejected counters.
    #[default]
    Json,
    /// OTLP/JSON to `/api/{org}/v1/logs` with the `stream-name` header.
    Otlp,
}

/// Options of [`OpenObserveClient::log_client`].
#[derive(Debug, Clone)]
pub struct LogClientOptions {
    /// Target stream (default: the config's `stream`).
    pub stream: Option<String>,
    /// Wire protocol (default [`LogProtocol::Json`]).
    pub protocol: LogProtocol,
    /// Batch settings.
    pub batch: BatchOptions,
    /// Resource attributes added to every record (`service.name`, …).
    pub resource: Attributes,
    /// Redaction applied before records leave the process (default
    /// [`RedactionProcessor::default`]); `None` disables it.
    pub redaction: Option<RedactionProcessor>,
    /// Minimum severity of loggers made by [`LogClient::logger`] (default `Info`).
    pub min_severity: Severity,
}

impl Default for LogClientOptions {
    fn default() -> Self {
        LogClientOptions {
            stream: None,
            protocol: LogProtocol::Json,
            batch: BatchOptions::default(),
            resource: Attributes::new(),
            redaction: Some(RedactionProcessor::default()),
            min_severity: Severity::Info,
        }
    }
}

impl LogClientOptions {
    /// Sets the target stream.
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }
    /// Sets the protocol.
    pub fn protocol(mut self, protocol: LogProtocol) -> Self {
        self.protocol = protocol;
        self
    }
    /// Sets the batch options.
    pub fn batch(mut self, batch: BatchOptions) -> Self {
        self.batch = batch;
        self
    }
    /// Adds one resource attribute.
    pub fn resource_attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.resource.insert(key.into(), value.into());
        self
    }
    /// Sets (or disables with `None`) the redaction processor.
    pub fn redaction(mut self, redaction: Option<RedactionProcessor>) -> Self {
        self.redaction = redaction;
        self
    }
    /// Sets the minimum logger severity.
    pub fn min_severity(mut self, severity: Severity) -> Self {
        self.min_severity = severity;
        self
    }
}

impl OpenObserveClient {
    /// Creates a client from a configuration.
    pub fn new(config: OpenObserveConfig) -> Result<Self> {
        config.validate()?;
        let transport = HttpTransport::new(config.common())?;
        for secret in config.known_secrets() {
            transport.add_known_secret(&secret);
        }
        let authorization = config.authorization();
        Ok(OpenObserveClient {
            inner: Arc::new(Inner {
                config,
                transport,
                authorization,
                on_close: CloseGroup::new(),
            }),
        })
    }

    /// Creates a client from the process environment.
    pub fn from_env() -> Result<Self> {
        Self::new(OpenObserveConfig::from_env()?)
    }

    /// The configuration in use.
    pub fn config(&self) -> &OpenObserveConfig {
        &self.inner.config
    }

    /// The HTTP transport (for service modules).
    pub fn transport(&self) -> &HttpTransport {
        &self.inner.transport
    }

    /// Registers work that [`OpenObserveClient::close`] runs (flush, stop watchers, …).
    pub fn on_close<F>(&self, f: F)
    where
        F: FnOnce() -> BoxFuture<'static, ()> + Send + 'static,
    {
        self.inner.on_close.register(f);
    }

    /// An authenticated request (`Authorization: Basic …` when credentials are configured).
    pub(crate) fn request(&self, method: Method, path: &str) -> RequestBuilder<'_> {
        let req = self.transport().request(method, path);
        match &self.inner.authorization {
            Some(auth) => req.header("authorization", auth.expose_secret().to_string()),
            None => req,
        }
    }

    /// `/api/{org}/<segments…>` (URL-encoded).
    pub(crate) fn org_path(&self, segments: &[&str]) -> String {
        org_path(self.config().org(), segments)
    }

    /// Logs ingestion and search.
    pub fn logs(&self) -> OpenObserveLogs {
        OpenObserveLogs::new(self.clone())
    }

    /// Stream management.
    pub fn streams(&self) -> OpenObserveStreams {
        OpenObserveStreams::new(self.clone())
    }

    /// Trace queries on the config's traces stream.
    pub fn traces(&self) -> OpenObserveTraceQuery {
        OpenObserveTraceQuery::new(self.clone(), None)
    }

    /// Organizations, service accounts and passcode.
    pub fn admin(&self) -> OpenObserveAdmin {
        OpenObserveAdmin::new(self.clone())
    }

    /// Unauthenticated liveness: `GET /healthz` → `{"status":"ok"}`. Never fails for an
    /// unhealthy or unreachable service; only `CANCELLED` and `CONFIG` are returned as errors.
    pub async fn health(&self) -> Result<HealthStatus> {
        let resp = self
            .transport()
            .get("/healthz")
            .operation("health")
            .ok_status(500)
            .ok_status(503)
            .send()
            .await;
        let resp = match resp {
            Ok(resp) => resp,
            Err(err) => return HealthStatus::from_error(err),
        };
        let word = resp.json::<StatusBody>().ok().and_then(|b| b.status);
        Ok(if resp.status() < 300 {
            HealthStatus::healthy(word.unwrap_or_else(|| "ok".to_owned()))
        } else {
            HealthStatus::unhealthy(word.unwrap_or_else(|| "unhealthy".to_owned()))
                .with_detail("httpStatus", resp.status().to_string())
        })
    }

    /// Authenticated round trip `GET /api/{org}/config`; returns the latency.
    /// `AUTHENTICATION` on bad or missing credentials.
    pub async fn ping(&self) -> Result<Duration> {
        let started = Instant::now();
        self.request(Method::GET, &self.org_path(&["config"]))
            .operation("ping")
            .send()
            .await
            .map_err(remap_error)?;
        Ok(started.elapsed())
    }

    /// Server version from the authenticated `GET /api/{org}/config` (e.g. `v1.1.2`);
    /// `UNSUPPORTED` when the response has none.
    pub async fn server_version(&self) -> Result<String> {
        let resp = self
            .request(Method::GET, &self.org_path(&["config"]))
            .operation("serverVersion")
            .send()
            .await
            .map_err(remap_error)?;
        let body = decode(&resp, "serverVersion")?;
        text(field(&body, "version")).ok_or_else(|| {
            Error::unsupported("serverVersion: the service does not report a version")
        })
    }

    /// The exporter used by [`log_client`](Self::log_client) for one stream.
    pub fn log_exporter(&self, stream: &str, protocol: LogProtocol) -> Arc<dyn LogExporter> {
        match protocol {
            LogProtocol::Json => Arc::new(OpenObserveLogExporter::new(self.logs(), stream)),
            LogProtocol::Otlp => Arc::new(OpenObserveOtlpLogExporter::new(self.logs(), stream)),
        }
    }

    /// A batching [`LogClient`] that exports to one stream. It is flushed and closed by
    /// [`close`](Self::close). Must be called inside a tokio runtime.
    pub fn log_client(&self, opts: LogClientOptions) -> Result<BatchingLogClient> {
        let stream = opts
            .stream
            .unwrap_or_else(|| self.config().stream().to_string());
        let client = BatchingLogClient::builder(self.log_exporter(&stream, opts.protocol))
            .batch_options(opts.batch)
            .resource(opts.resource)
            .redaction(opts.redaction)
            .min_severity(opts.min_severity)
            .build()?;
        let closing = client.clone();
        self.on_close(move || {
            Box::pin(async move {
                let _ = closing.close().await;
            })
        });
        Ok(client)
    }

    /// A [`LogQueryClient`](crate::logging::LogQueryClient) on a stream (default: the
    /// config's `stream`).
    pub fn log_query(&self, stream: Option<&str>) -> Result<OpenObserveLogQuery> {
        OpenObserveLogQuery::new(
            self.clone(),
            stream.unwrap_or_else(|| self.config().stream()),
        )
    }

    /// A [`TraceQueryClient`](crate::tracing::TraceQueryClient) on a traces stream (default:
    /// the config's `traces_stream`).
    pub fn trace_query(&self, stream: Option<&str>) -> OpenObserveTraceQuery {
        OpenObserveTraceQuery::new(self.clone(), stream)
    }

    /// Flushes and closes every log client made by [`log_client`](Self::log_client), then
    /// closes the transport. Idempotent.
    pub async fn close(&self) {
        self.inner.on_close.close().await;
        self.inner.transport.close();
    }

    /// Whether [`OpenObserveClient::close`] was called.
    pub fn is_closed(&self) -> bool {
        self.inner.transport.is_closed()
    }
}
