// Service-owned. init_tracing for OpenObserve: the official OTLP exporters pointed at
// `/api/{org}/v1/traces` with Basic auth and the `stream-name` header (API.md §5).

use std::collections::HashMap;

use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};

use crate::config::OpenObserveConfig;
use crate::core::transport::default_user_agent;
use crate::core::{Error, Result, TlsConfig};
use crate::http::{encode, stream_name};
use crate::tracing::{
    init_tracing_with, otlp_exporter, Compression, Protocol, TracingConfig, TracingHandle,
};
use crate::OpenObserveClient;

/// Default OTLP/gRPC port of OpenObserve.
pub const OPENOBSERVE_GRPC_PORT: u16 = 5081;

/// [`TracingConfig`] plus where to send the spans.
#[derive(Debug, Clone)]
pub struct OpenObserveTracingConfig {
    /// OpenTelemetry settings; an explicit `endpoint`, `protocol` or header wins over the
    /// OpenObserve defaults.
    pub tracing: TracingConfig,
    /// OpenObserve connection (URL, org, credentials, traces stream). Default:
    /// [`OpenObserveConfig::from_env`].
    pub openobserve: Option<OpenObserveConfig>,
    /// Traces stream (header `stream-name`). Default: the config's `traces_stream`.
    pub stream: Option<String>,
}

impl OpenObserveTracingConfig {
    /// Settings for `tracing` with the connection from the environment.
    pub fn new(tracing: TracingConfig) -> Self {
        OpenObserveTracingConfig {
            tracing,
            openobserve: None,
            stream: None,
        }
    }
    /// Sets the OpenObserve connection.
    pub fn openobserve(mut self, config: OpenObserveConfig) -> Self {
        self.openobserve = Some(config);
        self
    }
    /// Sets the traces stream.
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }
}

impl From<TracingConfig> for OpenObserveTracingConfig {
    fn from(tracing: TracingConfig) -> Self {
        OpenObserveTracingConfig::new(tracing)
    }
}

/// The OTLP/HTTP traces endpoint: `<url>/api/{org}/v1/traces`.
pub fn traces_endpoint(config: &OpenObserveConfig) -> String {
    let base = config.url().as_str().trim_end_matches('/');
    format!("{base}/api/{}/v1/traces", encode(config.org()))
}

/// The default OTLP/gRPC endpoint: the URL's host with port 5081 when it uses 5080 (or none).
pub fn grpc_endpoint(config: &OpenObserveConfig) -> String {
    let url = config.url();
    let host = url.host_str().unwrap_or("127.0.0.1");
    let port = match url.port() {
        Some(5080) | None => OPENOBSERVE_GRPC_PORT,
        Some(p) => p,
    };
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    format!("{}://{host}:{port}", url.scheme())
}

fn has_header(config: &TracingConfig, name: &str) -> bool {
    config.headers.keys().any(|k| k.eq_ignore_ascii_case(name))
}

/// Applies the service defaults under the caller's [`TracingConfig`]: the endpoint
/// ([`traces_endpoint`], or [`grpc_endpoint`] for gRPC) and the `authorization` (Basic) and
/// `stream-name` headers. Values already set in `config` win.
pub fn openobserve_tracing_defaults(
    mut config: TracingConfig,
    openobserve: &OpenObserveConfig,
    stream: Option<&str>,
) -> Result<TracingConfig> {
    let stream = stream_name(stream.unwrap_or(openobserve.traces_stream()), "tracing")?;
    if config.endpoint.is_none() {
        config.endpoint = Some(match config.protocol {
            Protocol::Grpc => grpc_endpoint(openobserve),
            _ => traces_endpoint(openobserve),
        });
    }
    if !has_header(&config, "stream-name") {
        config.headers.insert("stream-name".into(), stream);
    }
    if !has_header(&config, "authorization") {
        if let Some(auth) = openobserve.authorization() {
            config
                .headers
                .insert("authorization".into(), auth.expose_secret().to_string());
        }
    }
    if config.protocol == Protocol::Grpc && !has_header(&config, "organization") {
        config
            .headers
            .insert("organization".into(), openobserve.org().to_string());
    }
    Ok(config)
}

fn http_client(tls: &TlsConfig, openobserve: &OpenObserveConfig) -> Result<reqwest::Client> {
    let common = openobserve.common();
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .use_rustls_tls()
        .connect_timeout(common.connect_timeout())
        .user_agent(default_user_agent());
    if tls.insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let read = |path: &std::path::Path, what: &str| {
        std::fs::read(path).map_err(|e| {
            Error::config(format!("{what} file {} cannot be read", path.display())).with_source(e)
        })
    };
    let mut ca = Vec::new();
    if let Some(path) = &tls.ca_cert_path {
        ca.extend(read(path, "CA certificate")?);
        ca.push(b'\n');
    }
    if let Some(pem) = &tls.ca_cert_pem {
        ca.extend_from_slice(pem.as_bytes());
    }
    if !ca.is_empty() {
        let certs = reqwest::Certificate::from_pem_bundle(&ca)
            .map_err(|e| Error::config("CA certificate is not valid PEM").with_source(e))?;
        if certs.is_empty() {
            return Err(Error::config("CA certificate contains no certificate"));
        }
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    if let (Some(cert), Some(key)) = (&tls.client_cert_path, &tls.client_key_path) {
        let mut pem = read(cert, "client certificate")?;
        pem.push(b'\n');
        pem.extend(read(key, "client key")?);
        let identity = reqwest::Identity::from_pem(&pem).map_err(|e| {
            Error::config("client certificate or key is not valid PEM").with_source(e)
        })?;
        builder = builder.identity(identity);
    }
    if let Some(proxy) = common.proxy() {
        let proxy = reqwest::Proxy::all(proxy)
            .map_err(|e| Error::config("proxy is not a valid URL").with_source(e.without_url()))?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(|e| Error::config("cannot build the HTTP client").with_source(e.without_url()))
}

/// Builds the span exporter for a resolved config: `http/protobuf` → OTLP/HTTP protobuf,
/// `native` → OTLP/HTTP JSON (OpenObserve's own ingestion format), `grpc` → OTLP/gRPC with
/// the `authorization`, `organization` and `stream-name` metadata. TLS settings come from
/// `config.tls`, else from the OpenObserve config.
pub fn create_span_exporter(
    config: &TracingConfig,
    openobserve: &OpenObserveConfig,
) -> Result<opentelemetry_otlp::SpanExporter> {
    if config.protocol == Protocol::Grpc {
        return otlp_exporter(config);
    }
    let tls = config
        .tls
        .clone()
        .unwrap_or_else(|| openobserve.common().tls().clone());
    let protocol = if config.protocol == Protocol::Native {
        opentelemetry_otlp::Protocol::HttpJson
    } else {
        opentelemetry_otlp::Protocol::HttpBinary
    };
    let mut builder = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_http_client(http_client(&tls, openobserve)?)
        .with_protocol(protocol)
        .with_timeout(config.batch.export_timeout)
        .with_endpoint(
            config
                .endpoint
                .clone()
                .unwrap_or_else(|| traces_endpoint(openobserve)),
        )
        .with_headers({
            // The exporter sets `User-Agent` itself; overwrite that exact key.
            let mut user_agent = default_user_agent();
            let mut headers: HashMap<String, String> = HashMap::new();
            for (k, v) in &config.headers {
                if k.eq_ignore_ascii_case("user-agent") {
                    user_agent = v.clone();
                } else {
                    headers.insert(k.clone(), v.clone());
                }
            }
            headers.insert("User-Agent".into(), user_agent);
            headers
        });
    if config.compression == Compression::Gzip {
        builder = builder.with_compression(opentelemetry_otlp::Compression::Gzip);
    }
    builder
        .build()
        .map_err(|e| Error::config(format!("cannot build the OTLP exporter: {e}")))
}

/// Sets up OpenTelemetry tracing that exports to OpenObserve (SPEC §5). Endpoint, auth and
/// stream come from `config.openobserve` (default [`OpenObserveConfig::from_env`]); every
/// [`TracingConfig`] field can override them. Must be called inside a tokio runtime; `CONFIG`
/// for invalid settings.
///
/// ```no_run
/// # async fn demo() -> grow_openobserve::Result<()> {
/// use grow_openobserve::traces::init_tracing;
/// use grow_openobserve::tracing::{traced, TracingConfig};
///
/// let handle = init_tracing(TracingConfig::new("checkout"))?;
/// traced("charge", async { Ok::<_, std::io::Error>(()) }).await.ok();
/// handle.shutdown().await?;
/// # Ok(()) }
/// ```
pub fn init_tracing(config: impl Into<OpenObserveTracingConfig>) -> Result<TracingHandle> {
    let config = config.into();
    let openobserve = match config.openobserve {
        Some(c) => c,
        None => OpenObserveConfig::from_env()?,
    };
    let tracing =
        openobserve_tracing_defaults(config.tracing, &openobserve, config.stream.as_deref())?;
    init_tracing_with(tracing, |resolved| {
        create_span_exporter(resolved, &openobserve)
    })
}

impl OpenObserveClient {
    /// [`init_tracing`] with this client's configuration; `stream` defaults to the config's
    /// `traces_stream`.
    pub fn init_tracing(
        &self,
        tracing: TracingConfig,
        stream: Option<&str>,
    ) -> Result<TracingHandle> {
        let mut config = OpenObserveTracingConfig::new(tracing).openobserve(self.config().clone());
        config.stream = stream.map(str::to_string);
        init_tracing(config)
    }
}
