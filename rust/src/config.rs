// Skeleton created by sdk-kit 0.1.0; owned by this repository (edit freely).
//! `OpenObserveConfig`: common settings (SPEC §2.2) plus the OpenObserve organization,
//! credentials and default streams.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;

use crate::core::{
    CommonConfig, CommonConfigBuilder, EnvReader, Error, Hooks, Result, RetryPolicy, Secret,
    TlsConfig,
};

/// Endpoint used when neither the builder nor the environment sets one.
pub const DEFAULT_URL: &str = "http://127.0.0.1:5080";
/// Organization used when none is configured.
pub const DEFAULT_ORG: &str = "default";
/// Default log and traces stream (the server's OTLP default).
pub const DEFAULT_STREAM: &str = "default";
/// Prefix of every Grow environment variable.
pub const ENV_PREFIX: &str = "GROW_OPENOBSERVE_";

/// Vendor variables checked (in order) after `GROW_OPENOBSERVE_URL`.
const VENDOR_URL_VARS: &[&str] = &["OPENOBSERVE_URL"];
const VENDOR_ORG_VARS: &[&str] = &["OPENOBSERVE_ORG"];
const VENDOR_USER_VARS: &[&str] = &["OPENOBSERVE_USER"];
const VENDOR_PASSWORD_VARS: &[&str] = &["OPENOBSERVE_PASSWORD"];
const VENDOR_TOKEN_VARS: &[&str] = &["OPENOBSERVE_TOKEN"];
const VENDOR_STREAM_VARS: &[&str] = &["OPENOBSERVE_STREAM"];
const VENDOR_TRACES_STREAM_VARS: &[&str] = &["OPENOBSERVE_TRACES_STREAM"];

/// Server-side stream name normalization: lower case, every non `[a-z0-9_]` character → `_`.
pub fn normalize_stream_name(name: &str) -> String {
    crate::http::flatten_key(name.trim())
}

/// Immutable client configuration; build it with [`OpenObserveConfig::builder`] or
/// [`OpenObserveConfig::from_env`]. `Debug` output redacts every credential.
///
/// Environment variables read by [`OpenObserveConfig::from_env`] (explicit value >
/// `GROW_OPENOBSERVE_<NAME>` > `OPENOBSERVE_<NAME>` > default):
///
/// | Field | Variables | Default |
/// | - | - | - |
/// | url | `GROW_OPENOBSERVE_URL`, `OPENOBSERVE_URL` | `http://127.0.0.1:5080` |
/// | org | `GROW_OPENOBSERVE_ORG`, `OPENOBSERVE_ORG` | `default` |
/// | user | `GROW_OPENOBSERVE_USER`, `OPENOBSERVE_USER` | none |
/// | password | `GROW_OPENOBSERVE_PASSWORD`, `OPENOBSERVE_PASSWORD` | none |
/// | token | `GROW_OPENOBSERVE_TOKEN`, `OPENOBSERVE_TOKEN` | none |
/// | stream | `GROW_OPENOBSERVE_STREAM`, `OPENOBSERVE_STREAM` | `default` |
/// | traces stream | `GROW_OPENOBSERVE_TRACES_STREAM`, `OPENOBSERVE_TRACES_STREAM` | `default` |
///
/// OpenObserve only accepts HTTP Basic auth: `user` + `password`, or a service-account e-mail
/// (`user`) + its `token`. A token that starts with `Basic ` is a pre-encoded header and is sent
/// verbatim (no `user` needed). `Bearer` tokens are not accepted by the server.
#[derive(Clone)]
pub struct OpenObserveConfig {
    common: CommonConfig,
    org: String,
    user: Option<String>,
    password: Option<Secret>,
    token: Option<Secret>,
    stream: String,
    traces_stream: String,
}

impl fmt::Debug for OpenObserveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenObserveConfig")
            .field("common", &self.common)
            .field("org", &self.org)
            .field("user", &self.user)
            .field("password", &self.password)
            .field("token", &self.token)
            .field("stream", &self.stream)
            .field("traces_stream", &self.traces_stream)
            .finish()
    }
}

impl OpenObserveConfig {
    /// A builder with nothing set.
    pub fn builder() -> OpenObserveConfigBuilder {
        OpenObserveConfigBuilder::default()
    }

    /// Configuration from the process environment
    /// (explicit > `GROW_OPENOBSERVE_*` > vendor variables > defaults).
    pub fn from_env() -> Result<Self> {
        Self::builder().build_from_env()
    }

    /// Settings shared by every Grow Codes SDK.
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    /// Endpoint base URL.
    pub fn url(&self) -> &url::Url {
        self.common.url()
    }

    /// Organization identifier (default `default`).
    pub fn org(&self) -> &str {
        &self.org
    }

    /// Basic-auth user (a login e-mail or a service-account e-mail).
    pub fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Password of [`user`](Self::user), when configured.
    pub fn password(&self) -> Option<&Secret> {
        self.password.as_ref()
    }

    /// Service-account token or pre-encoded `Basic …` header, when configured.
    pub fn token(&self) -> Option<&Secret> {
        self.token.as_ref()
    }

    /// Default log stream (normalized).
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Default traces stream (normalized).
    pub fn traces_stream(&self) -> &str {
        &self.traces_stream
    }

    /// Whether credentials are configured.
    pub fn has_credentials(&self) -> bool {
        self.password.is_some() || self.token.is_some()
    }

    /// The `Authorization` header value (`Basic …`), or `None` without credentials. The result
    /// is a credential: never log its value.
    pub fn authorization(&self) -> Option<Secret> {
        if let Some(token) = &self.token {
            let token = token.expose_secret().trim();
            if is_pre_encoded(token) {
                return Some(Secret::new(format!("Basic {}", token[6..].trim())));
            }
            return Some(basic(self.user.as_deref().unwrap_or(""), token));
        }
        self.password
            .as_ref()
            .map(|p| basic(self.user.as_deref().unwrap_or(""), p.expose_secret()))
    }

    /// Credential values to redact from error messages and logs.
    pub fn known_secrets(&self) -> Vec<Secret> {
        let mut out: Vec<Secret> = self
            .password
            .iter()
            .chain(self.token.iter())
            .cloned()
            .collect();
        if let Some(header) = self.authorization() {
            out.push(Secret::new(&header.expose_secret()[6..]));
        }
        out
    }

    /// Checks the service rules (common rules are checked when building).
    pub fn validate(&self) -> Result<()> {
        if self.org.is_empty()
            || self
                .org
                .chars()
                .any(|c| matches!(c, '/' | '?' | '#') || c.is_whitespace())
        {
            return Err(Error::config(
                "invalid configuration: org must be a non-empty identifier",
            ));
        }
        if self.stream.is_empty() {
            return Err(Error::config(
                "invalid configuration: stream must not be blank",
            ));
        }
        if self.traces_stream.is_empty() {
            return Err(Error::config(
                "invalid configuration: tracesStream must not be blank",
            ));
        }
        if self.password.is_some() && self.token.is_some() {
            return Err(Error::config(
                "invalid configuration: set either password or token, not both",
            ));
        }
        if self
            .token
            .as_ref()
            .is_some_and(|t| t.expose_secret().trim().is_empty())
        {
            return Err(Error::config(
                "invalid configuration: credentials must not be blank",
            ));
        }
        let pre_encoded = self
            .token
            .as_ref()
            .is_some_and(|t| is_pre_encoded(t.expose_secret()));
        if (self.password.is_some() || (self.token.is_some() && !pre_encoded))
            && self.user.is_none()
        {
            return Err(Error::config(
                "invalid configuration: user is required with password or token",
            ));
        }
        if self.user.is_some() && !self.has_credentials() {
            return Err(Error::config(
                "invalid configuration: user requires password or token",
            ));
        }
        Ok(())
    }
}

fn is_pre_encoded(token: &str) -> bool {
    let t = token.trim().as_bytes();
    t.len() > 6 && t[..5].eq_ignore_ascii_case(b"basic") && t[5].is_ascii_whitespace()
}

fn basic(user: &str, secret: &str) -> Secret {
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{secret}"));
    Secret::new(format!("Basic {encoded}"))
}

fn non_empty(secret: Option<Secret>) -> Option<Secret> {
    secret.filter(|s| !s.is_empty())
}

/// Builder for [`OpenObserveConfig`]. Values set here win over the environment.
#[derive(Debug, Clone, Default)]
pub struct OpenObserveConfigBuilder {
    common: CommonConfigBuilder,
    org: Option<String>,
    user: Option<String>,
    password: Option<Secret>,
    token: Option<Secret>,
    stream: Option<String>,
    traces_stream: Option<String>,
}

impl OpenObserveConfigBuilder {
    /// Endpoint base URL (default [`DEFAULT_URL`]).
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.common = self.common.url(url);
        self
    }
    /// Organization (default `default`).
    pub fn org(mut self, org: impl Into<String>) -> Self {
        self.org = Some(org.into());
        self
    }
    /// Basic-auth user: a login e-mail or a service-account e-mail.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }
    /// Password of the user (mutually exclusive with `token`).
    pub fn password(mut self, password: impl Into<Secret>) -> Self {
        self.password = Some(password.into());
        self
    }
    /// Service-account token sent as `Basic base64(user:token)`, or a pre-encoded
    /// `Basic …` header value (mutually exclusive with `password`).
    pub fn token(mut self, token: impl Into<Secret>) -> Self {
        self.token = Some(token.into());
        self
    }
    /// Default log stream (default `default`; normalized like the server does).
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }
    /// Default traces stream (default `default`).
    pub fn traces_stream(mut self, stream: impl Into<String>) -> Self {
        self.traces_stream = Some(stream.into());
        self
    }
    /// Connect timeout (default 10 s).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.connect_timeout(timeout);
        self
    }
    /// Per-attempt timeout (default 30 s).
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.request_timeout(timeout);
        self
    }
    /// Whole-call timeout including retries (default 120 s).
    pub fn total_timeout(mut self, timeout: Duration) -> Self {
        self.common = self.common.total_timeout(timeout);
        self
    }
    /// Retry policy.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.common = self.common.retry(retry);
        self
    }
    /// Maximum retries (default 3).
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.common = self.common.max_retries(max_retries);
        self
    }
    /// TLS settings.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.common = self.common.tls(tls);
        self
    }
    /// Extra trusted CA certificates (PEM file).
    pub fn ca_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.common = self.common.ca_cert_path(path);
        self
    }
    /// Disables TLS verification (tests only).
    pub fn insecure_skip_verify(mut self, skip: bool) -> Self {
        self.common = self.common.insecure_skip_verify(skip);
        self
    }
    /// Proxy URL (environment proxies are ignored).
    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.common = self.common.proxy(proxy);
        self
    }
    /// Extra request header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.common = self.common.header(name, value);
        self
    }
    /// Text appended to the User-Agent.
    pub fn user_agent_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.common = self.common.user_agent_suffix(suffix);
        self
    }
    /// Request hooks.
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> Self {
        self.common = self.common.hooks(hooks);
        self
    }
    /// Edits any other common setting.
    pub fn common(mut self, f: impl FnOnce(CommonConfigBuilder) -> CommonConfigBuilder) -> Self {
        self.common = f(self.common);
        self
    }

    /// Builds without reading the environment.
    pub fn build(self) -> Result<OpenObserveConfig> {
        let user = self
            .user
            .map(|u| u.trim().to_string())
            .filter(|u| !u.is_empty());
        let config = OpenObserveConfig {
            common: self.common.build(DEFAULT_URL)?,
            org: self
                .org
                .unwrap_or_else(|| DEFAULT_ORG.to_string())
                .trim()
                .to_string(),
            user,
            password: non_empty(self.password),
            token: non_empty(self.token),
            stream: normalize_stream_name(self.stream.as_deref().unwrap_or(DEFAULT_STREAM)),
            traces_stream: normalize_stream_name(
                self.traces_stream.as_deref().unwrap_or(DEFAULT_STREAM),
            ),
        };
        config.validate()?;
        Ok(config)
    }

    /// Fills unset values from the process environment, then builds.
    pub fn build_from_env(self) -> Result<OpenObserveConfig> {
        self.build_with_env(&EnvReader::new())
    }

    /// Fills unset values from `env`, then builds. An explicit `password` ignores a token from
    /// the environment and vice versa, so overriding the credential kind never trips the
    /// "password and token" rule.
    pub fn build_with_env(mut self, env: &EnvReader) -> Result<OpenObserveConfig> {
        self.common = self.common.apply_env(env, VENDOR_URL_VARS)?;
        if self.org.is_none() {
            self.org = env.string("ORG", VENDOR_ORG_VARS);
        }
        if self.user.is_none() {
            self.user = env.string("USER", VENDOR_USER_VARS);
        }
        let explicit_password = self.password.is_some();
        let explicit_token = self.token.is_some();
        if !explicit_password && !explicit_token {
            self.password = non_empty(env.secret("PASSWORD", VENDOR_PASSWORD_VARS));
            self.token = non_empty(env.secret("TOKEN", VENDOR_TOKEN_VARS));
        }
        if self.stream.is_none() {
            self.stream = env.string("STREAM", VENDOR_STREAM_VARS);
        }
        if self.traces_stream.is_none() {
            self.traces_stream = env.string("TRACES_STREAM", VENDOR_TRACES_STREAM_VARS);
        }
        self.build()
    }
}
