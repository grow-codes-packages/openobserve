// Created by sdk-kit 0.1.0; owned by this repository.
//! Helpers for the integration tests: bootstrap credentials, unique names, clients for the
//! root user and the bootstrap service account, and polling.

use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use grow_openobserve as sdk;
use sdk::{OpenObserveClient, OpenObserveConfig};

/// Loads `KEY=value` lines from `$GROW_BOOTSTRAP_ENV` (default `/bootstrap/env`) into the
/// process environment; variables that are already set win. A missing file is not an error.
pub fn load_bootstrap_env() {
    let path = std::env::var("GROW_BOOTSTRAP_ENV").unwrap_or_else(|_| "/bootstrap/env".to_owned());
    let Ok(text) = std::fs::read_to_string(path) else {
        return;
    };
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().trim_start_matches("export ").trim();
            let value = value.trim().trim_matches('"');
            if !key.is_empty() && std::env::var_os(key).is_none() {
                std::env::set_var(key, value);
            }
        }
    }
}

/// `$GROW_CONFORMANCE_DIR`, else `<repo>/conformance/scenarios`.
pub fn conformance_dir() -> PathBuf {
    sdk::testing::scenarios_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
}

/// A unique lower-case name: `rs_<prefix>_<random>`.
pub fn unique(prefix: &str) -> String {
    format!("rs_{prefix}_{}", sdk::testing::run_id())
}

/// Client with the root credentials from the bootstrap file.
pub fn root_client() -> OpenObserveClient {
    load_bootstrap_env();
    OpenObserveClient::from_env().expect("root client from env")
}

/// Config authenticated with the bootstrap service account (user + token).
pub fn service_account_config() -> OpenObserveConfig {
    load_bootstrap_env();
    let user = std::env::var("OPENOBSERVE_SA_EMAIL").expect("OPENOBSERVE_SA_EMAIL");
    let token = std::env::var("OPENOBSERVE_SA_TOKEN").expect("OPENOBSERVE_SA_TOKEN");
    OpenObserveConfig::builder()
        .user(user)
        .token(token)
        .build_from_env()
        .expect("service account config")
}

/// Polls `f` every 250 ms until `ok(value)` holds or 20 s pass; returns the last value.
pub async fn eventually<T, F, Fut>(mut f: F, ok: impl Fn(&T) -> bool) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let value = f().await;
        if ok(&value) || Instant::now() > deadline {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
