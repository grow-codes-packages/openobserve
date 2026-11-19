// Service-owned. Helpers for the OpenObserve unit tests: recorded fixtures and mock clients.
#![allow(dead_code)]

use std::time::Duration;

use grow_openobserve as sdk;
use sdk::core::{RandomSource, RetryPolicy};
use sdk::{OpenObserveClient, OpenObserveConfig};
use serde_json::Value;
use wiremock::{MockServer, Request, ResponseTemplate};

/// A recorded fixture from `tests/fixtures/openobserve/<name>.json`.
pub fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/tests/fixtures/openobserve/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("fixture {path}"));
    serde_json::from_str(&text).expect("fixture JSON")
}

/// The recorded response body of a fixture.
pub fn body(name: &str) -> Value {
    fixture(name)["response"]["body"].clone()
}

/// The recorded response of a fixture (status and JSON or plain-text body).
pub fn respond(name: &str) -> ResponseTemplate {
    let f = fixture(name);
    let status = f["response"]["status"].as_u64().unwrap() as u16;
    match &f["response"]["body"] {
        Value::String(s) => ResponseTemplate::new(status).set_body_string(s.clone()),
        Value::Null => ResponseTemplate::new(status),
        other => ResponseTemplate::new(status).set_body_json(other.clone()),
    }
}

/// Retry policy with short backoffs.
pub fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(20),
        ..RetryPolicy::default()
    }
    .with_random(RandomSource::fixed(0.0))
}

/// Config pointing at the mock server with root credentials.
pub fn config(server: &MockServer) -> OpenObserveConfig {
    OpenObserveConfig::builder()
        .url(server.uri())
        .user("root@example.com")
        .password("Complexpass#123")
        .retry(fast_retry())
        .build()
        .unwrap()
}

/// Client pointing at the mock server.
pub fn client(server: &MockServer) -> OpenObserveClient {
    OpenObserveClient::new(config(server)).unwrap()
}

/// `Basic base64(root@example.com:Complexpass#123)`.
pub const ROOT_AUTH: &str = "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=";

/// Requests received by the server.
pub async fn requests(server: &MockServer) -> Vec<Request> {
    server.received_requests().await.unwrap_or_default()
}

/// The JSON body of a request.
pub fn json_body(req: &Request) -> Value {
    serde_json::from_slice(&req.body).expect("JSON body")
}

/// A query parameter of a request.
pub fn query(req: &Request, name: &str) -> Option<String> {
    req.url
        .query_pairs()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// A header of a request.
pub fn header(req: &Request, name: &str) -> Option<String> {
    req.headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}
