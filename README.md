# Grow Codes SDK for OpenObserve

[![CI](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/grow-codes-packages/openobserve?sort=semver)](https://github.com/grow-codes-packages/openobserve/releases)
[![Docs](https://img.shields.io/badge/docs-grow--codes--packages.github.io-blue)](https://grow-codes-packages.github.io/openobserve/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [![npm](https://img.shields.io/npm/v/@grow-codes/openobserve)](https://www.npmjs.com/package/@grow-codes/openobserve) | [![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-typescript.json)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml) |
| Rust | [![crates.io](https://img.shields.io/crates/v/grow-openobserve)](https://crates.io/crates/grow-openobserve) [![docs.rs](https://img.shields.io/docsrs/grow-openobserve)](https://docs.rs/grow-openobserve) | [![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-rust.json)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml) |
| Go | [![Go Reference](https://pkg.go.dev/badge/github.com/grow-codes-packages/openobserve/go.svg)](https://pkg.go.dev/github.com/grow-codes-packages/openobserve/go) | [![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-go.json)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml) |
| Python | [![PyPI](https://img.shields.io/pypi/v/grow-openobserve)](https://pypi.org/project/grow-openobserve/) | [![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-python.json)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml) |
| Java | [![Maven Central](https://img.shields.io/maven-central/v/codes.grow.packages/openobserve)](https://central.sonatype.com/artifact/codes.grow.packages/openobserve) | [![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-java.json)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml) |

Open-source client libraries for [OpenObserve](#trademarks-and-disclaimer) in **TypeScript, Rust, Go,
Python and Java** — one API, five idiomatic packages, tested against a real OpenObserve in Docker.

- **Typed client** with timeouts, retries with jitter, `Retry-After`, cancellation and one error
  model (`OpenObserveError` with canonical codes) in every language.
- **Secrets stay secret**: passwords, service-account tokens and passcodes are redacted in
  `toString`, logs, hooks and error messages.
- **OpenObserve API**: log ingestion (`_json`, `_multi`, Elasticsearch `_bulk`, OTLP), SQL search
  with microsecond time ranges, `_around`, `_values`, histograms, a polling tail, stream schema and
  settings, trace queries, organizations and service accounts.
- **Logging**: batched, compressed log shipping with back-pressure, redaction, a logger facade and
  adapters for the common logging frameworks of each language; log search and tail.
- **Tracing**: OpenTelemetry setup in one call (OTLP/HTTP to `/api/{org}/v1/traces` with Basic auth
  and the `stream-name` header, or OTLP/gRPC), span helpers, context propagation, framework
  middleware and a trace query client.

## Contents

- [Supported service version](#supported-service-version)
- [Install](#install)
- [Quickstart](#quickstart)
- [Configuration](#configuration)
- [Function overview](#function-overview)
- [Testing with Docker](#testing-with-docker)
- [Compatibility](#compatibility)
- [Contributing](#contributing)
- [Security](#security)
- [Licence](#licence)
- [Trademarks and disclaimer](#trademarks-and-disclaimer)
- [About Grow Codes](#about-grow-codes)

## Supported service version

Tested in CI against **OpenObserve `v1.1.2`** (`public.ecr.aws/zinclabs/openobserve:v1.1.2`, the
version pinned in the Grow Codes stack catalog). The SDK uses the open-source HTTP API (and OTLP);
enterprise-only routes such as the service graph are reported as `UNSUPPORTED`. Other versions
with the same API usually work; see [Compatibility](#compatibility) and
[docs/compatibility.md](docs/compatibility.md).

## Install

| Language | Package | Install |
| - | - | - |
| TypeScript (Node.js ≥ 22.12) | `@grow-codes/openobserve` | `npm install @grow-codes/openobserve` |
| Rust (MSRV 1.88) | `grow-openobserve` | `cargo add grow-openobserve` |
| Go (1.26+) | `github.com/grow-codes-packages/openobserve/go` | `go get github.com/grow-codes-packages/openobserve/go` |
| Python (≥ 3.10) | `grow-openobserve` | `pip install grow-openobserve` (`grow-openobserve[grpc]` for OTLP/gRPC) |
| Java (17+) | `codes.grow.packages:openobserve` | see below |

Maven:

```xml
<dependency>
  <groupId>codes.grow.packages</groupId>
  <artifactId>openobserve</artifactId>
  <version>0.1.0</version>
</dependency>
```

Gradle (Kotlin DSL):

```kotlin
implementation("codes.grow.packages:openobserve:0.1.0")
```

## Quickstart

Every package reads its configuration from the environment or from an explicit config object.
OpenObserve uses HTTP Basic auth: set `OPENOBSERVE_URL`, `OPENOBSERVE_USER` and either
`OPENOBSERVE_PASSWORD` or a service-account `OPENOBSERVE_TOKEN` (see
[Configuration](#configuration)). The examples check the health, write two log records, read them
back with SQL and send one trace.

### TypeScript

```ts
import { OpenObserveClient, withSpan } from "@grow-codes/openobserve";

const client = OpenObserveClient.fromEnv();
try {
  console.log((await client.health()).status, await client.serverVersion());
  await client.logs.ingestJson("app", [
    { message: "checkout started", level: "info" },
    { message: "payment failed", level: "error" },
  ]);
  const res = await client.logs.search(`SELECT * FROM "app" WHERE level = 'error'`, {
    trackTotalHits: true,
  });
  console.log(res.totalHits, res.hits[0]?.message);

  const tracing = client.initTracing({ serviceName: "checkout" });
  await withSpan("checkout", async () => {});
  await tracing.shutdown(); // flushes the spans
} finally {
  await client.close();
}
```

### Rust

```rust
use grow_openobserve::logs::{IngestOptions, SearchOptions};
use grow_openobserve::tracing::TracingConfig;
use grow_openobserve::OpenObserveClient;
use serde_json::json;

#[tokio::main]
async fn main() -> grow_openobserve::Result<()> {
    let client = OpenObserveClient::from_env()?;
    println!("{} {}", client.health().await?.status, client.server_version().await?);
    let logs = client.logs();
    logs.ingest_json(
        "app",
        &[
            json!({"message": "checkout started", "level": "info"}),
            json!({"message": "payment failed", "level": "error"}),
        ],
        IngestOptions::default(),
    )
    .await?;
    let res = logs
        .search(
            "SELECT * FROM \"app\" WHERE level = 'error'",
            SearchOptions::default().track_total_hits(true),
        )
        .await?;
    println!("{:?} {:?}", res.total_hits, res.hits.first());

    let tracing = client.init_tracing(TracingConfig::new("checkout"), None)?;
    tracing.shutdown().await?; // flushes the spans
    client.close().await;
    Ok(())
}
```

### Go

```go
package main

import (
	"context"
	"fmt"
	"log"

	openobserve "github.com/grow-codes-packages/openobserve/go"
)

func main() {
	ctx := context.Background()
	client, err := openobserve.NewClientFromEnv()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	health, _ := client.Health(ctx)
	fmt.Println(health.Status)
	if _, err := client.Logs().IngestJSON(ctx, "app", []map[string]any{
		{"message": "checkout started", "level": "info"},
		{"message": "payment failed", "level": "error"},
	}); err != nil {
		log.Fatal(err)
	}
	res, err := client.Logs().Search(ctx, `SELECT * FROM "app" WHERE level = 'error'`,
		openobserve.SQLSearchOptions{TrackTotalHits: true})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(*res.Total, len(res.Hits))

	tracing, err := client.InitTracing(ctx, openobserve.TracingConfig{ServiceName: "checkout"})
	if err != nil {
		log.Fatal(err)
	}
	_, span := tracing.Tracer("app").Start(ctx, "checkout")
	span.End()
	_ = tracing.Shutdown(ctx) // flushes the spans
}
```

### Python

```python
from grow_openobserve import OpenObserveClient, TracingConfig, with_span

with OpenObserveClient.from_env() as client:
    print(client.health().status, client.server_version())
    client.logs.ingest_json(
        "app",
        [
            {"message": "checkout started", "level": "info"},
            {"message": "payment failed", "level": "error"},
        ],
    )
    res = client.logs.search(
        "SELECT * FROM \"app\" WHERE level = 'error'", track_total_hits=True
    )
    print(res.total_hits, res.hits[0]["message"] if res.hits else None)

    tracing = client.init_tracing(TracingConfig(service_name="checkout"))
    with with_span("checkout"):
        pass
    tracing.shutdown()  # flushes the spans
```

Async code uses `AsyncOpenObserveClient` with the same methods (`async with`, `await client.health()`).

### Java

```java
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.logs.SqlSearchOptions;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import java.util.List;
import java.util.Map;

try (OpenObserveClient client = OpenObserveClient.fromEnv()) {
    System.out.println(client.health().status() + " " + client.serverVersion());
    client.logs().ingestJson("app", List.of(
        Map.of("message", "checkout started", "level", "info"),
        Map.of("message", "payment failed", "level", "error")));
    var res = client.logs().search("SELECT * FROM \"app\" WHERE level = 'error'",
        SqlSearchOptions.builder().trackTotalHits(true).build());
    System.out.println(res.total() + " " + res.hits());

    var tracing = client.initTracing(TracingConfig.builder("checkout").build());
    tracing.tracer("app").spanBuilder("checkout").startSpan().end();
    tracing.shutdown(); // flushes the spans
}
```

More complete programs live in [examples/](examples/) — one folder per language.

## Configuration

All settings can be passed in code or through environment variables. Precedence: **explicit value >
`GROW_OPENOBSERVE_*` variable > `OPENOBSERVE_*` variable > default.** A present but invalid value
raises a `ConfigError` that names the variable (never its value).

| Setting | Environment variables | Default |
| - | - | - |
| `url` | `GROW_OPENOBSERVE_URL`, `OPENOBSERVE_URL` | `http://127.0.0.1:5080` |
| `org` — organization | `GROW_OPENOBSERVE_ORG`, `OPENOBSERVE_ORG` | `default` |
| `user` — Basic-auth e-mail (root, user or service account) | `GROW_OPENOBSERVE_USER`, `OPENOBSERVE_USER` | none |
| `password` (secret) | `GROW_OPENOBSERVE_PASSWORD`, `OPENOBSERVE_PASSWORD` | none |
| `token` (secret) — service-account token, or a pre-encoded `Basic …` value | `GROW_OPENOBSERVE_TOKEN`, `OPENOBSERVE_TOKEN` | none |
| `stream` — default log stream | `GROW_OPENOBSERVE_STREAM`, `OPENOBSERVE_STREAM` | `default` |
| `tracesStream` — default traces stream | `GROW_OPENOBSERVE_TRACES_STREAM`, `OPENOBSERVE_TRACES_STREAM` | `default` |
| `connectTimeout` | `GROW_OPENOBSERVE_CONNECT_TIMEOUT` | 10 s |
| `requestTimeout` (per attempt) | `GROW_OPENOBSERVE_REQUEST_TIMEOUT` | 30 s |
| `totalTimeout` (including retries) | `GROW_OPENOBSERVE_TOTAL_TIMEOUT` | 120 s |
| `retry.maxRetries` | `GROW_OPENOBSERVE_MAX_RETRIES` | 3 |
| `tls.caCertPath` | `GROW_OPENOBSERVE_CA_CERT` | system roots |
| `tls.clientCertPath` / `tls.clientKeyPath` | `GROW_OPENOBSERVE_CLIENT_CERT` / `GROW_OPENOBSERVE_CLIENT_KEY` | none |
| `tls.insecureSkipVerify` (tests only) | `GROW_OPENOBSERVE_INSECURE_SKIP_VERIFY` | `false` |
| `proxy` | `GROW_OPENOBSERVE_PROXY` | none (environment proxies are ignored) |
| `headers`, `userAgentSuffix`, `hooks`, `logger` | — | none |

Authentication rules (identical in every language):

- OpenObserve accepts **HTTP Basic** only (`Bearer` is rejected). `user` + `password` sends
  `Basic base64(user:password)`; `user` + `token` sends `Basic base64(user:token)` (a service
  account's token works only with its own e-mail).
- A `token` that starts with `Basic ` is a pre-encoded header and is sent as is (no `user` needed).
- `password` and `token` are mutually exclusive; `user` needs one of them. Without credentials only
  `health()` works.
- Stream names are normalized like the server does: lower case, every character other than
  `[a-z0-9_]` becomes `_`.

Durations accept milliseconds (`1500`) or units (`500ms`, `10s`, `1m30s`); booleans accept
`true/false/1/0/yes/no/on/off`. Field names follow each language's convention
(`traces_stream` in Python and Rust, `TracesStream` in Go). The full reference is in
[docs/configuration.md](docs/configuration.md).

## Function overview

<!-- service:function-overview — the service fills this section; keep the heading. -->

The same functions exist in every language, named per language convention (`ingestJson` in
TypeScript and Java, `ingest_json` in Python and Rust, `IngestJSON` in Go). Every function has a
page with all five languages in the [documentation](https://grow-codes-packages.github.io/openobserve/functions/).

### Client and status

| Function | Description |
| - | - |
| `fromEnv()` / constructor | build from `GROW_OPENOBSERVE_*` / `OPENOBSERVE_*` or an explicit config |
| `health()` | unauthenticated `GET /healthz`; never throws for an unhealthy or unreachable service |
| `ping()` | authenticated `GET /api/{org}/config`; returns the latency |
| `serverVersion()` | version from `/api/{org}/config` (e.g. `v1.1.2`); `UNSUPPORTED` when missing |
| `close()` | idempotent; flushes every log client made by the client |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.serverVersion()` | `client.server_version().await?` | `client.ServerVersion(ctx)` | `client.server_version()` | `client.serverVersion()` |

### Logs ingestion (`client.logs`)

| Function | Description |
| - | - |
| `ingestJson(stream, records)` | `POST /api/{org}/{stream}/_json`; per-stream counters (HTTP 200 even on partial failure) |
| `ingestMulti(stream, ndjson)` | `POST /api/{org}/{stream}/_multi` (newline-delimited JSON) |
| `ingestBulk(actions)` | Elasticsearch-compatible `POST /api/{org}/_bulk`; only `index`/`create` (others → `VALIDATION`) |
| `ingestOtlp(stream, records)` | SDK `LogRecord`s as OTLP/HTTP JSON to `/api/{org}/v1/logs` with `stream-name` |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.logs.ingestJson("app", [{ message: "hi" }])` | `logs.ingest_json("app", &[json!({"message": "hi"})], IngestOptions::default()).await?` | `client.Logs().IngestJSON(ctx, "app", []map[string]any{{"message": "hi"}})` | `client.logs.ingest_json("app", [{"message": "hi"}])` | `client.logs().ingestJson("app", List.of(Map.of("message", "hi")))` |

### Search (`client.logs`)

| Function | Description |
| - | - |
| `search(sql, {start, end, from, size, trackTotalHits})` | `POST /api/{org}/_search` with a µs time range (default last 15 min, size 100); `trackTotalHits` adds the exact count |
| `searchAround(stream, timestamp, size)` | rows around one timestamp (`_around`), anchor included |
| `valuesOf(stream, fields, {start, end})` | most frequent values per field (`_values`) |
| `histogram(stream, {start, end, interval, filter})` | record counts per time bucket |
| `tail(stream, onRow, {filter, start, interval, overlap})` | follows a stream by polling with a `_timestamp` cursor (OpenObserve has no live tail) |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.logs.search(sql, { trackTotalHits: true })` | `logs.search(sql, SearchOptions::default().track_total_hits(true)).await?` | `client.Logs().Search(ctx, sql, openobserve.SQLSearchOptions{TrackTotalHits: true})` | `client.logs.search(sql, track_total_hits=True)` | `client.logs().search(sql, SqlSearchOptions.builder().trackTotalHits(true).build())` |

### Streams (`client.streams`)

| Function | Description |
| - | - |
| `listStreams(type)` | streams of one type with schema, settings and stats |
| `getStreamSchema(stream)` | fields, settings and stats; `NOT_FOUND` for a missing stream |
| `updateStreamSettings(stream, {retentionDays, partitionKeys, fullTextSearchKeys, indexFields, bloomFilterFields})` | a list replaces the current value (the SDK sends the add/remove deltas) |
| `createStream(stream, {fields, settings})` | creates a stream ahead of the first write |
| `deleteStream(stream)` | deletes a stream and its data |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.streams.updateStreamSettings("app", { retentionDays: 7 })` | `streams.update_stream_settings("app", StreamSettingsUpdate { retention_days: Some(7), ..Default::default() }, StreamType::Logs).await?` | `client.Streams().UpdateStreamSettings(ctx, "app", openobserve.StreamSettingsUpdate{RetentionDays: &days})` | `client.streams.update_stream_settings("app", retention_days=7)` | `client.streams().updateStreamSettings("app", StreamSettingsUpdate.builder().retentionDays(7).build())` |

### Tracing (`initTracing`, `client.traces`)

| Function | Description |
| - | - |
| `initTracing(config)` / `client.initTracing(config)` | OpenTelemetry SDK with the OpenObserve exporter (OTLP/HTTP protobuf by default, OTLP/gRPC on 5081) |
| `getTrace(traceId)` | all spans of a trace (default window 7 days, paginated); `null` when unknown |
| `searchTraces({service, operation, tags, minDuration, maxDuration, start, end, limit})` | trace summaries from `traces/latest` (default last 1 h) |
| `listServices()`, `listOperations(service)` | distinct services / span names (last 24 h) |
| `getDependencies(range)` | `UNSUPPORTED` (enterprise only) |
| `waitForTrace(traceId)`, `traceTimeRange(traceId)` | test helper; where and when a trace is stored |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.traces.getTrace(id)` | `client.traces().get_trace(&id).await?` | `client.Traces().GetTrace(ctx, id)` | `client.traces.get_trace(trace_id)` | `client.traces().getTrace(id)` |

The span helpers (`withSpan`, `traced`, `currentTraceId`, `inject`/`extract`, `traceFields`) and the
framework adapters are documented in [docs/tracing.md](docs/tracing.md).

### Logging category

| Function | Description |
| - | - |
| `logClient({stream, protocol})` | batching `LogClient` (`_json` or OTLP) with redaction, trace correlation and a `Logger` |
| `logExporter(stream, protocol)` | the `LogExporter` behind it (Go: `NewJSONLogExporter` / `NewOTLPLogExporter`) |
| `logQuery(stream)` | `LogQueryClient`: `search` (text → `match_all`, `SELECT …` → raw SQL), `tail`, `listStreams`, `listLabels`, `waitForLog` |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `client.logClient({ stream: "app" }).logger("api").info("hi")` | `client.log_client(LogClientOptions::default().stream("app"))?` | `client.NewLogClient("app", openobserve.LogClientOptions{})` | `client.log_client("app").logger("api").info("hi")` | `client.logClient(LogClientOptions.builder().stream("app").build())` |

### Admin helpers (`client.admin`)

| Function | Description |
| - | - |
| `listOrganizations()` | organizations visible to the user |
| `createServiceAccount(email)`, `rotateServiceAccountToken(email)` | the token is returned once, as a redacted secret |
| `listServiceAccounts()`, `deleteServiceAccount(email)` | listed tokens are masked by the server |
| `getPasscode()` | the user's ingestion passcode (secret) |

| TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - |
| `await client.admin.createServiceAccount("ci@example.com")` | `client.admin().create_service_account("ci@example.com", Default::default()).await?` | `client.Admin().CreateServiceAccount(ctx, "ci@example.com", openobserve.ServiceAccountOptions{})` | `client.admin.create_service_account("ci@example.com")` | `client.admin().createServiceAccount("ci@example.com")` |

<!-- /service:function-overview -->

## Testing with Docker

The integration suite runs every public function against a real OpenObserve (`public.ecr.aws/zinclabs/openobserve:v1.1.2`) in Docker.
Each language package is packed and installed into a separate consumer project first, so the tests
use the package exactly as you would.

```sh
make test-docker                     # all five languages
make test-docker LANGS="go python"   # a subset
```

No host ports are published and every run uses its own Compose project, so runs never collide; the
stack is always removed afterwards (`docker compose down -v`). Unit tests need no Docker:
`make test` or `make coverage`.

## Compatibility

| | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| Runtime | Node.js ≥ 22.12, ESM + CJS | stable, MSRV 1.88 | 1.26, 1.27 | 3.10 – 3.14 | 17, 21, 25 |
| Async | `Promise` + `AbortSignal` | `async` (tokio), `blocking` feature | `context.Context` | sync + `asyncio` | sync + `CompletableFuture` |
| OpenObserve | v1.1.2 | v1.1.2 | v1.1.2 | v1.1.2 | v1.1.2 |
| Tracing `native` protocol | OTLP/HTTP JSON | OTLP/HTTP JSON | OTLP/HTTP protobuf | OTLP/HTTP protobuf | OTLP/HTTP protobuf |

Not servable by the open-source container (covered by unit tests only): `getDependencies`
(enterprise service graph), OTLP `partialSuccess`, 429/5xx retries, TLS/proxy and the OTLP/gRPC
exporter. Server quirks (for example, `_around` ignores its `sql` filter and `start_time=0` is
rejected) and language differences are listed in [docs/compatibility.md](docs/compatibility.md).

## Contributing

Contributions are welcome — please read [CONTRIBUTING.md](CONTRIBUTING.md) and
[AGENTS.md](AGENTS.md) first. All five languages keep the same API; a change lands in all of them
together. Parts of this repository are generated by
[sdk-kit](https://github.com/grow-codes-packages/sdk-kit) and changed there.

## Security

Please report vulnerabilities privately to [opensource@grow.codes](mailto:opensource@grow.codes).
See [SECURITY.md](SECURITY.md).

## Licence

Licensed under the [Apache License, Version 2.0](LICENSE). See [NOTICE](NOTICE) for attributions.

## Trademarks and disclaimer

This is an independent, community-maintained project. It is **not affiliated with, endorsed or
sponsored by ZincLabs, Inc.** OpenObserve is a trademark of ZincLabs, Inc.; it is used here only to name the
service this SDK connects to. The OpenObserve server is licensed separately by its owner
(AGPL-3.0-only (open core)).

The software is provided "AS IS", without warranties or conditions of any kind. Grow Codes is not
responsible for any damage, data loss, security incident, outage or cost that results from using it.
See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## About Grow Codes

This SDK is built and maintained by [Grow Codes](https://grow.codes). Grow Codes publishes the
client libraries it builds as open source in the
[grow-codes-packages](https://github.com/grow-codes-packages) organisation, generated from one
shared kit so that every service and every language behaves the same way. Learn more at
[grow.codes](https://grow.codes).
