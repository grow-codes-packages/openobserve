---
name: openobserve-sdk
description: Add and use the Grow Codes SDK for OpenObserve (`openobserve`) in TypeScript, Rust, Go, Python or Java projects. Use when code must talk to OpenObserve - installing @grow-codes/openobserve, the grow-openobserve crate or PyPI package, the Go module github.com/grow-codes-packages/openobserve/go or Maven codes.grow.packages:openobserve, configuring it with GROW_OPENOBSERVE_* variables, handling OpenObserveError codes, testing with the bundled fakes, or running the Docker integration suite of this repository.
---

# OpenObserve SDK (Grow Codes)

One API for OpenObserve in five languages (categories: logging,tracing). Pick the package for the
project's language, configure it from the environment, and handle errors by their canonical code.

## 1. Add the package

| Language | Command | Import |
| - | - | - |
| TypeScript | `npm install @grow-codes/openobserve` | `import { OpenObserveClient } from "@grow-codes/openobserve"` |
| Rust | `cargo add grow-openobserve` (add `tokio` with `macros,rt-multi-thread`) | `use grow_openobserve::OpenObserveClient;` |
| Go | `go get github.com/grow-codes-packages/openobserve/go` | `openobserve "github.com/grow-codes-packages/openobserve/go"` |
| Python | `pip install grow-openobserve` (or `uv add grow-openobserve`) | `from grow_openobserve import OpenObserveClient` |
| Java | Maven `codes.grow.packages:openobserve` (Gradle `implementation("codes.grow.packages:openobserve:<version>")`) | `import codes.grow.packages.openobserve.OpenObserveClient;` |

Check the latest version in the README badges before pinning one.

## 2. Configure

Prefer environment configuration so no credential is written into code:

```sh
export OPENOBSERVE_URL=https://openobserve.example.internal
export OPENOBSERVE_ORG=default                 # organization (default "default")
export OPENOBSERVE_USER=ci@example.com         # user or service-account e-mail
export OPENOBSERVE_TOKEN=...                   # its service-account token, or OPENOBSERVE_PASSWORD; never committed
export OPENOBSERVE_STREAM=app                  # default log stream (default "default")
export OPENOBSERVE_TRACES_STREAM=app_traces    # default traces stream (default "default")
```

Precedence: explicit config value > `GROW_OPENOBSERVE_<NAME>` > `OPENOBSERVE_<NAME>` > default.
OpenObserve accepts **HTTP Basic** only: `user` + `password`, or `user` + service-account `token`
(sent as `Basic base64(user:token)`); a `token` starting with `Basic ` is sent as is. `password`
and `token` are mutually exclusive. Stream names are normalized (lower case, non `[a-z0-9_]` → `_`).
Other variables: `…_CONNECT_TIMEOUT`, `…_REQUEST_TIMEOUT`, `…_TOTAL_TIMEOUT` (`10s`, `1500`),
`…_MAX_RETRIES`, `…_CA_CERT`, `…_CLIENT_CERT`, `…_CLIENT_KEY`, `…_PROXY`. Environment proxies
(`HTTPS_PROXY`) are ignored unless `proxy` is set.

## 3. Use

Create one client per process and close it on shutdown (TypeScript `await client.close()`, Rust
`client.close().await`, Go `defer client.Close()`, Python `with …`/`close()`, Java
try-with-resources).

- `health()` never throws for an unhealthy service; check `healthy`.
- `ping()` verifies credentials (`AUTHENTICATION` error when they are wrong).
- All I/O is retried with jitter on 429/502/503/504 and connection errors; only idempotent calls
  are retried on 500.
- Errors carry `code` (`CONFIG`, `AUTHENTICATION`, `PERMISSION_DENIED`, `NOT_FOUND`, `CONFLICT`,
  `RATE_LIMITED`, `VALIDATION`, `UNAVAILABLE`, `TIMEOUT`, `UNSUPPORTED`, `CANCELLED`), `retryable`,
  `httpStatus` and `requestId`. Branch on the code, not on the message text. Go:
  `errors.Is(err, openobserve.ErrNotFound)`.

### Logging

Create the log client once, send records with `send`/`sendBatch` or use the logger facade
(`logger("component").info("message", attrs)`) or the framework adapter for the language (pino or
winston, `tracing`/`log`, `slog`/zap, `logging`/structlog, Logback/Log4j2/JUL). Records are batched
and redacted; call `close()` on shutdown so the queue is flushed.

### Tracing

Call `initTracing({ serviceName })` once at startup, wrap work in `withSpan(name, fn)` or the
`traced` helper, propagate context with `inject`/`extract`, and call `shutdown()` on exit. Use the
trace query client to read traces back (`getTrace`, `searchTraces`, `waitForTrace`).

<!-- service:skill-usage — the service adds its own functions and examples here. -->

### OpenObserve functions

Modules hang off the client: `logs` (ingest + SQL search), `streams`, `traces` (trace query on
`tracesStream`), `admin` (TypeScript/Python properties, Rust/Java methods, Go `client.Logs()` …).

| Task | Call (TypeScript name; same in every language, per naming convention) |
| - | - |
| write JSON records | `client.logs.ingestJson(stream, records)` — check `failed`, HTTP 200 even on partial failure |
| write NDJSON / ES bulk / OTLP | `ingestMulti(stream, ndjson)`, `ingestBulk(actions)` (only `index`/`create`), `ingestOtlp(records, {stream})` |
| SQL search | `client.logs.search('SELECT * FROM "app" WHERE level = \'error\'', {start, end, size, trackTotalHits})` — times are µs (converted for you), default last 15 min, `totalHits` only with `trackTotalHits` |
| context, values, buckets | `searchAround(stream, ts, size)`, `valuesOf(stream, fields)`, `histogram(stream, {interval: "1 minute"})` |
| follow a stream | `client.logs.tail(stream, onRow, {filter})` (polling; never before `start`) or `client.logQuery(stream).tail(query, onRecord)` |
| ship app logs | `client.logClient({stream})` → `.logger("component").info(...)`; framework adapters take this client; `client.close()` flushes |
| search app logs | `client.logQuery(stream).search("text")` (text → `match_all`, `SELECT …` → raw SQL), `waitForLog(text)` in tests |
| streams | `client.streams.getStreamSchema(s)`, `updateStreamSettings(s, {retentionDays, partitionKeys, fullTextSearchKeys, indexFields, bloomFilterFields})` (lists replace), `createStream`, `deleteStream`, `listStreams(type)` |
| tracing | `client.initTracing({serviceName})` (OTLP/HTTP to `/api/{org}/v1/traces`, Basic auth + `stream-name`), then `client.traces.getTrace(id)` / `searchTraces({service, tags})` / `waitForTrace(id)` |
| service accounts | `client.admin.createServiceAccount(email)` → `{email, token}` (token only here; list shows a masked one) |

Gotchas: a missing stream is `NOT_FOUND` (server 400/20002); `getDependencies` is always
`UNSUPPORTED` (enterprise only); `_around` ignores `sql`; tag filters in `searchTraces` match single
spans; the `native` tracing protocol is OTLP/JSON in TypeScript/Rust and OTLP/protobuf in
Go/Python/Java. Full per-language examples: `docs/functions/*.md`; differences:
`docs/compatibility.md`.

Per-language snippets:

```ts
const client = OpenObserveClient.fromEnv();
await client.logs.ingestJson("app", [{ message: "hi", level: "info" }]);
const res = await client.logs.search(`SELECT * FROM "app"`, { trackTotalHits: true });
```

```rust
let client = OpenObserveClient::from_env()?;
client.logs().ingest_json("app", &[json!({"message": "hi"})], IngestOptions::default()).await?;
let res = client.logs().search("SELECT * FROM \"app\"", SearchOptions::default().track_total_hits(true)).await?;
```

```go
client, err := openobserve.NewClientFromEnv()
_, err = client.Logs().IngestJSON(ctx, "app", []map[string]any{{"message": "hi"}})
res, err := client.Logs().Search(ctx, `SELECT * FROM "app"`, openobserve.SQLSearchOptions{TrackTotalHits: true})
```

```python
client = OpenObserveClient.from_env()
client.logs.ingest_json("app", [{"message": "hi"}])
res = client.logs.search('SELECT * FROM "app"', track_total_hits=True)
```

```java
var client = OpenObserveClient.fromEnv();
client.logs().ingestJson("app", List.of(Map.of("message", "hi")));
var res = client.logs().search("SELECT * FROM \"app\"", SqlSearchOptions.builder().trackTotalHits(true).build());
```

## 4. Test code that uses the SDK

Use the in-memory fakes from the testing module instead of a real OpenObserve in unit tests:
TypeScript `@grow-codes/openobserve/testing`, Rust feature `testing`, Go package `testkit`, Python
`grow_openobserve.testing`, Java `codes.grow.packages.openobserve.testing`.

## 5. Work on this repository

Read `AGENTS.md` first. Short version: the five languages keep the same API; files with the
`Generated by sdk-kit` header are changed in sdk-kit, not here; `make lint coverage` must pass
(≥ 90 % lines and branches); `make test-docker LANGS="<lang>"` runs the integration suite against
`public.ecr.aws/zinclabs/openobserve:v1.1.2`.
