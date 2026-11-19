# Compatibility

## Runtimes

| Language | Supported | Tested in CI |
| - | - | - |
| TypeScript | Node.js ≥ 22.12, ESM and CommonJS | Node 22, 24 |
| Rust | MSRV 1.88, edition 2021, tokio | stable, 1.88 |
| Go | Go ≥ 1.26 | 1.26, 1.27 |
| Python | ≥ 3.10 | 3.10, 3.13, 3.14 |
| Java | ≥ 17 (Java 17 bytecode) | Temurin 17, 21, 25 |

## OpenObserve versions

The integration suite runs against `public.ecr.aws/zinclabs/openobserve:v1.1.2` (open-source
build, local mode, disk storage), the version pinned in the Grow Codes stack catalog. The SDK only
uses the HTTP API and OTLP. Other versions that expose the same API are expected to work; report
problems as an issue.

## Functions covered by unit tests only

Every public function is called against the real service in the Docker suite. These cases cannot
be produced by the open-source container and are covered by unit tests with recorded or synthetic
responses:

| Function / case | Languages | Reason |
| - | - | - |
| `getDependencies` | all | the service graph is an enterprise feature; OSS answers 404, so the SDK raises `UNSUPPORTED` without a request |
| OTLP `partialSuccess` (`ingestOtlp`, OTLP log exporter) | all | the container never reports rejected OTLP records |
| retries on 429, 5xx and `Retry-After` | all | the container never throttles |
| TLS (CA bundle, client certificates) and proxies | all | the test stack is plain HTTP on a private network |
| OTLP/gRPC trace exporter (port 5081) | all | the exporter is built and its metadata checked; spans are exported over OTLP/HTTP in the suite |

## Server behaviour worth knowing (v1.1.2)

| Behaviour | What the SDK does |
| - | - |
| Only HTTP Basic auth works; `Bearer` is rejected for every credential type | `token` is sent as `Basic base64(user:token)`; a value starting with `Basic ` is sent as is |
| `GET /service_accounts` returns a masked token that does not authenticate | tokens come only from `createServiceAccount` / `rotateServiceAccountToken` |
| Search `total` is the number of rows in the page | `trackTotalHits` runs a second count query with `from: 0` (with the page's `from` the count row would be skipped) |
| Times in milliseconds silently return no rows | every time is converted to microseconds |
| `start_time=0` is rejected with HTTP 400 | the tail never queries before its `start` (the overlap window is clamped) |
| `_around` accepts a `sql` parameter but ignores it | TypeScript, Python and Rust pass `sql` through (documented as ignored); Go and Java do not offer it |
| `traces/{id}/details` finds a trace through the trace index even with an old explicit window, but answers 400 for an unknown trace without a window | `getTrace` always sends a window (default 7 days) and returns `null` for unknown traces |
| A missing stream answers HTTP 400 with vendor code `20002` | mapped to `NOT_FOUND`; "already exists" (400) is mapped to `CONFLICT` |
| Ingest partial failures and `_bulk` item errors keep HTTP 200 | results expose `failed` / `errors` / `rejected` |
| `_bulk` silently drops `delete` and `update` actions | the SDK rejects them client-side with `VALIDATION` |
| Span `start_time`/`end_time` are nanoseconds, `duration` is microseconds; attributes are flattened and stringified, events and links are JSON strings | normalized into the shared `Span` model |
| A filtered `traces/latest` summary covers only the spans that match the filter | documented on `searchTraces` |
| OTLP log trace flags are not stored | the OTLP log exporter cannot round-trip `traceFlags` (the `_json` exporter stores them) |
| Stream and field names are normalized (lower case, non `[a-z0-9_]` → `_`) | the SDK normalizes names the same way before sending |

## Skipped conformance scenarios

| Scenario | Reason |
| - | - |
| — | none: core, logging (batch, redaction, round trip) and tracing (tree, services) pass in all five languages |

## Language differences

The API is the same in every language; only the conventions differ (naming, durations, async
model). Deliberate differences:

| Topic | Difference |
| - | - |
| Tracing `native` protocol | OTLP/HTTP **JSON** in TypeScript and Rust; OTLP/HTTP **protobuf** in Go, Python and Java, whose official exporters have no JSON encoding. Both reach the same endpoint and store the same data. |
| Explicit list deltas in `updateStreamSettings` | TypeScript (`{ add, remove }`), Python (`ListDelta`) and Rust (`ListUpdate::Delta`) can send a delta as given; Go and Java take the wanted list and always compute the delta. |
| Time windows on `getTrace`, `listServices`, `listOperations` | TypeScript, Python and Rust accept `start`/`end`; Go and Java use the defaults (7 days, 24 h). |
| Stream type on `searchAround`, `valuesOf`, `histogram` | TypeScript, Python and Rust accept a stream type; Go and Java query log streams. |
| `total` in `LogQueryClient.search` | TypeScript (`trackTotalHits` option), Python (`track_total_hits=`) and Rust (`search_with(…, true)`) can fill `total`; in Go and Java the category `search` leaves it empty — use `logs.search(sql, trackTotalHits)` there. |
| Log client construction | Go uses `NewLogClient(stream, opts)` and `NewLogClientWith(exporter, opts)` with `NewJSONLogExporter` / `NewOTLPLogExporter`; the other languages take the protocol as an option. |
| Raw request bodies | Rust has `ingest_multi_records` and `ingest_bulk_raw` next to the typed calls; TypeScript and Python accept a string or a list in `ingestMulti`/`ingestBulk`. |
| Rust `blocking` feature | `blocking::OpenObserveClient` wraps the common calls (health, ping, version, ingest, search, streams, traces, organizations); everything else is reachable through `async_client()` + `block_on`. |
| Search page size | The default is 100 everywhere. `-1` asks for the server's own limit in every language; `0` does too, except in Go, where the zero value means "default 100". |
| Environment loading in Java | Java cannot change the process environment, so `loadEnv` sets **system properties** instead and returns the names it set. |
