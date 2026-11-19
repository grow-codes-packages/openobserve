# Functions

The OpenObserve API of this SDK. Every function exists in all five languages with the same
behaviour, defaults and error codes; names follow each language's convention. Each module page
shows every function with an example per language and the request it sends.

| Module | Page | What it covers |
| - | - | - |
| Client and status | [Client and status](functions/client.md) | `fromEnv`, authentication, `health`, `ping`, `serverVersion`, `close` |
| Logs ingestion | [Ingest](functions/ingest.md) | `ingestJson`, `ingestMulti`, `ingestBulk`, `ingestOtlp` |
| Search | [Search and tail](functions/search.md) | `search` (+ `trackTotalHits`), `searchAround`, `valuesOf`, `histogram`, `tail` |
| Streams | [Streams](functions/streams.md) | `listStreams`, `getStreamSchema`, `updateStreamSettings`, `createStream`, `deleteStream` |
| Tracing | [Traces](functions/traces.md) | `initTracing` / `client.initTracing`, `getTrace`, `searchTraces`, `listServices`, `listOperations`, `getDependencies`, `waitForTrace`, `traceTimeRange` |
| Logging category | [Log client and search](functions/logging.md) | `logClient`, `logExporter`, `logQuery` (`search`, `tail`, `listStreams`, `listLabels`, `waitForLog`) |
| Admin helpers | [Admin](functions/admin.md) | `listOrganizations`, service accounts, `getPasscode` |

## Names per language

| Function | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| from environment | `OpenObserveClient.fromEnv()` | `OpenObserveClient::from_env()` | `openobserve.NewClientFromEnv()` | `OpenObserveClient.from_env()` | `OpenObserveClient.fromEnv()` |
| health | `health()` | `health()` | `Health(ctx)` | `health()` | `health()` |
| ping | `ping()` → ms | `ping()` → `Duration` | `Ping(ctx)` → `time.Duration` | `ping()` → ms | `ping()` → `Duration` |
| server version | `serverVersion()` | `server_version()` | `ServerVersion(ctx)` | `server_version()` | `serverVersion()` |
| logs module | `client.logs` | `client.logs()` | `client.Logs()` | `client.logs` | `client.logs()` |
| JSON ingest | `ingestJson(stream, records)` | `ingest_json(stream, &records, opts)` | `IngestJSON(ctx, stream, records)` | `ingest_json(stream, records)` | `ingestJson(stream, records)` |
| NDJSON ingest | `ingestMulti(stream, ndjson)` | `ingest_multi(stream, ndjson, opts)` / `ingest_multi_records` | `IngestMulti(ctx, stream, ndjson)` | `ingest_multi(stream, ndjson)` | `ingestMulti(stream, ndjson)` |
| bulk ingest | `ingestBulk(actions)` | `ingest_bulk(&actions, opts)` / `ingest_bulk_raw` | `IngestBulk(ctx, actions)` | `ingest_bulk(actions)` | `ingestBulk(actions)` |
| OTLP ingest | `ingestOtlp(records, { stream })` | `ingest_otlp(&records, opts)` | `IngestOTLP(ctx, stream, records, gzip)` | `ingest_otlp(records, stream=)` | `ingestOtlp(stream, records, gzip)` |
| SQL search | `search(sql, opts)` | `search(sql, opts)` | `Search(ctx, sql, opts)` | `search(sql, ...)` | `search(sql, opts)` |
| around | `searchAround(stream, ts, opts)` | `search_around(stream, ts, opts)` | `SearchAround(ctx, stream, ts, size)` | `search_around(stream, ts, size=)` | `searchAround(stream, ts, size)` |
| values | `valuesOf(stream, fields, opts)` | `values_of(stream, &fields, opts)` | `ValuesOf(ctx, stream, fields, opts)` | `values_of(stream, fields)` | `valuesOf(stream, fields, opts)` |
| histogram | `histogram(stream, opts)` | `histogram(stream, opts)` | `Histogram(ctx, stream, opts)` | `histogram(stream, ...)` | `histogram(stream, opts)` |
| tail | `tail(stream, onRow, opts)` | `tail(stream, on_row, opts)` | `Tail(ctx, stream, onRow, opts)` | `tail(stream, on_row, ...)` | `tail(stream, onRow, opts)` |
| streams module | `client.streams` | `client.streams()` | `client.Streams()` | `client.streams` | `client.streams()` |
| list streams | `listStreams(opts)` | `list_streams(type, opts)` | `ListStreams(ctx, opts)` | `list_streams(type)` | `listStreams(type)` |
| schema | `getStreamSchema(stream)` | `get_stream_schema(stream, type)` | `GetStreamSchema(ctx, stream, type)` | `get_stream_schema(stream)` | `getStreamSchema(stream)` |
| settings | `updateStreamSettings(stream, update)` | `update_stream_settings(stream, update, type)` | `UpdateStreamSettings(ctx, stream, update)` | `update_stream_settings(stream, ...)` | `updateStreamSettings(stream, update)` |
| create | `createStream(stream, opts)` | `create_stream(stream, opts)` | `CreateStream(ctx, stream, opts)` | `create_stream(stream, ...)` | `createStream(stream, opts)` |
| delete | `deleteStream(stream)` | `delete_stream(stream, type)` | `DeleteStream(ctx, stream, type)` | `delete_stream(stream)` | `deleteStream(stream)` |
| tracing setup | `initTracing(cfg)` / `client.initTracing(cfg)` | `init_tracing(cfg)` / `client.init_tracing(cfg, stream)` | `InitTracing(ctx, cfg, tc)` / `client.InitTracing(ctx, tc)` | `init_tracing(cfg)` / `client.init_tracing(cfg)` | `OpenObserveTracing.initTracing(cfg, tc)` / `client.initTracing(tc)` |
| trace query | `client.traces` / `traceQuery(stream)` | `client.traces()` / `trace_query(stream)` | `client.Traces()` / `TraceQuery(stream)` | `client.traces` / `trace_query(stream)` | `client.traces()` / `traceQuery(stream)` |
| log client | `logClient(opts)` | `log_client(opts)` | `NewLogClient(stream, opts)` / `NewLogClientWith(exporter, opts)` | `log_client(stream, ...)` | `logClient(opts)` |
| log exporter | `logExporter(stream, protocol)` | `log_exporter(stream, protocol)` | `NewJSONLogExporter(stream)` / `NewOTLPLogExporter(stream)` | `log_exporter(stream, protocol=)` | `logExporter(stream, otlp)` |
| log query | `logQuery(stream)` | `log_query(stream)` | `LogQuery(stream)` | `log_query(stream)` | `logQuery(stream)` |
| admin module | `client.admin` | `client.admin()` | `client.Admin()` | `client.admin` | `client.admin()` |

Every I/O method takes a cancellation handle: TypeScript `{ signal }`, Rust dropping the future,
Go `ctx`, Python `cancel=` (or the `AsyncOpenObserveClient`), Java the `…Async` variant.

## Wire facts that apply everywhere

- **Time is microseconds.** Search windows, `_timestamp` values and cursors are epoch µs. The SDK
  converts `Date`/`SystemTime`/`time.Time`/`datetime`/`Instant` for you; OpenObserve silently returns
  nothing for millisecond values.
- **Stream names are normalized** like the server does (lower case, non `[a-z0-9_]` → `_`).
- **HTTP 200 does not mean every record was stored**: check `failed` (`ingestJson`), `errors`
  (`ingestBulk`) or `rejected` (`ingestOtlp`).
- **Error mapping**: a missing stream answers HTTP 400 with vendor code `20002`; the SDK raises
  `NOT_FOUND`. "already exists" (HTTP 400) becomes `CONFLICT`. The search `trace_id` of the server
  is the error's `requestId`.
