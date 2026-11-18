# grow-openobserve (Python)

Python SDK for [OpenObserve](https://grow-codes-packages.github.io/openobserve/) by Grow Codes:
logs ingestion and SQL search, stream management, OpenTelemetry tracing export and trace
queries, admin helpers, plus the Grow logging/tracing category interfaces.
Unofficial and community maintained; "OpenObserve" is a trademark of ZincLabs, Inc.

```bash
pip install grow-openobserve
```

```python
from grow_openobserve import OpenObserveClient

# Reads OPENOBSERVE_URL, OPENOBSERVE_ORG, OPENOBSERVE_USER, OPENOBSERVE_PASSWORD (or
# OPENOBSERVE_TOKEN), OPENOBSERVE_STREAM ... (GROW_OPENOBSERVE_* wins) from the environment.
with OpenObserveClient.from_env() as client:
    print(client.health(), client.server_version())
    client.logs.ingest_json("app", [{"message": "hello", "level": "info"}])
    page = client.logs.search('SELECT * FROM "app" ORDER BY _timestamp DESC', size=10)
    print(page.hits)
```

| Module | Functions |
| - | - |
| `client.logs` | `ingest_json`, `ingest_multi`, `ingest_bulk`, `ingest_otlp`, `search`, `search_around`, `values_of`, `histogram`, `tail` |
| `client.streams` | `list_streams`, `get_stream_schema`, `update_stream_settings`, `create_stream`, `delete_stream` |
| `client.traces` / `client.trace_query(stream)` | `get_trace`, `search_traces`, `list_services`, `list_operations`, `get_dependencies` (unsupported in OSS), `wait_for_trace`, `trace_time_range` |
| `client.admin` | `list_organizations`, `create_service_account`, `list_service_accounts`, `rotate_service_account_token`, `delete_service_account`, `get_passcode` |
| logging category | `client.log_client(stream)` (batching `LogClient`, `Logger`, `LogHandler`, structlog processor), `client.log_query(stream)` (`LogQueryClient`) |
| tracing category | `init_tracing(TracingConfig, openobserve=config)` or `client.init_tracing(...)`, span helpers, ASGI/WSGI/httpx/requests adapters |

The async client has the same modules and methods:

```python
import asyncio
from grow_openobserve import AsyncOpenObserveClient

async def main() -> None:
    async with AsyncOpenObserveClient.from_env() as client:
        print(await client.logs.search('SELECT count(*) AS n FROM "app"'))

asyncio.run(main())
```

- Python 3.10 or newer, typed (`py.typed`), `httpx` based, environment proxies ignored unless
  configured.
- OpenObserve accepts HTTP Basic only: `user` + `password`, or a service-account `user` +
  `token`; a `token` starting with `Basic ` is sent verbatim.
- Times are aware `datetime`s or epoch **microseconds** (`int`); the SDK converts them to the
  server's microseconds (milliseconds would silently match nothing).
- Retries with full-jitter backoff and `Retry-After`, connect/request/total timeouts,
  cancellation, hooks and a silent-by-default `logging` logger named `grow_openobserve`.
- Credentials (and tokens returned by the server) are wrapped in `Secret` and never appear in
  `repr()`, logs or error messages.
- `grow_openobserve.testing` ships in-memory fakes and the conformance runner.

Documentation: https://grow-codes-packages.github.io/openobserve/ ·
Source: https://github.com/grow-codes-packages/openobserve · Licence: Apache-2.0
