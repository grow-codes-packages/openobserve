# Python examples

Install the package, point it at a running OpenObserve and run an example:

```bash
pip install grow-openobserve
export OPENOBSERVE_URL=http://127.0.0.1:5080
export OPENOBSERVE_USER=root@example.com
export OPENOBSERVE_PASSWORD=...     # or OPENOBSERVE_TOKEN=<service-account token>
python examples/python/basic.py
```

| File | Shows |
| - | - |
| `basic.py` | sync client: health, ping, server version, error handling |
| `async_basic.py` | async client, cancellation by task |
| `hooks_and_logging.py` | request hooks, the `grow_openobserve` logger, retries and timeouts |
| `logs.py` | ingest, SQL search, values, batching log client + `Logger` + `logging` handler, log query, stream settings |
| `tracing.py` | `init_tracing` to OpenObserve, span helpers, reading the trace back with `client.traces` |
| `admin.py` | organizations, a temporary service account used as Basic credentials |
