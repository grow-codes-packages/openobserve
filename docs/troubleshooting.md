# Troubleshooting

## The client cannot connect

- `health()` returns `healthy: false, status: "unreachable"`: the URL is wrong or the service is
  down. Check `GROW_OPENOBSERVE_URL` (default `http://127.0.0.1:5080`) and that the port is
  reachable from where your code runs (inside Docker, `127.0.0.1` is the container itself).
- Calls fail with `UNAVAILABLE` after several seconds: the client retried with backoff. Lower
  `maxRetries` or `totalTimeout` to fail faster.
- Behind a corporate proxy: `HTTP_PROXY`/`HTTPS_PROXY` are **ignored** on purpose. Set `proxy`
  (or `GROW_OPENOBSERVE_PROXY`).

## `CONFIG` errors

The message names the setting or environment variable, never its value. Typical causes: a URL
without `http://` or `https://`, a duration such as `10 sec` (use `10s` or `10000`), a client
certificate without its key, `MAX_RETRIES` below 0.

## `AUTHENTICATION` or `PERMISSION_DENIED`

- `ping()` is the quickest credential check: it makes one authenticated call.
- OpenObserve only accepts HTTP Basic auth. Set `OPENOBSERVE_USER` plus `OPENOBSERVE_PASSWORD`,
  or the service account's e-mail as `OPENOBSERVE_USER` plus its token as `OPENOBSERVE_TOKEN`
  (the `GROW_OPENOBSERVE_*` names win over these; explicit config values win over both).
- A service-account token works only with its own e-mail. The token shown by
  `listServiceAccounts()` is masked by the server; use the one returned by
  `createServiceAccount()` or `rotateServiceAccountToken()`.
- `organization not found` / `AUTHENTICATION` for a valid user: check `OPENOBSERVE_ORG`
  (default `default`).
- The SDK never prints credentials, so `toString()` of the config shows `[REDACTED]` even when a
  token is set.

## Search returns no rows

- Times must be epoch **microseconds**. The SDK converts `Date`/`time.Time`/`datetime`/`Instant`
  values for you; when you write `_timestamp` yourself in SQL, use microseconds.
- The default window is the last 15 minutes. Pass `start`/`end` for older data.
- Quote stream names in SQL (`SELECT * FROM "default"`), and use the normalized name (lower case,
  non `[a-z0-9_]` → `_`).
- Records rejected at ingest (for example older than the ingest window) keep HTTP 200: check
  `failed` of `ingestJson` / `errors` of `ingestBulk`.

## `NOT_FOUND` for a stream that should exist

OpenObserve creates a stream on its first write; before that, search and schema calls answer
`NOT_FOUND` (the server's HTTP 400 with code `20002`). Write a record or call `createStream` first,
and check the stream type (`logs` vs `traces`).

## TLS errors

- Self-signed or private CA: set `tls.caCertPath` (`GROW_OPENOBSERVE_CA_CERT`).
- Mutual TLS: set both `tls.clientCertPath` and `tls.clientKeyPath`.
- `tls.insecureSkipVerify` only for local tests.

## `RATE_LIMITED`

The service asked the client to slow down. The client waits for `Retry-After` up to
`maxRetryAfter` (60 s); longer waits fail at once with `retryAfter` set on the error so your code
can schedule a later attempt.

## `TIMEOUT`

`requestTimeout` (30 s) bounds one attempt and `totalTimeout` (120 s) the whole call including
retries. Raise them for slow operations, or pass a per-call deadline (signal, ctx, cancel token,
future timeout).

## See what the client does

Turn on the [logger](hooks.md#logger) or add [hooks](hooks.md#hooks): both see every attempt with
redacted headers and URLs.

## Docker integration tests fail

- Run one language: `make test-docker LANGS=go`.
- In CI, download the `compose-logs-<lang>` artifact of the failed job.
- Locally, the stack is removed after each run; rerun with Docker logs open
  (`docker compose -f docker/compose.yml logs -f`) in a second terminal.

## Still stuck?

Open an issue with the language, package version, OpenObserve version and a minimal reproduction
(with secrets removed): <https://github.com/grow-codes-packages/openobserve/issues>.
