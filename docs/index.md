# OpenObserve SDK

[![Grow Codes](https://img.shields.io/badge/Grow%20Codes-grow.codes-2ea44f)](https://grow.codes)
[![CI](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml/badge.svg)](https://github.com/grow-codes-packages/openobserve/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](license.md)

One API for **OpenObserve** in five languages: TypeScript, Rust, Go, Python and Java. Every package
has the same functions, options, defaults, error codes and behaviour, tested against a real
`public.ecr.aws/zinclabs/openobserve:v1.1.2` in Docker.

| Language | Package | Coverage |
| - | - | - |
| TypeScript | [`@grow-codes/openobserve`](https://www.npmjs.com/package/@grow-codes/openobserve) | ![TypeScript coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-typescript.json) |
| Rust | [`grow-openobserve`](https://crates.io/crates/grow-openobserve) | ![Rust coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-rust.json) |
| Go | [`github.com/grow-codes-packages/openobserve/go`](https://pkg.go.dev/github.com/grow-codes-packages/openobserve/go) | ![Go coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-go.json) |
| Python | [`grow-openobserve`](https://pypi.org/project/grow-openobserve/) | ![Python coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-python.json) |
| Java | [`codes.grow.packages:openobserve`](https://central.sonatype.com/artifact/codes.grow.packages/openobserve) | ![Java coverage](https://img.shields.io/endpoint?url=https://grow-codes-packages.github.io/openobserve/badges/coverage-java.json) |

## What you get

- **Client** with health, authenticated ping and server version ([Client](client.md)).
- **Configuration** from code or `GROW_OPENOBSERVE_*` / vendor environment variables, immutable
  and validated ([Configuration](configuration.md)).
- **Retries** with full-jitter backoff and `Retry-After`, connect/request/total timeouts and
  cancellation in every language.
- **Typed errors** with one set of codes across languages ([Error handling](errors.md)).
- **Redaction everywhere**: credentials never appear in errors, logs, hooks or `toString`
  ([Redaction](redaction.md)).
- **Logging**: batching log client, logger facade, redaction processor, framework adapters and
  log search ([Logging](logging.md)).
- **Tracing**: OpenTelemetry setup, span helpers, propagation, framework adapters and trace
  queries ([Tracing](tracing.md)).
- **Service functions** for the OpenObserve API ([Functions](functions.md)).

Start with [Install](install.md) and the [Quickstart](quickstart.md).

!!! note "Independent project"
    This SDK is made by [Grow Codes](https://grow.codes). OpenObserve is a trademark of
    ZincLabs, Inc.. This project is not affiliated with or endorsed by ZincLabs, Inc.. See
    [License & disclaimer](license.md).
