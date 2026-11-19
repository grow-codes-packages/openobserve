# Client and status

The client holds one connection pool and the configuration. Build it from the environment or from
an explicit config; it is safe to share between threads/tasks.

| Function | Request | Result |
| - | - | - |
| `health()` | `GET /healthz` (no auth) | `HealthStatus` (`healthy`, `status`, `details`); never throws for an unhealthy or unreachable service |
| `ping()` | `GET /api/{org}/config` (auth) | latency; `AUTHENTICATION` on bad or missing credentials |
| `serverVersion()` | `GET /api/{org}/config` (auth) | `version`, e.g. `v1.1.2`; `UNSUPPORTED` when the server reports none |
| `close()` | — | idempotent; flushes and closes every log client made by `logClient`, then releases connections |

## Authentication

OpenObserve accepts **HTTP Basic** only:

| Configured | Header sent |
| - | - |
| `user` + `password` | `Basic base64(user:password)` |
| `user` + `token` (service-account token) | `Basic base64(user:token)` |
| `token` starting with `Basic ` | the value as is |
| nothing | no header (only `health()` works) |

`password` and `token` are mutually exclusive and `user` needs one of them (`CONFIG` otherwise).
A service-account token is returned only by `createServiceAccount` / `rotateServiceAccountToken`
([Admin](admin.md)); the list call returns a masked token that does not authenticate.

=== "TypeScript"

    ```ts
    import { OpenObserveClient } from "@grow-codes/openobserve";

    const client = new OpenObserveClient({
      url: "https://openobserve.example.com",
      org: "default",
      user: "ci@example.com",
      token: process.env.OO_TOKEN, // or password: ...
    });
    const health = await client.health(); // { healthy: true, status: "ok", details: {} }
    const ms = await client.ping();
    const version = await client.serverVersion(); // "v1.1.2"
    await client.close();
    ```

=== "Rust"

    ```rust
    use grow_openobserve::{OpenObserveClient, OpenObserveConfig};

    let config = OpenObserveConfig::builder()
        .url("https://openobserve.example.com")
        .org("default")
        .user("ci@example.com")
        .token(std::env::var("OO_TOKEN").unwrap_or_default())
        .build()?;
    let client = OpenObserveClient::new(config)?;
    let health = client.health().await?;
    let latency = client.ping().await?;
    let version = client.server_version().await?;
    client.close().await;
    ```

=== "Go"

    ```go
    client, err := openobserve.NewClient(openobserve.Config{
        CommonConfig: openobserve.CommonConfig{URL: "https://openobserve.example.com"},
        Org:          "default",
        User:         "ci@example.com",
        Token:        openobserve.NewSecret(os.Getenv("OO_TOKEN")),
    })
    if err != nil {
        return err // errors.Is(err, openobserve.ErrConfig)
    }
    defer client.Close()
    health, _ := client.Health(ctx)
    latency, err := client.Ping(ctx)
    version, err := client.ServerVersion(ctx)
    ```

=== "Python"

    ```python
    import os
    from grow_openobserve import OpenObserveClient, OpenObserveConfig

    config = OpenObserveConfig(
        url="https://openobserve.example.com",
        org="default",
        user="ci@example.com",
        token=os.environ["OO_TOKEN"],
    )
    with OpenObserveClient(config) as client:
        health = client.health()
        ms = client.ping()
        version = client.server_version()
    ```

=== "Java"

    ```java
    OpenObserveConfig config = OpenObserveConfig.builder()
        .url("https://openobserve.example.com")
        .org("default")
        .user("ci@example.com")
        .token(System.getenv("OO_TOKEN"))
        .build();
    try (OpenObserveClient client = OpenObserveClient.create(config)) {
        HealthStatus health = client.health();
        Duration latency = client.ping();
        String version = client.serverVersion();
    }
    ```

## Modules

| Module | TypeScript | Rust | Go | Python | Java |
| - | - | - | - | - | - |
| logs | `client.logs` | `client.logs()` | `client.Logs()` | `client.logs` | `client.logs()` |
| streams | `client.streams` | `client.streams()` | `client.Streams()` | `client.streams` | `client.streams()` |
| traces (configured `tracesStream`) | `client.traces` | `client.traces()` | `client.Traces()` | `client.traces` | `client.traces()` |
| admin | `client.admin` | `client.admin()` | `client.Admin()` | `client.admin` | `client.admin()` |

The shared client surface (hooks, logger, retries, errors) is described in [Client](../client.md),
[Error handling](../errors.md) and [Hooks and logger](../hooks.md).
