# Quickstart

Connect to OpenObserve, check that it is healthy, make an authenticated call and close the client.
The client reads `OPENOBSERVE_*` / `GROW_OPENOBSERVE_*` variables (see
[Configuration](configuration.md)) or takes the values in code. OpenObserve uses HTTP Basic auth:
a user e-mail plus its password, or a service-account e-mail plus its token.

```sh
export OPENOBSERVE_URL=http://127.0.0.1:5080
export OPENOBSERVE_ORG=default
export OPENOBSERVE_USER=root@example.com
export OPENOBSERVE_PASSWORD=...     # or OPENOBSERVE_TOKEN=<service-account token>; never commit real secrets
```

=== "TypeScript"

    ```ts
    import { OpenObserveClient } from "@grow-codes/openobserve";

    const client = OpenObserveClient.fromEnv();
    try {
      const health = await client.health();
      console.log("healthy:", health.healthy, health.status);
      console.log("ping:", await client.ping(), "ms");
      console.log("server version:", await client.serverVersion());
    } finally {
      await client.close();
    }
    ```

=== "Rust"

    ```rust
    use grow_openobserve::OpenObserveClient;

    #[tokio::main]
    async fn main() -> grow_openobserve::Result<()> {
        let client = OpenObserveClient::from_env()?;
        let health = client.health().await?;
        println!("healthy: {} {}", health.healthy, health.status);
        println!("ping: {:?}", client.ping().await?);
        println!("server version: {}", client.server_version().await?);
        client.close().await;
        Ok(())
    }
    ```

=== "Go"

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

        health, err := client.Health(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("healthy:", health.Healthy, health.Status)

        latency, err := client.Ping(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("ping:", latency)

        version, err := client.ServerVersion(ctx)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println("server version:", version)
    }
    ```

=== "Python"

    ```python
    from grow_openobserve import OpenObserveClient

    with OpenObserveClient.from_env() as client:
        health = client.health()
        print("healthy:", health.healthy, health.status)
        print("ping:", client.ping(), "ms")
        print("server version:", client.server_version())
    ```

    Async code uses `AsyncOpenObserveClient` with the same methods:

    ```python
    import asyncio
    from grow_openobserve import AsyncOpenObserveClient

    async def main() -> None:
        async with AsyncOpenObserveClient.from_env() as client:
            print(await client.health())

    asyncio.run(main())
    ```

=== "Java"

    ```java
    import codes.grow.packages.openobserve.OpenObserveClient;
    import codes.grow.packages.openobserve.core.HealthStatus;

    public final class Quickstart {
      public static void main(String[] args) {
        try (OpenObserveClient client = OpenObserveClient.fromEnv()) {
          HealthStatus health = client.health();
          System.out.println("healthy: " + health.healthy() + " " + health.status());
          System.out.println("ping: " + client.ping().toMillis() + " ms");
          System.out.println("server version: " + client.serverVersion());
        }
      }
    }
    ```

## Write and read logs, send a trace

The [Functions](functions.md) pages show every call; the README quickstart writes two records,
reads them back with SQL and sends a trace in each language. The runnable programs in
[`examples/`](https://github.com/grow-codes-packages/openobserve/tree/main/examples) do the same
end to end.

## Next steps

- [Configuration](configuration.md): timeouts, retries, TLS, proxy, environment variables.
- [Functions](functions.md): the OpenObserve API of this SDK.
- [Error handling](errors.md): error codes and retries.
