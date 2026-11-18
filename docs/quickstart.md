# Quickstart

Connect to OpenObserve, check that it is healthy, make an authenticated call and close the client.
The client reads `GROW_OPENOBSERVE_URL` and `GROW_OPENOBSERVE_TOKEN` (and the vendor variables
listed in [Configuration](configuration.md)), or takes the values in code.

```sh
export GROW_OPENOBSERVE_URL=http://127.0.0.1:5080
export GROW_OPENOBSERVE_TOKEN=...   # never commit real tokens
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

## Next steps

- [Configuration](configuration.md): timeouts, retries, TLS, proxy, environment variables.
- [Functions](functions.md): the OpenObserve API of this SDK.
- [Error handling](errors.md): error codes and retries.
