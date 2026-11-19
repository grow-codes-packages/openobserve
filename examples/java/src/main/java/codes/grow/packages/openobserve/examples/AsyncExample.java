// Created by sdk-kit 0.1.0; owned by this repository.
package codes.grow.packages.openobserve.examples;

import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.HealthStatus;
import codes.grow.packages.openobserve.logs.SqlSearchOptions;
import codes.grow.packages.openobserve.logs.SqlSearchResult;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** The {@code …Async} methods: run calls concurrently, chain them and cancel a slow one. */
public final class AsyncExample {
  private AsyncExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   */
  public static void main(String[] args) {
    try (var client = OpenObserveClient.fromEnv()) {
      CompletableFuture<HealthStatus> health = client.healthAsync();
      CompletableFuture<Duration> ping = client.pingAsync();
      CompletableFuture.allOf(health, ping.exceptionally(e -> null)).join();
      System.out.println("health: " + health.join());
      ping.whenComplete(
          (latency, error) ->
              System.out.println(error == null ? "ping: " + latency : "ping failed: " + error));

      // Ingest, then search the same stream once the write completed.
      CompletableFuture<SqlSearchResult> rows =
          client
              .logs()
              .ingestJsonAsync("example_async", List.of(Map.of("message", "async hello")))
              .thenCompose(
                  r ->
                      client
                          .logs()
                          .searchAsync(
                              "SELECT * FROM \"example_async\"", SqlSearchOptions.defaults()));
      System.out.println("rows: " + rows.join().hits().size());

      // Cancelling a future aborts the HTTP exchange and any pending retry.
      CompletableFuture<String> version = client.serverVersionAsync();
      version.cancel(true);
      System.out.println("version cancelled: " + version.isCancelled());
    }
  }
}
