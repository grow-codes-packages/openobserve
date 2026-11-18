// Created by sdk-kit 0.1.0; owned by this repository.
package codes.grow.packages.openobserve.examples;

import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.HealthStatus;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.logs.IngestResult;
import codes.grow.packages.openobserve.logs.SqlSearchOptions;
import codes.grow.packages.openobserve.logs.SqlSearchResult;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Basics: health, ping, server version, ingest JSON records, SQL search and errors. */
public final class BasicExample {
  private BasicExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   */
  public static void main(String[] args) {
    // Reads GROW_OPENOBSERVE_* / OPENOBSERVE_* (URL, ORG, USER, PASSWORD or TOKEN, STREAM, ...).
    try (var client = OpenObserveClient.fromEnv()) {
      HealthStatus health = client.health(); // never throws for an unreachable service
      System.out.println("health: " + health);
      if (!health.healthy()) {
        return;
      }
      try {
        System.out.println("ping: " + client.ping().toMillis() + " ms");
        System.out.println("server version: " + client.serverVersion());
      } catch (AuthenticationException e) {
        // messages never contain the password or token
        System.out.println("bad credentials: " + e.getMessage());
        return;
      }

      String stream = "example_basic";
      IngestResult ingest =
          client
              .logs()
              .ingestJson(
                  stream,
                  List.of(
                      Map.of("message", "order created", "level", "info", "order_id", 42),
                      Map.of("message", "payment failed", "level", "error", "order_id", 42)));
      System.out.println("stored " + ingest.successful() + ", rejected " + ingest.failed());

      // Times are Instants; the SDK sends Unix microseconds. Quote reserved stream names.
      SqlSearchResult rows =
          client
              .logs()
              .search(
                  "SELECT * FROM \"" + stream + "\" WHERE match_all('payment') ORDER BY _timestamp",
                  SqlSearchOptions.builder()
                      .start(Instant.now().minusSeconds(600))
                      .trackTotalHits(true)
                      .build());
      System.out.println("matches: " + rows.total().orElse(-1) + " " + rows.hits());

      try {
        client.logs().search("SELECT * FROM no_such_stream");
      } catch (NotFoundException e) {
        System.out.println("not found: " + e.getMessage());
      }
    }
  }
}
