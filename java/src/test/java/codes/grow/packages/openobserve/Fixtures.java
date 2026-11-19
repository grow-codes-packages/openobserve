package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.RetryPolicy;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Recorded OpenObserve v1.1.2 responses (src/test/resources/fixtures) for mock servers. */
public final class Fixtures {
  /** Test user. */
  public static final String USER = "root@example.com";

  /** Test password (redaction checks look for it). */
  public static final String PASSWORD = "pw-s3cr3t-value";

  private Fixtures() {}

  /**
   * Loads a fixture file.
   *
   * @param name fixture name without {@code .json}
   * @return the fixture
   */
  public static JsonNode load(String name) {
    try (InputStream in = Fixtures.class.getResourceAsStream("/fixtures/" + name + ".json")) {
      if (in == null) {
        throw new IllegalArgumentException("no fixture " + name);
      }
      return Json.tree(in.readAllBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns the response body of a fixture as text (JSON re-encoded, strings verbatim).
   *
   * @param name fixture name
   * @return body text
   */
  public static String body(String name) {
    JsonNode body = load(name).path("response").path("body");
    return body.isTextual() ? body.asText() : body.isNull() ? "" : body.toString();
  }

  /**
   * Queues the recorded response of a fixture.
   *
   * @param server mock server
   * @param name fixture name
   * @return the server
   */
  public static MockServer enqueue(MockServer server, String name) {
    JsonNode resp = load(name).path("response");
    List<String> headers = new ArrayList<>();
    for (Map.Entry<String, JsonNode> e : resp.path("headers").properties()) {
      headers.add(e.getKey().equalsIgnoreCase("content-type") ? "Content-Type" : e.getKey());
      headers.add(e.getValue().asText());
    }
    return server.enqueue(resp.path("status").asInt(), body(name), headers.toArray(new String[0]));
  }

  /**
   * A client for a mock server with fast retries.
   *
   * @param server mock server
   * @return client builder
   */
  public static OpenObserveClient.Builder client(MockServer server) {
    return OpenObserveClient.builder()
        .url(server.url())
        .user(USER)
        .password(PASSWORD)
        .retry(RetryPolicy.builder().initialBackoff(Duration.ofMillis(1)).build());
  }
}
