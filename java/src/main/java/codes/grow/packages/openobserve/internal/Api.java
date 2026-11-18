package codes.grow.packages.openobserve.internal;

import codes.grow.packages.openobserve.OpenObserveConfig;
import codes.grow.packages.openobserve.core.ErrorCode;
import codes.grow.packages.openobserve.core.HttpTransport;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.OpenObserveException;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.TransportResponse;
import codes.grow.packages.openobserve.core.UnavailableException;
import codes.grow.packages.openobserve.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Authenticated request helper shared by the OpenObserve service modules: adds the Basic {@code
 * Authorization} header, maps OpenObserve error bodies (JSON or plain text, vendor codes on HTTP
 * 400) to SDK exceptions and keeps the secret values that error messages must never contain.
 *
 * <p>Internal API: it may change without notice; use the service modules of {@code
 * OpenObserveClient} instead.
 */
public final class Api {
  /** Vendor code of "stream not found" (reported with HTTP 400). */
  static final String VENDOR_STREAM_NOT_FOUND = "20002";

  /** Client error statuses mapped here (the transport handles 408, 429 and 5xx). */
  private static final int[] MAPPED_STATUSES = {400, 401, 403, 404, 405, 409, 412, 413, 415, 422};

  private final OpenObserveConfig config;
  private final HttpTransport transport;
  private final Set<String> runtimeSecrets;

  /**
   * Creates the helper.
   *
   * @param config validated configuration
   * @param transport the client's transport
   * @param runtimeSecrets mutable set of secrets learned at run time (service-account tokens)
   */
  public Api(OpenObserveConfig config, HttpTransport transport, Set<String> runtimeSecrets) {
    this.config = config;
    this.transport = transport;
    this.runtimeSecrets = runtimeSecrets;
  }

  /**
   * Creates a helper with its own runtime secret set (tests and exporters).
   *
   * @param config validated configuration
   * @param transport transport
   */
  public Api(OpenObserveConfig config, HttpTransport transport) {
    this(config, transport, ConcurrentHashMap.newKeySet());
  }

  /**
   * Returns the configuration.
   *
   * @return the configuration
   */
  public OpenObserveConfig config() {
    return config;
  }

  /**
   * Returns the transport.
   *
   * @return the transport
   */
  public HttpTransport transport() {
    return transport;
  }

  /**
   * Returns {@code /api/<org><suffix>}.
   *
   * @param suffix path suffix starting with {@code /}
   * @return the path
   */
  public String orgPath(String suffix) {
    return "/api/" + config.org() + suffix;
  }

  /**
   * Remembers a secret learned at run time so error messages redact it.
   *
   * @param secret value (ignored when shorter than 4 characters)
   */
  public void addSecret(String secret) {
    if (secret != null && secret.length() >= 4) {
      runtimeSecrets.add(secret);
    }
  }

  /**
   * Returns every secret value to redact (configuration and run time).
   *
   * @return secret values
   */
  public List<String> secrets() {
    List<String> out = new ArrayList<>(config.knownSecrets());
    out.addAll(runtimeSecrets);
    return out;
  }

  /**
   * Redacts every known secret from a text.
   *
   * @param text text
   * @return the redacted text
   */
  public String redact(String text) {
    return codes.grow.packages.openobserve.core.Redaction.redactText(text, secrets());
  }

  /**
   * Sends an authenticated request; OpenObserve error responses fail the future with the mapped
   * exception.
   *
   * @param request request builder (operation set)
   * @return a future with the 2xx response
   */
  public CompletableFuture<TransportResponse> sendAsync(TransportRequest.Builder request) {
    TransportRequest req =
        request
            .header("Authorization", config.authorization())
            .acceptStatus(MAPPED_STATUSES)
            .build();
    return transport
        .sendAsync(req)
        .thenApply(
            r -> {
              if (r.status() >= 400) {
                throw mapError(req.operation(), r, secrets());
              }
              return r;
            });
  }

  /**
   * Sends an authenticated request and decodes the JSON body.
   *
   * @param request request builder (operation set)
   * @return a future with the JSON tree
   */
  public CompletableFuture<JsonNode> jsonAsync(TransportRequest.Builder request) {
    TransportRequest probe = request.build();
    String op = probe.operation();
    return sendAsync(request).thenApply(r -> json(op, r));
  }

  /**
   * Decodes a response body as JSON; an empty or invalid body is an {@link UnavailableException}.
   *
   * @param operation operation name
   * @param r response
   * @return the JSON tree
   */
  public static JsonNode json(String operation, TransportResponse r) {
    try {
      return Json.tree(r.body());
    } catch (ValidationException e) {
      throw new UnavailableException(
          operation + ": invalid JSON response (HTTP " + r.status() + ")",
          r.status(),
          r.requestId().orElse(null),
          null,
          null);
    }
  }

  /**
   * Waits for a future and unwraps SDK exceptions.
   *
   * @param <T> result type
   * @param future future
   * @param operation operation name
   * @return the result
   */
  public static <T> T await(CompletableFuture<T> future, String operation) {
    return HttpTransport.await(future, operation);
  }

  /**
   * Runs a future-producing call; a synchronous exception (for example a {@code
   * ValidationException} from argument checks) becomes a failed future.
   *
   * @param <T> result type
   * @param call the call
   * @return the call's future, or a failed future
   */
  public static <T> CompletableFuture<T> defer(Supplier<CompletableFuture<T>> call) {
    try {
      return call.get();
    } catch (RuntimeException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Posts one {@code _search} request ({@code POST /api/{org}/_search?type=<type>}).
   *
   * @param operation operation name
   * @param sql SQL
   * @param startMicros window start (Unix microseconds)
   * @param endMicros window end (Unix microseconds)
   * @param from row offset
   * @param size page size (0 = server default)
   * @param trackTotalHits ask for the count form
   * @param type stream type wire value
   * @return a future with the response envelope
   */
  public CompletableFuture<JsonNode> searchAsync(
      String operation,
      String sql,
      long startMicros,
      long endMicros,
      int from,
      int size,
      boolean trackTotalHits,
      String type) {
    Map<String, Object> q = new LinkedHashMap<>();
    q.put("sql", sql);
    q.put("start_time", startMicros);
    q.put("end_time", endMicros);
    q.put("from", from);
    q.put("size", size);
    if (trackTotalHits) {
      q.put("track_total_hits", true);
    }
    return jsonAsync(
        TransportRequest.post(orgPath("/_search"))
            .query("type", type)
            .json(Map.of("query", q))
            .idempotent(true)
            .operation(operation));
  }

  /**
   * Maps an OpenObserve error response: the vendor message (JSON {@code message} or the first line
   * of a text body), the search {@code trace_id} as request id, vendor code 20002 or "stream not
   * found" on HTTP 400 to {@code NOT_FOUND} and "already exists" on HTTP 400 to {@code CONFLICT}.
   *
   * @param operation operation name
   * @param r error response
   * @param secrets secret values to redact
   * @return the exception
   */
  public static OpenObserveException mapError(
      String operation, TransportResponse r, Collection<String> secrets) {
    int status = r.status();
    byte[] body = r.body();
    String text = new String(body, StandardCharsets.UTF_8).trim();
    String vendorCode = "";
    String requestId = r.requestId().orElse(null);
    String vendorText = null;
    if (text.startsWith("{")) {
      try {
        JsonNode node = Json.tree(body);
        vendorCode = node.path("code").asText("");
        if (node.path("message").isTextual()) {
          vendorText = node.get("message").asText();
        }
        if (requestId == null && node.path("trace_id").isTextual()) {
          String id = node.get("trace_id").asText();
          requestId = id.isEmpty() ? null : id;
        }
      } catch (ValidationException ignored) {
        // not JSON after all: use the text
      }
    }
    if (vendorText == null && !text.startsWith("{") && !text.startsWith("[")) {
      int nl = text.indexOf('\n');
      vendorText = nl >= 0 ? text.substring(0, nl) : text;
    }
    Map<String, List<String>> headers = new LinkedHashMap<>();
    headers.put("content-type", List.of("text/plain"));
    byte[] vendorBytes =
        vendorText == null ? new byte[0] : vendorText.getBytes(StandardCharsets.UTF_8);
    String message = HttpTransport.errorMessage(operation, status, headers, vendorBytes, secrets);
    ErrorCode code = ErrorCode.forStatus(status);
    String lower = vendorText == null ? "" : vendorText.toLowerCase(Locale.ROOT);
    if (VENDOR_STREAM_NOT_FOUND.equals(vendorCode)
        || (status == 400 && lower.contains("stream not found"))) {
      code = ErrorCode.NOT_FOUND;
    } else if (status == 400 && lower.contains("already exists")) {
      code = ErrorCode.CONFLICT;
    }
    return OpenObserveException.of(code, message, status, requestId, null, null);
  }
}
