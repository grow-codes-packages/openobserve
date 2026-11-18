package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.SdkInfo;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.logging.ExportOptions;
import codes.grow.packages.openobserve.logging.ExportResult;
import codes.grow.packages.openobserve.logging.LogExporter;
import codes.grow.packages.openobserve.logging.LogRecord;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LogExporter} that sends OTLP/HTTP JSON ({@code ExportLogsServiceRequest}) to {@code
 * /api/{org}/v1/logs} with a {@code stream-name} header. Stored rows use the OTLP column names
 * ({@code body}, {@code severity}, {@code severity_number}, {@code trace_id}, {@code span_id},
 * flattened attributes and resource); OpenObserve does not store trace flags.
 */
public final class OtlpLogExporter implements LogExporter {
  private final Api api;
  private final String stream;

  /**
   * Creates the exporter (use {@code OpenObserveClient.logExporter(stream, true)}).
   *
   * @param api request helper
   * @param stream target stream
   */
  public OtlpLogExporter(Api api, String stream) {
    this.api = api;
    this.stream = Values.stream("logs.exportOtlp", stream);
  }

  /**
   * Returns the (normalized) target stream.
   *
   * @return the stream
   */
  public String stream() {
    return stream;
  }

  /**
   * Sends one batch as OTLP JSON. A {@code partialSuccess} answer is reported as rejected records
   * with a {@code ValidationException}.
   *
   * @param records records
   * @param options gzip flag and time budget
   * @return future result
   */
  @Override
  public CompletableFuture<ExportResult> export(List<LogRecord> records, ExportOptions options) {
    String op = "logs.exportOtlp";
    return Api.defer(
        () -> {
          TransportRequest.Builder req =
              JsonLogExporter.body(
                      TransportRequest.post(api.orgPath("/v1/logs")), request(records), options)
                  .header("stream-name", stream)
                  .operation(op);
          return api.sendAsync(req)
              .thenApply(
                  r -> {
                    int n = records.size();
                    JsonNode body;
                    try {
                      body = Json.tree(r.body());
                    } catch (ValidationException e) {
                      return ExportResult.success(n);
                    }
                    long rejected =
                        Values.asLong(body.path("partialSuccess").get("rejectedLogRecords"), 0);
                    if (rejected <= 0) {
                      return ExportResult.success(n);
                    }
                    int rej = (int) Math.min(rejected, n);
                    String msg = Values.text(body.path("partialSuccess").get("errorMessage"));
                    return ExportResult.failure(
                        n - rej,
                        rej,
                        new ValidationException(
                            op
                                + ": "
                                + rej
                                + " of "
                                + n
                                + " records rejected: "
                                + api.redact(msg)));
                  });
        });
  }

  /**
   * Groups records by resource into an OTLP/JSON request.
   *
   * @param records records
   * @return the request object
   */
  static Map<String, Object> request(List<LogRecord> records) {
    Map<String, List<Object>> groups = new LinkedHashMap<>();
    Map<String, Map<String, Object>> resources = new LinkedHashMap<>();
    for (LogRecord r : records) {
      String key = new String(Json.encode(new TreeMap<>(r.resource())));
      resources.putIfAbsent(key, r.resource());
      Map<String, Object> lr = new LinkedHashMap<>();
      lr.put("timeUnixNano", Long.toString(nanos(r.timestamp())));
      r.observedTimestamp().ifPresent(t -> lr.put("observedTimeUnixNano", Long.toString(nanos(t))));
      lr.put("severityNumber", r.severity().number());
      lr.put("severityText", r.severityText());
      lr.put("body", Map.of("stringValue", r.body()));
      lr.put("attributes", keyValues(r.attributes()));
      r.traceId().ifPresent(v -> lr.put("traceId", v));
      r.spanId().ifPresent(v -> lr.put("spanId", v));
      r.traceFlags().ifPresent(v -> lr.put("flags", v));
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(lr);
    }
    List<Object> resourceLogs = new ArrayList<>();
    groups.forEach(
        (key, logs) ->
            resourceLogs.add(
                Map.of(
                    "resource",
                    Map.of("attributes", keyValues(resources.get(key))),
                    "scopeLogs",
                    List.of(
                        Map.of(
                            "scope",
                            Map.of("name", "grow-codes-openobserve", "version", SdkInfo.VERSION),
                            "logRecords",
                            logs)))));
    return Map.of("resourceLogs", resourceLogs);
  }

  private static long nanos(Instant t) {
    return Values.micros(t) * 1000L + t.getNano() % 1000;
  }

  /** OTLP KeyValue list, sorted by key. */
  static List<Object> keyValues(Map<String, Object> attrs) {
    List<Object> out = new ArrayList<>();
    new TreeMap<>(attrs)
        .forEach(
            (k, v) -> {
              Map<String, Object> kv = new LinkedHashMap<>();
              kv.put("key", k);
              kv.put("value", anyValue(v));
              out.add(kv);
            });
    return out;
  }

  /** OTLP AnyValue (JSON form) of a normalized attribute value. */
  static Map<String, Object> anyValue(Object v) {
    if (v instanceof Boolean) {
      return Map.of("boolValue", v);
    }
    if (v instanceof Long) {
      return Map.of("intValue", v.toString());
    }
    if (v instanceof Double) {
      double d = (Double) v;
      return Double.isFinite(d) ? Map.of("doubleValue", d) : Map.of("stringValue", v.toString());
    }
    if (v instanceof List) {
      List<Object> values = new ArrayList<>();
      for (Object e : (List<?>) v) {
        values.add(anyValue(e));
      }
      return Map.of("arrayValue", Map.of("values", values));
    }
    if (v instanceof Map) {
      Map<String, Object> m = new LinkedHashMap<>();
      ((Map<?, ?>) v).forEach((k, e) -> m.put(String.valueOf(k), e));
      return Map.of("kvlistValue", Map.of("values", keyValues(m)));
    }
    return Map.of("stringValue", String.valueOf(v));
  }

  /**
   * Nothing to release.
   *
   * @return a completed future
   */
  @Override
  public CompletableFuture<Void> shutdown() {
    return CompletableFuture.completedFuture(null);
  }
}
