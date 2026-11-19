package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.logging.ExportOptions;
import codes.grow.packages.openobserve.logging.ExportResult;
import codes.grow.packages.openobserve.logging.LogExporter;
import codes.grow.packages.openobserve.logging.LogRecord;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * {@link LogExporter} that writes records to an OpenObserve stream through the {@code _json} ingest
 * API (the default exporter of {@code OpenObserveClient.logClient()}). Each record becomes one row:
 * {@code _timestamp} (µs), {@code body}, {@code severity} (text), {@code severity_number}, {@code
 * trace_id}, {@code span_id}, {@code trace_flags}, the record attributes and the resource keys
 * flattened with {@code _} ({@code service.name} becomes {@code service_name}).
 */
public final class JsonLogExporter implements LogExporter {
  private final Api api;
  private final String stream;

  /**
   * Creates the exporter (use {@code OpenObserveClient.logExporter(stream, false)}).
   *
   * @param api request helper
   * @param stream target stream
   */
  public JsonLogExporter(Api api, String stream) {
    this.api = api;
    this.stream = Values.stream("logs.export", stream);
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
   * Converts a record to an ingest row.
   *
   * @param r record
   * @return the row
   */
  static Map<String, Object> row(LogRecord r) {
    Map<String, Object> row = new LinkedHashMap<>(r.attributes());
    r.resource().forEach((k, v) -> row.put(Values.normalizeKey(k), v));
    row.put("_timestamp", Values.micros(r.timestamp()));
    row.put("body", r.body());
    row.put("severity", r.severityText());
    row.put("severity_number", r.severity().number());
    r.traceId().ifPresent(v -> row.put("trace_id", v));
    r.spanId().ifPresent(v -> row.put("span_id", v));
    r.traceFlags().ifPresent(v -> row.put("trace_flags", v));
    return row;
  }

  /**
   * Sends one batch. Records the server rejects (for example outside the ingest window) are
   * reported in {@link ExportResult#rejected()} with a {@code ValidationException}.
   *
   * @param records records
   * @param options gzip flag and time budget
   * @return future result
   */
  @Override
  public CompletableFuture<ExportResult> export(List<LogRecord> records, ExportOptions options) {
    String op = "logs.export";
    return Api.defer(
        () -> {
          List<Map<String, Object>> rows = new ArrayList<>(records.size());
          for (LogRecord r : records) {
            rows.add(row(r));
          }
          TransportRequest.Builder req =
              body(TransportRequest.post(api.orgPath("/" + stream + "/_json")), rows, options)
                  .operation(op);
          return api.sendAsync(req)
              .thenApply(
                  r -> {
                    IngestResult ir = Logs.ingestResult(op, r);
                    if (ir.failed() == 0) {
                      return ExportResult.success(ir.successful());
                    }
                    String reason = "records rejected";
                    for (IngestStreamStatus s : ir.streams()) {
                      reason = s.error().orElse(reason);
                    }
                    return ExportResult.failure(
                        ir.successful(),
                        ir.failed(),
                        new ValidationException(
                            op
                                + ": "
                                + ir.failed()
                                + " of "
                                + records.size()
                                + " records rejected: "
                                + api.redact(reason)));
                  });
        });
  }

  /**
   * Sets a JSON body, gzip-compressed when the options ask for it, and the time budget.
   *
   * @param req request builder
   * @param value JSON value
   * @param options export options
   * @return the builder
   */
  static TransportRequest.Builder body(
      TransportRequest.Builder req, Object value, ExportOptions options) {
    byte[] json = Json.encode(value);
    if (options.gzip()) {
      req.body(Values.gzip(json), "application/json").header("Content-Encoding", "gzip");
    } else {
      req.body(json, "application/json");
    }
    return req.timeout(options.timeout());
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
