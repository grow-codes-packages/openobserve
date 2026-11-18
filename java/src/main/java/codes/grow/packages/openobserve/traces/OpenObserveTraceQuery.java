package codes.grow.packages.openobserve.traces;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.UnsupportedException;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.tracing.query.DependencyLink;
import codes.grow.packages.openobserve.tracing.query.Span;
import codes.grow.packages.openobserve.tracing.query.Trace;
import codes.grow.packages.openobserve.tracing.query.TraceIds;
import codes.grow.packages.openobserve.tracing.query.TraceQueryClient;
import codes.grow.packages.openobserve.tracing.query.TraceSearchQuery;
import codes.grow.packages.openobserve.tracing.query.TraceSummary;
import codes.grow.packages.openobserve.tracing.query.Traces;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * {@link TraceQueryClient} for one OpenObserve traces stream ({@code
 * OpenObserveClient.traceQuery()}). Every lookup has a blocking form and an {@code …Async} form.
 */
public final class OpenObserveTraceQuery implements TraceQueryClient {
  /** Window searched by {@link #searchTraces} without a start (1 hour). */
  public static final Duration DEFAULT_TRACE_SEARCH_WINDOW = Duration.ofHours(1);

  /** Window searched by {@link #getTrace} (7 days). */
  public static final Duration DEFAULT_GET_TRACE_WINDOW = Duration.ofDays(7);

  /** Window searched by {@link #listServices} and {@link #listOperations} (24 hours). */
  public static final Duration DEFAULT_SERVICES_WINDOW = Duration.ofHours(24);

  /** Stored columns that never become span attributes. */
  private static final Set<String> INTERNAL_COLUMNS =
      Set.of(
          "_timestamp",
          "_o2_ingest_ts",
          "flags",
          "events",
          "links",
          "trace_id",
          "span_id",
          "reference_parent_span_id",
          "reference_parent_trace_id",
          "reference_ref_type",
          "operation_name",
          "span_kind",
          "service_name",
          "start_time",
          "end_time",
          "duration",
          "span_status",
          "status_code",
          "status_message");

  private static final Span.Kind[] KINDS = {
    Span.Kind.UNSPECIFIED,
    Span.Kind.INTERNAL,
    Span.Kind.SERVER,
    Span.Kind.CLIENT,
    Span.Kind.PRODUCER,
    Span.Kind.CONSUMER
  };

  private final Api api;
  private final String stream;

  /**
   * Creates the query client (use {@code OpenObserveClient.traceQuery(stream)}).
   *
   * @param api request helper
   * @param stream traces stream
   */
  public OpenObserveTraceQuery(Api api, String stream) {
    this.api = api;
    this.stream = Values.stream("traces.traceQuery", stream);
  }

  /**
   * Returns the (normalized) traces stream.
   *
   * @return the stream
   */
  public String stream() {
    return stream;
  }

  /**
   * Returns the trace with all its spans ({@code GET /api/{org}/{stream}/traces/{id}/details} over
   * the last 7 days, following pages) or empty when it does not exist.
   *
   * @param traceId 32-hex trace id (shorter ids are left-padded)
   * @return the trace, or empty
   */
  @Override
  public Optional<Trace> getTrace(String traceId) {
    return Api.await(getTraceAsync(traceId), "traces.getTrace");
  }

  @Override
  public CompletableFuture<Optional<Trace>> getTraceAsync(String traceId) {
    String op = "traces.getTrace";
    return Api.defer(
        () -> {
          String id = TraceIds.normalizeTraceId(traceId);
          Instant end = Instant.now().plusSeconds(60);
          Instant start = end.minus(DEFAULT_GET_TRACE_WINDOW);
          List<Span> spans = new ArrayList<>();
          return page(op, id, Values.micros(start), Values.micros(end), null, null, spans, 0)
              .thenApply(
                  v ->
                      spans.isEmpty()
                          ? Optional.<Trace>empty()
                          : Optional.of(new Trace(id, Traces.sortSpans(spans))));
        });
  }

  private CompletableFuture<Void> page(
      String op,
      String id,
      long start,
      long end,
      String afterStart,
      String afterSpan,
      List<Span> spans,
      int pageNo) {
    TransportRequest.Builder req =
        TransportRequest.get(api.orgPath("/" + stream + "/traces/" + id + "/details"))
            .query("start_time", start)
            .query("end_time", end)
            .query("after_start_time", afterStart)
            .query("after_span_id", afterSpan)
            .operation(op);
    return api.jsonAsync(req)
        .thenCompose(
            body -> {
              for (JsonNode row : body.path("hits")) {
                spans.add(spanFromRow(Values.row(row)));
              }
              JsonNode next = body.path("next_after");
              String nextStart = Values.text(next.get("start_time"));
              if (!body.path("has_more").asBoolean(false) || nextStart.isEmpty() || pageNo >= 999) {
                return CompletableFuture.completedFuture(null);
              }
              return page(
                  op,
                  id,
                  start,
                  end,
                  nextStart,
                  Values.text(next.get("span_id")),
                  spans,
                  pageNo + 1);
            });
  }

  /**
   * Maps a stored span row to a {@link Span}: {@code start_time}/{@code end_time} are nanoseconds,
   * {@code span_kind} a string number, {@code events} and {@code links} JSON text; other columns
   * become attributes (OpenObserve stores non-string values as text).
   *
   * @param row stored row
   * @return the span
   */
  public static Span spanFromRow(Map<String, Object> row) {
    String parent = text(row.get("reference_parent_span_id"));
    Span.Builder b =
        Span.builder(
                text(row.get("trace_id")),
                text(row.get("span_id")),
                text(row.get("operation_name")))
            .serviceName(text(row.get("service_name")))
            .times(nsToMicros(row.get("start_time")), nsToMicros(row.get("end_time")));
    if (!parent.isEmpty()) {
      b.parentSpanId(parent);
    }
    String kind = text(row.get("span_kind"));
    Span.Kind k = Span.Kind.parse(kind);
    if (kind.matches("[0-5]")) {
      k = KINDS[Integer.parseInt(kind)];
    }
    b.kind(k);
    String message = text(row.get("status_message"));
    String status = text(row.get("span_status")).toUpperCase(Locale.ROOT);
    Span.StatusCode code =
        "OK".equals(status)
            ? Span.StatusCode.OK
            : "ERROR".equals(status) ? Span.StatusCode.ERROR : Span.StatusCode.UNSET;
    b.status(code, message.isEmpty() ? null : message);
    Map<String, Object> attrs = new TreeMap<>();
    row.forEach(
        (key, v) -> {
          if (!INTERNAL_COLUMNS.contains(key)
              && !key.startsWith("infer_service_")
              && !key.startsWith("o2_db_")) {
            attrs.put(key, v);
          }
        });
    b.attributes(attrs);
    for (JsonNode e : jsonArray(row.get("events"))) {
      Map<String, Object> a = new LinkedHashMap<>(Values.row(e));
      a.remove("name");
      a.remove("_timestamp");
      b.event(
          new Span.Event(
              Values.text(e.get("name")), nsToMicros(Values.asLong(e.get("_timestamp"), 0)), a));
    }
    for (JsonNode l : jsonArray(row.get("links"))) {
      Map<String, Object> a = new LinkedHashMap<>(Values.row(l));
      a.remove("context");
      a.remove("droppedAttributesCount");
      JsonNode ctx = l.path("context");
      try {
        b.link(new Span.Link(Values.text(ctx.get("traceId")), Values.text(ctx.get("spanId")), a));
      } catch (ValidationException ignored) {
        // a link without valid ids is dropped
      }
    }
    return b.build();
  }

  private static Iterable<JsonNode> jsonArray(Object column) {
    if (column instanceof String && !((String) column).isEmpty()) {
      try {
        JsonNode n = Json.mapper().readTree((String) column);
        if (n.isArray()) {
          return n;
        }
      } catch (java.io.IOException ignored) {
        // not JSON: no events or links
      }
    }
    return List.of();
  }

  private static String text(Object v) {
    return v == null ? "" : String.valueOf(v);
  }

  private static long nsToMicros(Object v) {
    if (v instanceof Long) {
      return (Long) v / 1000;
    }
    if (v instanceof String) {
      try {
        return Long.parseLong((String) v) / 1000;
      } catch (NumberFormatException e) {
        return 0;
      }
    }
    return 0;
  }

  /**
   * Builds the {@code traces/latest} filter (an SQL {@code WHERE} fragment).
   *
   * @param q query
   * @return the filter, empty when the query has no condition
   */
  static String filter(TraceSearchQuery q) {
    List<String> parts = new ArrayList<>();
    q.service().ifPresent(s -> parts.add("service_name = " + Values.sqlString(s)));
    q.operation().ifPresent(s -> parts.add("operation_name = " + Values.sqlString(s)));
    q.minDurationMicros().ifPresent(d -> parts.add("duration >= " + d));
    q.maxDurationMicros().ifPresent(d -> parts.add("duration <= " + d));
    new TreeMap<>(q.tags())
        .forEach(
            (k, v) -> {
              String col = Values.normalizeKey(k);
              if (!col.isEmpty()) {
                parts.add(Values.sqlIdent(col) + " = " + Values.sqlString(v));
              }
            });
    return String.join(" AND ", parts);
  }

  /**
   * Finds traces ({@code GET /api/{org}/{stream}/traces/latest}), newest first, over the last hour
   * by default. Tag keys are flattened like the stored columns ({@code http.method} becomes {@code
   * http_method}). With filters, each summary covers only the matching spans (start, duration and
   * span count), as OpenObserve reports.
   *
   * @param query filters, window and limit
   * @return summaries
   */
  @Override
  public List<TraceSummary> searchTraces(TraceSearchQuery query) {
    return Api.await(searchTracesAsync(query), "traces.searchTraces");
  }

  @Override
  public CompletableFuture<List<TraceSummary>> searchTracesAsync(TraceSearchQuery query) {
    String op = "traces.searchTraces";
    return Api.defer(
        () -> {
          TraceSearchQuery q = query == null ? TraceSearchQuery.builder().build() : query;
          Instant[] w =
              Values.window(
                  op, q.start().orElse(null), q.end().orElse(null), DEFAULT_TRACE_SEARCH_WINDOW);
          String f = filter(q);
          TransportRequest.Builder req =
              TransportRequest.get(api.orgPath("/" + stream + "/traces/latest"))
                  .query("start_time", Values.micros(w[0]))
                  .query("end_time", Values.micros(w[1]))
                  .query("from", 0)
                  .query("size", q.limit())
                  .query("filter", f.isEmpty() ? null : f)
                  .operation(op);
          return api.jsonAsync(req).thenApply(OpenObserveTraceQuery::summaries);
        });
  }

  private static List<TraceSummary> summaries(JsonNode body) {
    List<TraceSummary> out = new ArrayList<>();
    for (JsonNode h : body.path("hits")) {
      JsonNode first = h.path("first_event");
      String svc = Values.text(first.get("service_name"));
      String name = Values.text(first.get("operation_name"));
      out.add(
          new TraceSummary(
              Values.text(h.get("trace_id")).toLowerCase(Locale.ROOT),
              svc.isEmpty() ? null : svc,
              name.isEmpty() ? null : name,
              Values.asLong(h.get("start_time"), 0) / 1000,
              Values.asLong(h.get("duration"), 0),
              (int) Values.asLong(h.path("spans").get(0), 0)));
    }
    return out;
  }

  private CompletableFuture<List<String>> distinct(String op, String column, String where) {
    String sql =
        "SELECT DISTINCT "
            + column
            + " FROM "
            + Values.sqlIdent(stream)
            + (where.isEmpty() ? "" : " WHERE " + where)
            + " ORDER BY "
            + column;
    Instant end = Instant.now().plusSeconds(60);
    return api.searchAsync(
            op,
            sql,
            Values.micros(end.minus(DEFAULT_SERVICES_WINDOW)),
            Values.micros(end),
            0,
            10_000,
            false,
            "traces")
        .thenApply(
            body -> {
              List<String> out = new ArrayList<>();
              for (JsonNode h : body.path("hits")) {
                String v = Values.text(h.get(column));
                if (!v.isEmpty()) {
                  out.add(v);
                }
              }
              return out;
            });
  }

  /**
   * Lists the service names that reported spans in the last 24 hours.
   *
   * @return names, sorted
   */
  @Override
  public List<String> listServices() {
    return Api.await(listServicesAsync(), "traces.listServices");
  }

  @Override
  public CompletableFuture<List<String>> listServicesAsync() {
    return distinct("traces.listServices", "service_name", "");
  }

  /**
   * Lists the span names of a service in the last 24 hours.
   *
   * @param service service name
   * @return names, sorted
   */
  @Override
  public List<String> listOperations(String service) {
    return Api.await(listOperationsAsync(service), "traces.listOperations");
  }

  @Override
  public CompletableFuture<List<String>> listOperationsAsync(String service) {
    String op = "traces.listOperations";
    if (service == null || service.isEmpty()) {
      return CompletableFuture.failedFuture(new ValidationException(op + ": service is required"));
    }
    return distinct(op, "operation_name", "service_name = " + Values.sqlString(service));
  }

  /**
   * Always throws: the service graph is an OpenObserve enterprise feature that the open-source
   * build does not serve.
   *
   * @param start ignored
   * @param end ignored
   * @return never
   * @throws UnsupportedException always
   */
  @Override
  public List<DependencyLink> getDependencies(Instant start, Instant end) {
    throw new UnsupportedException(
        "traces.getDependencies: the service graph is an OpenObserve enterprise feature");
  }

  @Override
  public CompletableFuture<List<DependencyLink>> getDependenciesAsync(Instant start, Instant end) {
    return Api.defer(() -> CompletableFuture.completedFuture(getDependencies(start, end)));
  }

  /**
   * Looks a trace up in the server's trace index ({@code GET /api/{org}/traces/time_range}).
   *
   * @param traceId trace id
   * @return where and when the trace was seen ({@code found = false} when unknown)
   */
  public TraceTimeRange traceTimeRange(String traceId) {
    return Api.await(traceTimeRangeAsync(traceId), "traces.traceTimeRange");
  }

  /**
   * Asynchronous {@link #traceTimeRange}.
   *
   * @param traceId trace id
   * @return future range
   */
  public CompletableFuture<TraceTimeRange> traceTimeRangeAsync(String traceId) {
    String op = "traces.traceTimeRange";
    return Api.defer(
        () -> {
          String id = TraceIds.normalizeTraceId(traceId);
          return api.jsonAsync(
                  TransportRequest.get(api.orgPath("/traces/time_range"))
                      .query("trace_id", id)
                      .operation(op))
              .thenApply(
                  body -> {
                    for (JsonNode r : body.path("results")) {
                      if ("found".equals(Values.text(r.get("status")))) {
                        JsonNode range = r.path("range");
                        return new TraceTimeRange(
                            id,
                            true,
                            Optional.of(Values.text(r.get("stream"))),
                            Optional.of(
                                Values.fromMicros(Values.asLong(range.get("start_time"), 0))),
                            Optional.of(
                                Values.fromMicros(Values.asLong(range.get("end_time"), 0))));
                      }
                    }
                    return new TraceTimeRange(id, false, null, null, null);
                  });
        });
  }
}
