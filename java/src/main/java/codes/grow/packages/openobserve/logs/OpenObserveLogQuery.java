package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.logging.LogQueryClient;
import codes.grow.packages.openobserve.logging.LogRecord;
import codes.grow.packages.openobserve.logging.LogSearchResult;
import codes.grow.packages.openobserve.logging.SearchOptions;
import codes.grow.packages.openobserve.logging.Severity;
import codes.grow.packages.openobserve.logging.SortOrder;
import codes.grow.packages.openobserve.logging.TailHandle;
import codes.grow.packages.openobserve.logging.TailOptions;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * {@link LogQueryClient} for one OpenObserve log stream ({@code OpenObserveClient.logQuery()}).
 *
 * <p>A query that starts with {@code SELECT} (case-insensitive) is raw SQL; any other query is a
 * full-text search, {@code match_all('<query>')}, on the stream; an empty query matches every
 * record. Results are normalized into {@link LogRecord}s: {@code _timestamp}, {@code body} or
 * {@code message}, {@code severity} or {@code level} (+ {@code severity_number}), {@code trace_id},
 * {@code span_id}, {@code trace_flags} and the resource keys {@code service_name}, {@code
 * service_version}, {@code deployment_environment} and {@code host_name}; every other column
 * becomes an attribute.
 */
public final class OpenObserveLogQuery implements LogQueryClient {
  /**
   * How far each tail poll reaches back before the cursor, never before the start (rows are
   * de-duplicated by content).
   */
  public static final Duration TAIL_OVERLAP = RowTailer.OVERLAP;

  /** Page size of one tail poll. */
  static final int TAIL_PAGE_SIZE = RowTailer.PAGE_SIZE;

  private static final Pattern TRACE_ID = Pattern.compile("[0-9a-f]{32}");
  private static final Pattern SPAN_ID = Pattern.compile("[0-9a-f]{16}");
  private static final Map<String, String> RESOURCE_COLUMNS =
      Map.of(
          "service_name", "service.name",
          "service_version", "service.version",
          "deployment_environment", "deployment.environment",
          "host_name", "host.name");

  private final Api api;
  private final Streams streams;
  private final String stream;

  /**
   * Creates the query client (use {@code OpenObserveClient.logQuery(stream)}).
   *
   * @param api request helper
   * @param stream stream name (normalized)
   */
  public OpenObserveLogQuery(Api api, String stream) {
    this.api = api;
    this.streams = new Streams(api);
    this.stream = Values.stream("logs.logQuery", stream);
  }

  /**
   * Returns the (normalized) stream this client queries.
   *
   * @return the stream name
   */
  public String stream() {
    return stream;
  }

  /**
   * Turns a query into SQL.
   *
   * @param query SQL, text or empty
   * @param order sort order for non-SQL queries
   * @return the SQL
   */
  String buildSql(String query, SortOrder order) {
    String q = query == null ? "" : query.trim();
    if (Values.isSql(q)) {
      return q;
    }
    StringBuilder sql = new StringBuilder("SELECT * FROM ").append(Values.sqlIdent(stream));
    if (!q.isEmpty()) {
      sql.append(" WHERE match_all(").append(Values.sqlString(q)).append(')');
    }
    return sql.append(" ORDER BY _timestamp ")
        .append(order == SortOrder.ASC ? "ASC" : "DESC")
        .toString();
  }

  /**
   * Returns one page of matching records (default window: the last 15 minutes, limit 100, newest
   * first). The cursor is the row offset of the next page; {@code nextCursor} is empty on the last
   * page. {@code total} is not reported (use {@code Logs.search} with {@code trackTotalHits}).
   *
   * @param query SQL or text
   * @param options window, limit, order, cursor
   * @return one page
   */
  @Override
  public LogSearchResult search(String query, SearchOptions options) {
    return Api.await(searchAsync(query, options), "logs.search");
  }

  @Override
  public CompletableFuture<LogSearchResult> searchAsync(String query, SearchOptions options) {
    String op = "logs.search";
    return Api.defer(
        () -> {
          SearchOptions o = options == null ? SearchOptions.defaults() : options;
          Instant[] w =
              Values.window(
                  op, o.start().orElse(null), o.end().orElse(null), Values.DEFAULT_SEARCH_WINDOW);
          int from = 0;
          if (o.cursor().isPresent()) {
            try {
              from = Integer.parseInt(o.cursor().get());
            } catch (NumberFormatException e) {
              from = -1;
            }
            if (from < 0) {
              throw new ValidationException(op + ": invalid cursor");
            }
          }
          int start = from;
          int limit = o.limit();
          return api.searchAsync(
                  op,
                  buildSql(query, o.order()),
                  Values.micros(w[0]),
                  Values.micros(w[1]),
                  from,
                  limit,
                  false,
                  StreamType.LOGS.wireValue())
              .thenApply(
                  body -> {
                    List<LogRecord> records = new ArrayList<>();
                    for (JsonNode h : body.path("hits")) {
                      records.add(recordFromRow(Values.row(h)));
                    }
                    Optional<String> next =
                        records.size() >= limit
                            ? Optional.of(Integer.toString(start + records.size()))
                            : Optional.empty();
                    return new LogSearchResult(records, next, OptionalLong.empty());
                  });
        });
  }

  /**
   * Lists the names of the log streams.
   *
   * @return names
   */
  @Override
  public List<String> listStreams() {
    return Api.await(listStreamsAsync(), "logs.listStreams");
  }

  @Override
  public CompletableFuture<List<String>> listStreamsAsync() {
    return streams
        .listStreamsAsync(ListStreamsOptions.of(StreamType.LOGS))
        .thenApply(l -> l.streams().stream().map(StreamInfo::name).toList());
  }

  /**
   * Lists the field names of the stream's schema.
   *
   * @return names
   */
  @Override
  public List<String> listLabels() {
    return Api.await(listLabelsAsync(), "logs.listLabels");
  }

  @Override
  public CompletableFuture<List<String>> listLabelsAsync() {
    return streams
        .getStreamSchemaAsync(stream, StreamType.LOGS)
        .thenApply(i -> i.schema().stream().map(SchemaField::name).toList());
  }

  /**
   * Polls the stream for new matching records and calls {@code onRecord} for each one, oldest
   * first, until the handle is stopped. OpenObserve has no live tail and no row id: each poll
   * re-reads the last {@link #TAIL_OVERLAP} and drops rows already seen (exact duplicates within
   * the same microsecond are therefore delivered once). Search errors go to {@code
   * options.onError()} and polling continues.
   *
   * @param query SQL or text
   * @param onRecord callback per record
   * @param options start (default now), interval (default 1 s), error callback
   * @return the handle
   */
  @Override
  public TailHandle tail(String query, Consumer<LogRecord> onRecord, TailOptions options) {
    if (onRecord == null) {
      throw new ValidationException("logs.tail: onRecord is required");
    }
    TailOptions o = options == null ? TailOptions.defaults() : options;
    Duration interval =
        o.interval().isNegative() || o.interval().isZero() ? Duration.ofSeconds(1) : o.interval();
    String sql = buildSql(query, SortOrder.ASC);
    return new RowTailer(
            api,
            from -> sql,
            StreamType.LOGS,
            TAIL_PAGE_SIZE,
            TAIL_OVERLAP,
            o.start().orElseGet(Instant::now),
            row -> onRecord.accept(recordFromRow(row)),
            o.onError())
        .start(interval);
  }

  /** Content key of a row (JSON with sorted keys), so equal rows give equal keys. */
  static String hash(Map<String, Object> row) {
    return RowTailer.hash(row);
  }

  /**
   * Normalizes a stored log row into a {@link LogRecord}.
   *
   * @param row stored row
   * @return the record
   */
  public static LogRecord recordFromRow(Map<String, Object> row) {
    Map<String, Object> rest = new LinkedHashMap<>(row);
    rest.remove("_o2_ingest_ts");
    LogRecord.Builder b = LogRecord.builder();
    Object ts = rest.remove("_timestamp");
    if (ts instanceof Long) {
      b.timestamp(Values.fromMicros((Long) ts));
    }
    String body = take(rest, "body", "message");
    b.body(body == null ? "" : body);
    String sevText = take(rest, "severity", "level");
    Object sevNum = rest.get("severity_number");
    Severity sev = null;
    if (sevNum instanceof Long && (Long) sevNum >= 1 && (Long) sevNum <= 24) {
      rest.remove("severity_number");
      sev = Severity.fromNumber(((Long) sevNum).intValue());
    } else if (sevText != null) {
      sev = Severity.parse(sevText).orElse(null);
    }
    if (sev != null) {
      b.severity(sev);
    }
    if (sevText != null) {
      b.severityText(sevText);
    }
    Object trace = rest.get("trace_id");
    if (trace instanceof String && TRACE_ID.matcher(lower(trace)).matches()) {
      b.traceId(lower(rest.remove("trace_id")));
    }
    Object span = rest.get("span_id");
    if (span instanceof String && SPAN_ID.matcher(lower(span)).matches()) {
      b.spanId(lower(rest.remove("span_id")));
    }
    Object flags = rest.get("trace_flags");
    if (flags instanceof Long && (Long) flags >= 0 && (Long) flags <= 255) {
      rest.remove("trace_flags");
      b.traceFlags(((Long) flags).intValue());
    }
    Map<String, Object> resource = new LinkedHashMap<>();
    RESOURCE_COLUMNS.forEach(
        (col, key) -> {
          if (rest.containsKey(col)) {
            resource.put(key, rest.remove(col));
          }
        });
    return b.resource(new TreeMap<>(resource)).attributes(rest).build();
  }

  private static String take(Map<String, Object> row, String first, String second) {
    if (row.containsKey(first)) {
      return Values.text(Json.mapper().valueToTree(row.remove(first)));
    }
    if (row.containsKey(second)) {
      return Values.text(Json.mapper().valueToTree(row.remove(second)));
    }
    return null;
  }

  private static String lower(Object o) {
    return String.valueOf(o).toLowerCase(Locale.ROOT);
  }
}
