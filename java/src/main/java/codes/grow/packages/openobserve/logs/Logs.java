package codes.grow.packages.openobserve.logs;

import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.TransportResponse;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.logging.LogRecordJson;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Log ingestion and SQL search of OpenObserve ({@code OpenObserveClient.logs()}).
 *
 * <p>Every method has a blocking form and an {@code …Async} form returning {@link
 * CompletableFuture}. Stream names are normalized like the server does (lower case, every character
 * other than {@code [a-z0-9_]} becomes {@code _}). Times are {@link Instant}s; the SDK sends Unix
 * microseconds.
 */
public final class Logs {
  private final Api api;

  /**
   * Creates the module (use {@code OpenObserveClient.logs()}).
   *
   * @param api request helper
   */
  public Logs(Api api) {
    this.api = api;
  }

  // ---------------------------------------------------------------------------------------------
  // ingest

  /**
   * Stores records in a stream ({@code POST /api/{org}/{stream}/_json}). A {@code _timestamp} field
   * may hold Unix microseconds (milliseconds, nanoseconds and RFC 3339 strings are detected by the
   * server); without it the server time is used. Nested objects are flattened with {@code _}.
   *
   * @param stream stream name
   * @param records records (at least one)
   * @return per-stream counters; rejected records do not fail the call
   */
  public IngestResult ingestJson(String stream, List<? extends Map<String, ?>> records) {
    return Api.await(ingestJsonAsync(stream, records), "logs.ingestJson");
  }

  /**
   * Asynchronous {@link #ingestJson}.
   *
   * @param stream stream name
   * @param records records
   * @return future counters
   */
  public CompletableFuture<IngestResult> ingestJsonAsync(
      String stream, List<? extends Map<String, ?>> records) {
    String op = "logs.ingestJson";
    return Api.defer(
        () -> {
          String s = Values.stream(op, stream);
          if (records == null || records.isEmpty()) {
            throw new ValidationException(op + ": at least one record is required");
          }
          return api.sendAsync(
                  TransportRequest.post(api.orgPath("/" + s + "/_json"))
                      .json(records)
                      .operation(op))
              .thenApply(r -> ingestResult(op, r));
        });
  }

  /**
   * Stores newline-delimited JSON objects in a stream ({@code POST /api/{org}/{stream}/_multi}).
   * One invalid line rejects the whole request with a {@code ValidationException}.
   *
   * @param stream stream name
   * @param ndjson one JSON object per line
   * @return per-stream counters
   */
  public IngestResult ingestMulti(String stream, String ndjson) {
    return Api.await(ingestMultiAsync(stream, ndjson), "logs.ingestMulti");
  }

  /**
   * Asynchronous {@link #ingestMulti}.
   *
   * @param stream stream name
   * @param ndjson one JSON object per line
   * @return future counters
   */
  public CompletableFuture<IngestResult> ingestMultiAsync(String stream, String ndjson) {
    String op = "logs.ingestMulti";
    return Api.defer(
        () -> {
          String s = Values.stream(op, stream);
          if (ndjson == null || ndjson.isBlank()) {
            throw new ValidationException(op + ": ndjson body is empty");
          }
          return api.sendAsync(
                  TransportRequest.post(api.orgPath("/" + s + "/_multi"))
                      .body(ndjson.getBytes(StandardCharsets.UTF_8), "application/x-ndjson")
                      .operation(op))
              .thenApply(r -> ingestResult(op, r));
        });
  }

  static IngestResult ingestResult(String op, TransportResponse r) {
    JsonNode body = Api.json(op, r);
    int ok = 0;
    int failed = 0;
    List<IngestStreamStatus> streams = new ArrayList<>();
    for (JsonNode s : body.path("status")) {
      int sOk = s.path("successful").asInt(0);
      int sFailed = s.path("failed").asInt(0);
      String err = Values.text(s.get("error"));
      streams.add(
          new IngestStreamStatus(
              Values.text(s.get("name")),
              sOk,
              sFailed,
              err.isEmpty() ? Optional.empty() : Optional.of(err)));
      ok += sOk;
      failed += sFailed;
    }
    return new IngestResult(ok, failed, streams);
  }

  /**
   * Sends Elasticsearch-compatible bulk actions ({@code POST /api/{org}/_bulk}). Only {@code index}
   * and {@code create} actions are accepted.
   *
   * @param actions actions (at least one)
   * @return the per-item outcome; rejected items do not fail the call
   * @throws ValidationException for an empty list, another action, a blank stream or a null
   *     document
   */
  public BulkResult ingestBulk(List<BulkAction> actions) {
    return Api.await(ingestBulkAsync(actions), "logs.ingestBulk");
  }

  /**
   * Asynchronous {@link #ingestBulk}.
   *
   * @param actions actions
   * @return future outcome
   */
  public CompletableFuture<BulkResult> ingestBulkAsync(List<BulkAction> actions) {
    String op = "logs.ingestBulk";
    return Api.defer(
        () -> {
          if (actions == null || actions.isEmpty()) {
            throw new ValidationException(op + ": at least one action is required");
          }
          ByteArrayOutputStream buf = new ByteArrayOutputStream();
          for (int i = 0; i < actions.size(); i++) {
            BulkAction a = actions.get(i);
            if (!BulkAction.INDEX.equals(a.action()) && !BulkAction.CREATE.equals(a.action())) {
              throw new ValidationException(
                  op
                      + ": action "
                      + i
                      + ": only \"index\" and \"create\" are supported (OpenObserve ignores \""
                      + a.action()
                      + "\")");
            }
            String index = Values.stream(op, a.index());
            if (a.document() == null) {
              throw new ValidationException(op + ": action " + i + ": document is required");
            }
            buf.writeBytes(Json.encode(Map.of(a.action(), Map.of("_index", index))));
            buf.write('\n');
            buf.writeBytes(Json.encode(a.document()));
            buf.write('\n');
          }
          return api.sendAsync(
                  TransportRequest.post(api.orgPath("/_bulk"))
                      .body(buf.toByteArray(), "application/x-ndjson")
                      .operation(op))
              .thenApply(r -> bulkResult(op, r));
        });
  }

  private static BulkResult bulkResult(String op, TransportResponse r) {
    JsonNode body = Api.json(op, r);
    List<BulkItem> items = new ArrayList<>();
    for (JsonNode item : body.path("items")) {
      for (Map.Entry<String, JsonNode> e : item.properties()) {
        JsonNode f = e.getValue();
        JsonNode error = f.path("error");
        String type = Values.text(error.get("type"));
        String reason = Values.text(error.get("reason"));
        String text = type.isEmpty() ? reason : reason.isEmpty() ? type : type + ": " + reason;
        items.add(
            new BulkItem(
                e.getKey(),
                Values.text(f.get("_index")),
                f.path("status").asInt(0),
                text.isEmpty() ? Optional.empty() : Optional.of(text)));
      }
    }
    return new BulkResult(body.path("took").asLong(0), body.path("errors").asBoolean(false), items);
  }

  // ---------------------------------------------------------------------------------------------
  // search

  /**
   * Runs an OpenObserve SQL query over the last 15 minutes ({@code POST /api/{org}/_search}).
   *
   * @param sql SQL, for example {@code SELECT * FROM "default" WHERE match_all('x')}
   * @return one page of rows
   */
  public SqlSearchResult search(String sql) {
    return search(sql, SqlSearchOptions.defaults());
  }

  /**
   * Runs an OpenObserve SQL query ({@code POST /api/{org}/_search}). Find text with {@code
   * match_all('x')}, {@code str_match(field, 'x')} or {@code re_match(field, 'regex')}; quote
   * reserved stream names ({@code "default"}). A {@code LIMIT} in the SQL overrides {@code size}.
   *
   * @param sql SQL
   * @param options window, paging, count and stream type
   * @return one page of rows
   */
  public SqlSearchResult search(String sql, SqlSearchOptions options) {
    return Api.await(searchAsync(sql, options), "logs.search");
  }

  /**
   * Asynchronous {@link #search(String, SqlSearchOptions)}.
   *
   * @param sql SQL
   * @param options options
   * @return future page
   */
  public CompletableFuture<SqlSearchResult> searchAsync(String sql, SqlSearchOptions options) {
    String op = "logs.search";
    return Api.defer(
        () -> {
          if (sql == null || sql.isBlank()) {
            throw new ValidationException(op + ": sql is required");
          }
          SqlSearchOptions o = options == null ? SqlSearchOptions.defaults() : options;
          if (o.from() < 0 || o.size() < 0) {
            throw new ValidationException(op + ": from and size must not be negative");
          }
          Instant[] w =
              Values.window(
                  op, o.start().orElse(null), o.end().orElse(null), Values.DEFAULT_SEARCH_WINDOW);
          long s = Values.micros(w[0]);
          long e = Values.micros(w[1]);
          String type = o.streamType().wireValue();
          CompletableFuture<SqlSearchResult> rows =
              api.searchAsync(op, sql, s, e, o.from(), o.size(), false, type)
                  .thenApply(Logs::sqlResult);
          if (!o.trackTotalHits()) {
            return rows;
          }
          return rows.thenCompose(
              res ->
                  api.searchAsync(op, sql, s, e, 0, 0, true, type)
                      .thenApply(
                          c -> res.withTotal(c.path("hits").path(0).path("zo_sql_num").asLong(0))));
        });
  }

  static SqlSearchResult sqlResult(JsonNode w) {
    List<Map<String, Object>> hits = new ArrayList<>();
    for (JsonNode h : w.path("hits")) {
      hits.add(Values.row(h));
    }
    String id = Values.text(w.get("trace_id"));
    return new SqlSearchResult(
        hits,
        OptionalLong.empty(),
        w.path("from").asInt(0),
        w.path("size").asInt(0),
        w.path("took").asLong(0),
        w.path("scan_records").asLong(0),
        id.isEmpty() ? Optional.empty() : Optional.of(id),
        w.path("is_partial").asBoolean(false),
        w.path("histogram_interval").asLong(0));
  }

  /**
   * Returns up to {@code size} rows of a stream around a timestamp, the anchor row included ({@code
   * GET /api/{org}/{stream}/_around}).
   *
   * @param stream stream name
   * @param timestamp anchor time
   * @param size number of rows (&lt;= 0 means 10)
   * @return the rows
   */
  public SqlSearchResult searchAround(String stream, Instant timestamp, int size) {
    return Api.await(searchAroundAsync(stream, timestamp, size), "logs.searchAround");
  }

  /**
   * Asynchronous {@link #searchAround}.
   *
   * @param stream stream name
   * @param timestamp anchor time
   * @param size number of rows
   * @return future rows
   */
  public CompletableFuture<SqlSearchResult> searchAroundAsync(
      String stream, Instant timestamp, int size) {
    String op = "logs.searchAround";
    return Api.defer(
        () -> {
          String s = Values.stream(op, stream);
          if (timestamp == null) {
            throw new ValidationException(op + ": timestamp is required");
          }
          return api.jsonAsync(
                  TransportRequest.get(api.orgPath("/" + s + "/_around"))
                      .query("key", Values.micros(timestamp))
                      .query("size", size <= 0 ? 10 : size)
                      .query("type", StreamType.LOGS.wireValue())
                      .operation(op))
              .thenApply(Logs::sqlResult);
        });
  }

  /**
   * Returns the most frequent values of fields over the last 15 minutes.
   *
   * @param stream stream name
   * @param fields field names (at least one)
   * @return values per field
   */
  public List<FieldValues> valuesOf(String stream, List<String> fields) {
    return valuesOf(stream, fields, ValuesOptions.defaults());
  }

  /**
   * Returns the most frequent values of fields ({@code GET /api/{org}/{stream}/_values}).
   *
   * @param stream stream name
   * @param fields field names (at least one)
   * @param options window and values per field
   * @return values per field
   */
  public List<FieldValues> valuesOf(String stream, List<String> fields, ValuesOptions options) {
    return Api.await(valuesOfAsync(stream, fields, options), "logs.valuesOf");
  }

  /**
   * Asynchronous {@link #valuesOf(String, List, ValuesOptions)}.
   *
   * @param stream stream name
   * @param fields field names
   * @param options options
   * @return future values
   */
  public CompletableFuture<List<FieldValues>> valuesOfAsync(
      String stream, List<String> fields, ValuesOptions options) {
    String op = "logs.valuesOf";
    return Api.defer(
        () -> {
          String s = Values.stream(op, stream);
          if (fields == null || fields.isEmpty()) {
            throw new ValidationException(op + ": at least one field is required");
          }
          ValuesOptions o = options == null ? ValuesOptions.defaults() : options;
          Instant[] w =
              Values.window(
                  op, o.start().orElse(null), o.end().orElse(null), Values.DEFAULT_SEARCH_WINDOW);
          return api.jsonAsync(
                  TransportRequest.get(api.orgPath("/" + s + "/_values"))
                      .query("fields", String.join(",", fields))
                      .query("start_time", Values.micros(w[0]))
                      .query("end_time", Values.micros(w[1]))
                      .query("size", o.size() <= 0 ? 10 : o.size())
                      .query("type", StreamType.LOGS.wireValue())
                      .operation(op))
              .thenApply(Logs::fieldValues);
        });
  }

  private static List<FieldValues> fieldValues(JsonNode body) {
    List<FieldValues> out = new ArrayList<>();
    for (JsonNode h : body.path("hits")) {
      List<ValueCount> values = new ArrayList<>();
      for (JsonNode v : h.path("values")) {
        values.add(
            new ValueCount(
                LogRecordJson.toAttrValue(v.get("zo_sql_key")),
                Values.asLong(v.get("zo_sql_num"), 0)));
      }
      out.add(new FieldValues(Values.text(h.get("field")), values));
    }
    return out;
  }

  /**
   * Counts records of a stream per time bucket over the last 15 minutes.
   *
   * @param stream stream name
   * @return buckets
   */
  public HistogramResult histogram(String stream) {
    return histogram(stream, HistogramOptions.defaults());
  }

  /**
   * Counts records of a stream per time bucket (SQL {@code histogram()} through {@code _search}).
   *
   * @param stream stream name
   * @param options window, interval and filter
   * @return buckets
   */
  public HistogramResult histogram(String stream, HistogramOptions options) {
    return Api.await(histogramAsync(stream, options), "logs.histogram");
  }

  /**
   * Asynchronous {@link #histogram(String, HistogramOptions)}.
   *
   * @param stream stream name
   * @param options options
   * @return future buckets
   */
  public CompletableFuture<HistogramResult> histogramAsync(
      String stream, HistogramOptions options) {
    String op = "logs.histogram";
    return Api.defer(
        () -> {
          String s = Values.stream(op, stream);
          HistogramOptions o = options == null ? HistogramOptions.defaults() : options;
          Instant[] w =
              Values.window(
                  op, o.start().orElse(null), o.end().orElse(null), Values.DEFAULT_SEARCH_WINDOW);
          String hist = "histogram(_timestamp)";
          if (o.interval().isPresent()) {
            Duration d = o.interval().get();
            if (d.isNegative() || d.isZero()) {
              throw new ValidationException(op + ": interval must be positive");
            }
            hist = "histogram(_timestamp, " + Values.sqlString(intervalText(d)) + ")";
          }
          StringBuilder sql =
              new StringBuilder("SELECT ")
                  .append(hist)
                  .append(" AS zo_sql_key, count(*) AS zo_sql_num FROM ")
                  .append(Values.sqlIdent(s));
          String filter = o.filter().map(String::trim).orElse("");
          if (!filter.isEmpty()) {
            sql.append(" WHERE ").append(filter);
          }
          sql.append(" GROUP BY zo_sql_key ORDER BY zo_sql_key");
          return api.searchAsync(
                  op,
                  sql.toString(),
                  Values.micros(w[0]),
                  Values.micros(w[1]),
                  0,
                  0,
                  false,
                  StreamType.LOGS.wireValue())
              .thenApply(Logs::histogramResult);
        });
  }

  private static HistogramResult histogramResult(JsonNode w) {
    List<HistogramBucket> buckets = new ArrayList<>();
    for (JsonNode h : w.path("hits")) {
      buckets.add(
          new HistogramBucket(
              parseBucketTime(Values.text(h.get("zo_sql_key"))),
              Values.asLong(h.get("zo_sql_num"), 0)));
    }
    return new HistogramResult(Duration.ofSeconds(w.path("histogram_interval").asLong(0)), buckets);
  }

  /**
   * Renders a duration as an SQL interval ({@code "5 minute"}, {@code "30 second"}).
   *
   * @param d positive duration
   * @return the interval text
   */
  static String intervalText(Duration d) {
    long seconds = Math.max(d.getSeconds(), 1);
    if (seconds % 3600 == 0) {
      return (seconds / 3600) + " hour";
    }
    if (seconds % 60 == 0) {
      return (seconds / 60) + " minute";
    }
    return seconds + " second";
  }

  /**
   * Parses a histogram key: naive UTC {@code 2026-11-18T20:03:00}, RFC 3339 or Unix microseconds.
   *
   * @param text key
   * @return the instant ({@link Instant#EPOCH} when unparseable)
   */
  static Instant parseBucketTime(String text) {
    String t = text.trim();
    try {
      return LocalDateTime.parse(t.replace(' ', 'T')).toInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
      // try the next form
    }
    try {
      return OffsetDateTime.parse(t).toInstant();
    } catch (DateTimeParseException ignored) {
      // try the next form
    }
    try {
      return Values.fromMicros(Long.parseLong(t));
    } catch (NumberFormatException e) {
      return Instant.EPOCH;
    }
  }
}
