package codes.grow.packages.openobserve.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.Fixtures;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.OpenObserveException;
import codes.grow.packages.openobserve.core.UnavailableException;
import codes.grow.packages.openobserve.core.ValidationException;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogsTest {
  private MockServer server;
  private OpenObserveClient client;
  private Logs logs;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).build();
    logs = client.logs();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  private JsonNode body(int i) {
    return Json.tree(server.requests().get(i).body().getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void ingestJson() {
    Fixtures.enqueue(server, "ingestJson.ok");
    Fixtures.enqueue(server, "ingestJson.200_partial_failure");
    IngestResult ok = logs.ingestJson("Sdk-Logs", List.of(Map.of("message", "a", "n", 1)));
    assertThat(ok.successful()).isEqualTo(3);
    assertThat(ok.failed()).isZero();
    assertThat(ok.streams()).hasSize(1);
    assertThat(ok.streams().get(0).error()).isEmpty();
    MockServer.Recorded r = server.requests().get(0);
    assertThat(r.method()).isEqualTo("POST");
    assertThat(r.uri()).isEqualTo("/api/default/sdk_logs/_json");
    assertThat(body(0).get(0).get("message").asText()).isEqualTo("a");
    IngestResult partial = logs.ingestJsonAsync("s", List.of(Map.of("message", "old"))).join();
    assertThat(partial.failed()).isEqualTo(1);
    assertThat(partial.streams().get(0).error())
        .hasValueSatisfying(e -> assertThat(e).contains("Too old"));
  }

  @Test
  void ingestErrors() {
    Fixtures.enqueue(server, "ingestJson.400_invalid_json");
    Fixtures.enqueue(server, "ingestJson.401");
    server.enqueue(200, "not json");
    assertThatThrownBy(() -> logs.ingestJson("s", List.of(Map.of("a", 1))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("SerdeJsonError")
        .hasMessageContaining("HTTP 400");
    assertThatThrownBy(() -> logs.ingestJson("s", List.of(Map.of("a", 1))))
        .isInstanceOf(AuthenticationException.class)
        .message()
        .doesNotContain(Fixtures.PASSWORD);
    assertThatThrownBy(() -> logs.ingestJson("s", List.of(Map.of("a", 1))))
        .isInstanceOf(UnavailableException.class)
        .hasMessageContaining("invalid JSON");
    assertThatThrownBy(() -> logs.ingestJson(" ", List.of(Map.of("a", 1))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("stream name");
    assertThatThrownBy(() -> logs.ingestJson(null, List.of(Map.of("a", 1))))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestJson("s", List.of()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestJson("s", null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestJsonAsync("s", List.of()).join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(ValidationException.class);
  }

  @Test
  void ingestMulti() {
    Fixtures.enqueue(server, "ingestMulti.ok");
    Fixtures.enqueue(server, "ingestMulti.400_invalid_line");
    IngestResult r = logs.ingestMulti("s", "{\"a\":1}\n{\"a\":2}\n");
    assertThat(r.successful()).isPositive();
    assertThat(server.requests().get(0).header("Content-Type")).isEqualTo("application/x-ndjson");
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/s/_multi");
    assertThatThrownBy(() -> logs.ingestMultiAsync("s", "{bad").join())
        .hasCauseInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestMulti("s", "  ")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestMulti("s", null)).isInstanceOf(ValidationException.class);
  }

  @Test
  void ingestBulk() {
    Fixtures.enqueue(server, "ingestBulk.ok");
    Fixtures.enqueue(server, "ingestBulk.200_item_error");
    server.enqueue(
        200,
        "{\"took\":1,\"errors\":true,\"items\":[{\"index\":{\"_index\":\"s\",\"status\":400,"
            + "\"error\":{\"reason\":\"only reason\"}}},{\"create\":{\"_index\":\"s\",\"status\":400,"
            + "\"error\":{\"type\":\"only_type\"}}}]}");
    BulkResult ok =
        logs.ingestBulk(
            List.of(
                BulkAction.index("Bulk-S", Map.of("message", "a")),
                BulkAction.create("bulk_s", Map.of("message", "b"))));
    assertThat(ok.errors()).isFalse();
    assertThat(ok.items()).isNotEmpty();
    assertThat(ok.items().get(0).error()).isEmpty();
    String sent = server.requests().get(0).body();
    assertThat(sent)
        .startsWith("{\"index\":{\"_index\":\"bulk_s\"}}\n{\"message\":\"a\"}\n")
        .contains("{\"create\":{\"_index\":\"bulk_s\"}}")
        .endsWith("\n");
    BulkResult failed = logs.ingestBulkAsync(List.of(new BulkAction(null, "s", Map.of()))).join();
    assertThat(failed.errors()).isTrue();
    assertThat(failed.items().get(0).status()).isEqualTo(422);
    assertThat(failed.items().get(0).error())
        .hasValueSatisfying(e -> assertThat(e).startsWith("timestamp_parsing_failed: Too old"));
    BulkResult partial = logs.ingestBulk(List.of(BulkAction.index("s", Map.of())));
    assertThat(partial.items().get(0).error()).contains("only reason");
    assertThat(partial.items().get(1).error()).contains("only_type");
    assertThat(partial.items().get(1).action()).isEqualTo("create");
    assertThatThrownBy(() -> logs.ingestBulk(List.of(new BulkAction("delete", "s", Map.of()))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("delete");
    assertThatThrownBy(() -> logs.ingestBulk(List.of(new BulkAction("index", "s", null))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("document");
    assertThatThrownBy(() -> logs.ingestBulk(List.of(new BulkAction("", "", Map.of()))))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestBulk(List.of())).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.ingestBulk(null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> BulkAction.index("s", null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void searchSendsMicrosecondsAndParsesRows() {
    Fixtures.enqueue(server, "search.ok");
    Instant start = Instant.parse("2026-11-18T20:00:00Z");
    Instant end = Instant.parse("2026-11-18T21:00:00.000001Z");
    SqlSearchResult r =
        logs.search(
            "SELECT * FROM \"s\"",
            SqlSearchOptions.builder()
                .start(start)
                .end(end)
                .from(1)
                .size(2)
                .streamType(StreamType.LOGS)
                .build());
    JsonNode q = body(0).get("query");
    assertThat(q.get("start_time").asLong()).isEqualTo(1795032000000000L);
    assertThat(q.get("end_time").asLong()).isEqualTo(1795035600000001L);
    assertThat(q.get("from").asInt()).isEqualTo(1);
    assertThat(q.get("size").asInt()).isEqualTo(2);
    assertThat(q.has("track_total_hits")).isFalse();
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/_search?type=logs");
    assertThat(r.hits()).isNotEmpty();
    assertThat(r.hits().get(0)).containsKey("_timestamp");
    assertThat(r.total()).isEmpty();
    assertThat(r.searchId()).isPresent();
    assertThat(r.partial()).isFalse();
  }

  @Test
  void searchWithTotalAndDefaults() {
    Fixtures.enqueue(server, "search.paging");
    Fixtures.enqueue(server, "search.track_total_hits");
    server.enqueue(200, "{\"hits\":[]}");
    SqlSearchResult r =
        logs.search("SELECT * FROM s", SqlSearchOptions.builder().trackTotalHits(true).build());
    assertThat(r.total()).isPresent();
    assertThat(r.total().getAsLong()).isPositive();
    JsonNode count = body(1).get("query");
    assertThat(count.get("track_total_hits").asBoolean()).isTrue();
    long window = count.get("end_time").asLong() - count.get("start_time").asLong();
    assertThat(window).isEqualTo(Duration.ofMinutes(15).toNanos() / 1000);
    SqlSearchResult empty = logs.search("SELECT 1", null);
    assertThat(empty.hits()).isEmpty();
    assertThat(empty.searchId()).isEmpty();
  }

  @Test
  void searchErrors() {
    Fixtures.enqueue(server, "search.400_stream_not_found");
    Fixtures.enqueue(server, "search.400_unknown_field");
    Fixtures.enqueue(server, "search.404_unknown_org");
    Fixtures.enqueue(server, "search.422_missing_start_time");
    Fixtures.enqueue(server, "search.400_bad_sql");
    OpenObserveException nf = (OpenObserveException) catchSdk(() -> logs.search("SELECT * FROM x"));
    assertThat(nf).isInstanceOf(NotFoundException.class);
    assertThat(nf.httpStatus()).hasValue(400);
    assertThat(nf.requestId()).contains("01a1f050f79b75028dc692d5e926b1aa");
    assertThat(nf.getMessage()).contains("Search stream not found");
    assertThatThrownBy(() -> logs.search("SELECT nocol FROM s"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("unknown field");
    assertThatThrownBy(() -> logs.search("SELECT 1"))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("Organization not found");
    assertThatThrownBy(() -> logs.search("SELECT 1"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("HTTP 422");
    assertThatThrownBy(() -> logs.search("SELEC"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("SQL error");
    assertThatThrownBy(() -> logs.search(" ")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.search(null)).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.search("SELECT 1", SqlSearchOptions.builder().from(-1).build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.search("SELECT 1", SqlSearchOptions.builder().size(-2).build()))
        .isInstanceOf(ValidationException.class);
    assertThat(SqlSearchOptions.defaults().size()).isEqualTo(100);
    Instant now = Instant.now();
    assertThatThrownBy(
            () ->
                logs.search(
                    "SELECT 1",
                    SqlSearchOptions.builder().start(now).end(now.minusSeconds(1)).build()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("start must not be after end");
  }

  private static Throwable catchSdk(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException e) {
      return e;
    }
    throw new AssertionError("no exception");
  }

  @Test
  void searchAround() {
    Fixtures.enqueue(server, "searchAround.ok");
    Fixtures.enqueue(server, "searchAround.400_stream_not_found");
    Instant t = Instant.parse("2026-11-18T20:00:00Z");
    SqlSearchResult r = logs.searchAround("s", t, 0);
    assertThat(r.hits()).isNotEmpty();
    assertThat(server.requests().get(0).uri())
        .startsWith("/api/default/s/_around?")
        .contains("key=1795032000000000")
        .contains("size=10")
        .contains("type=logs");
    assertThatThrownBy(() -> logs.searchAroundAsync("nostream", t, 5).join())
        .hasCauseInstanceOf(NotFoundException.class);
    assertThatThrownBy(() -> logs.searchAround("s", null, 5))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void valuesOf() {
    Fixtures.enqueue(server, "valuesOf.ok");
    Fixtures.enqueue(server, "valuesOf.ok");
    Fixtures.enqueue(server, "valuesOf.400_missing_time");
    List<FieldValues> v = logs.valuesOf("s", List.of("level", "run_id"));
    assertThat(v).hasSize(2);
    assertThat(v.get(0).field()).isEqualTo("level");
    assertThat(v.get(0).values().get(0)).isEqualTo(new ValueCount("debug", 2));
    assertThat(server.requests().get(0).uri())
        .startsWith("/api/default/s/_values?")
        .contains("fields=level%2Crun_id")
        .contains("start_time=")
        .contains("size=10")
        .contains("type=logs");
    Instant end = Instant.parse("2026-11-18T21:00:00Z");
    logs.valuesOf("s", List.of("level"), ValuesOptions.of(end.minusSeconds(60), end, 3));
    assertThat(server.requests().get(1).uri())
        .contains("start_time=1795035540000000")
        .contains("end_time=1795035600000000")
        .contains("size=3");
    assertThatThrownBy(() -> logs.valuesOfAsync("s", List.of("x"), null).join())
        .hasCauseInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.valuesOf("s", List.of())).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.valuesOf("s", null)).isInstanceOf(ValidationException.class);
    assertThat(new ValuesOptions(null, null, 0).start()).isEmpty();
  }

  @Test
  void histogram() {
    Fixtures.enqueue(server, "histogram.ok");
    server.enqueue(
        200,
        "{\"hits\":[{\"zo_sql_key\":\"2026-11-18T20:03:00Z\",\"zo_sql_num\":1},"
            + "{\"zo_sql_key\":\"1795032000000000\",\"zo_sql_num\":2},"
            + "{\"zo_sql_key\":\"garbage\",\"zo_sql_num\":\"3\"}]}");
    server.enqueue(200, "{\"hits\":[]}");
    HistogramResult r =
        logs.histogram("s", HistogramOptions.of(null, null, Duration.ofMinutes(1), "run_id = 'x'"));
    assertThat(r.interval()).isEqualTo(Duration.ofSeconds(60));
    assertThat(r.buckets()).hasSize(1);
    assertThat(r.buckets().get(0).start()).isEqualTo(Instant.parse("2026-11-18T20:20:00Z"));
    assertThat(r.buckets().get(0).count()).isEqualTo(5);
    assertThat(body(0).get("query").get("sql").asText())
        .isEqualTo(
            "SELECT histogram(_timestamp, '1 minute') AS zo_sql_key, count(*) AS zo_sql_num FROM"
                + " \"s\" WHERE run_id = 'x' GROUP BY zo_sql_key ORDER BY zo_sql_key");
    HistogramResult other = logs.histogram("s");
    assertThat(other.buckets())
        .extracting(HistogramBucket::start)
        .containsExactly(
            Instant.parse("2026-11-18T20:03:00Z"),
            Instant.parse("2026-11-18T20:00:00Z"),
            Instant.EPOCH);
    assertThat(other.buckets().get(2).count()).isEqualTo(3);
    assertThat(body(1).get("query").get("sql").asText())
        .startsWith("SELECT histogram(_timestamp) AS");
    logs.histogramAsync("s", HistogramOptions.of(null, null, null, "  ")).join();
    assertThat(body(2).get("query").get("sql").asText()).doesNotContain("WHERE");
    assertThatThrownBy(
            () -> logs.histogram("s", HistogramOptions.of(null, null, Duration.ZERO, null)))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(
            () ->
                logs.histogram("s", HistogramOptions.of(null, null, Duration.ofSeconds(-1), null)))
        .isInstanceOf(ValidationException.class);
    server.enqueue(200, "{\"hits\":[]}");
    assertThat(logs.histogram("s", null).buckets()).isEmpty();
  }

  @Test
  void intervalText() {
    assertThat(Logs.intervalText(Duration.ofHours(2))).isEqualTo("2 hour");
    assertThat(Logs.intervalText(Duration.ofMinutes(5))).isEqualTo("5 minute");
    assertThat(Logs.intervalText(Duration.ofSeconds(30))).isEqualTo("30 second");
    assertThat(Logs.intervalText(Duration.ofMillis(10))).isEqualTo("1 second");
    assertThat(Logs.parseBucketTime("2026-11-18 20:03:00"))
        .isEqualTo(Instant.parse("2026-11-18T20:03:00Z"));
  }

  @Test
  void modelDefaults() {
    assertThat(StreamType.parse("TRACES")).isEqualTo(StreamType.TRACES);
    assertThat(StreamType.parse("weird")).isEqualTo(StreamType.LOGS);
    assertThat(StreamType.METRICS.wireValue()).isEqualTo("metrics");
    assertThat(new IngestStreamStatus("s", 1, 0, null).error()).isEmpty();
    assertThat(new BulkItem("index", "s", 200, null).error()).isEmpty();
    assertThat(HistogramOptions.defaults().interval()).isEmpty();
    assertThat(new HistogramOptions(null, null, null, null).filter()).isEmpty();
    SqlSearchOptions d = SqlSearchOptions.defaults();
    assertThat(d.start()).isEmpty();
    assertThat(d.end()).isEmpty();
    assertThat(d.trackTotalHits()).isFalse();
    assertThat(d.streamType()).isEqualTo(StreamType.LOGS);
    SqlSearchResult r = new SqlSearchResult(List.of(), null, 0, 0, 0, 0, null, false, 0);
    assertThat(r.total()).isEmpty();
    assertThat(r.searchId()).isEmpty();
  }

  @Test
  void ingestOtlp() {
    server.enqueue(200, "{}");
    server.enqueue(
        200, "{\"partialSuccess\":{\"rejectedLogRecords\":\"1\",\"errorMessage\":\"old\"}}");
    codes.grow.packages.openobserve.logging.LogRecord rec =
        codes.grow.packages.openobserve.logging.LogRecord.builder().body("otlp").build();
    assertThat(logs.ingestOtlp(List.of(), "Otlp.S", false)).isEqualTo(new OtlpIngestResult(0, 0));
    assertThat(logs.ingestOtlp(null)).isEqualTo(new OtlpIngestResult(0, 0));
    assertThat(server.requests()).isEmpty();
    assertThat(logs.ingestOtlp(List.of(rec, rec), "Otlp.S", true))
        .isEqualTo(new OtlpIngestResult(2, 0));
    MockServer.Recorded r = server.requests().get(0);
    assertThat(r.uri()).isEqualTo("/api/default/v1/logs");
    assertThat(r.header("stream-name")).isEqualTo("otlp_s");
    assertThat(r.header("content-encoding")).isEqualTo("gzip");
    assertThat(logs.ingestOtlpAsync(List.of(rec, rec), null, false).join())
        .isEqualTo(new OtlpIngestResult(1, 1));
    assertThatThrownBy(() -> logs.ingestOtlp(List.of(rec), " ", false))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void tailPollsFromStartAndReportsErrors() throws Exception {
    Instant start = Instant.now().plusSeconds(1);
    long s = codes.grow.packages.openobserve.internal.Values.micros(start);
    String row1 = "{\"_timestamp\":" + (s + 1) + ",\"message\":\"one\"}";
    String row2 = "{\"_timestamp\":" + (s + 2) + ",\"message\":\"two\"}";
    String old = "{\"_timestamp\":1,\"message\":\"old\"}";
    java.util.concurrent.atomic.AtomicInteger polls =
        new java.util.concurrent.atomic.AtomicInteger();
    server.fallback(
        ex -> {
          int n = polls.incrementAndGet();
          if (n == 1) {
            MockServer.send(ex, 200, "{\"hits\":[" + old + "," + row1 + "]}");
          } else if (n == 2) {
            MockServer.send(ex, 400, "{\"code\":400,\"message\":\"boom\"}");
          } else {
            MockServer.send(ex, 200, "{\"hits\":[" + row1 + "," + row2 + "]}");
          }
        });
    List<String> got = new java.util.concurrent.CopyOnWriteArrayList<>();
    List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
    LogTailOptions opts =
        LogTailOptions.builder()
            .filter("src = 'x'")
            .start(start)
            .interval(Duration.ofMillis(10))
            .overlap(Duration.ofSeconds(2))
            .pageSize(5)
            .streamType(StreamType.LOGS)
            .onError(errors::add)
            .build();
    assertThat(opts.filter()).contains("src = 'x'");
    var h = logs.tail("Tail.S", row -> got.add(String.valueOf(row.get("message"))), opts);
    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
    while ((got.size() < 2 || errors.isEmpty()) && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    h.stop();
    h.stop();
    assertThat(got).containsExactly("one", "two");
    assertThat(errors).first().isInstanceOf(ValidationException.class);
    JsonNode q = body(0).get("query");
    assertThat(q.get("sql").asText())
        .isEqualTo(
            "SELECT * FROM \"tail_s\" WHERE _timestamp >= "
                + s
                + " AND (src = 'x') ORDER BY _timestamp ASC");
    assertThat(q.get("start_time").asLong()).isEqualTo(s);
    assertThat(q.get("size").asInt()).isEqualTo(5);
    assertThatThrownBy(() -> logs.tail(" ", row -> {}, null))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> logs.tail("s", null, null)).isInstanceOf(ValidationException.class);
    LogTailOptions d = LogTailOptions.defaults();
    assertThat(d.filter()).isEmpty();
    assertThat(d.start()).isEmpty();
    assertThat(d.interval()).isEqualTo(Duration.ofSeconds(1));
    assertThat(d.overlap()).isEqualTo(Duration.ofSeconds(1));
    assertThat(d.pageSize()).isEqualTo(1000);
    assertThat(d.streamType()).isEqualTo(StreamType.LOGS);
    d.onError().accept(new RuntimeException("ignored"));
    assertThat(
            LogTailOptions.builder()
                .filter("  ")
                .interval(Duration.ofMillis(-1))
                .overlap(Duration.ofMillis(-1))
                .build()
                .filter())
        .isEmpty();
    logs.tail("s", row -> {}, LogTailOptions.builder().interval(Duration.ofHours(1)).build())
        .stop();
  }
}
