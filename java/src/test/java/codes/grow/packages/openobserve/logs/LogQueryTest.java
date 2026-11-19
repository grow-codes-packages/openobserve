package codes.grow.packages.openobserve.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.Fixtures;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.logging.LogRecord;
import codes.grow.packages.openobserve.logging.LogSearchResult;
import codes.grow.packages.openobserve.logging.SearchOptions;
import codes.grow.packages.openobserve.logging.Severity;
import codes.grow.packages.openobserve.logging.SortOrder;
import codes.grow.packages.openobserve.logging.TailHandle;
import codes.grow.packages.openobserve.logging.TailOptions;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogQueryTest {
  private MockServer server;
  private OpenObserveClient client;
  private OpenObserveLogQuery query;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).stream("App-Logs").build();
    query = client.logQuery();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  private String sql(int i) {
    return Json.tree(server.requests().get(i).body().getBytes(StandardCharsets.UTF_8))
        .get("query")
        .get("sql")
        .asText();
  }

  @Test
  void buildSql() {
    assertThat(query.stream()).isEqualTo("app_logs");
    assertThat(query.buildSql("  select 1 ", SortOrder.DESC)).isEqualTo("select 1");
    assertThat(query.buildSql("it's", SortOrder.DESC))
        .isEqualTo("SELECT * FROM \"app_logs\" WHERE match_all('it''s') ORDER BY _timestamp DESC");
    assertThat(query.buildSql("", SortOrder.ASC))
        .isEqualTo("SELECT * FROM \"app_logs\" ORDER BY _timestamp ASC");
    assertThat(query.buildSql(null, SortOrder.ASC)).endsWith("ASC");
  }

  @Test
  void searchNormalizesRowsAndPages() {
    Fixtures.enqueue(server, "search.otlp_logs");
    server.enqueue(200, "{\"hits\":[]}");
    LogSearchResult r =
        query.search("needle", SearchOptions.builder().limit(2).cursor("4").build());
    assertThat(sql(0)).contains("match_all('needle')");
    JsonNode q =
        Json.tree(server.requests().get(0).body().getBytes(StandardCharsets.UTF_8)).get("query");
    assertThat(q.get("from").asInt()).isEqualTo(4);
    assertThat(q.get("size").asInt()).isEqualTo(2);
    assertThat(r.records()).hasSizeGreaterThanOrEqualTo(2);
    LogRecord first = r.records().get(0);
    assertThat(first.body()).isEqualTo("otlp pb needle6moiazbq");
    assertThat(first.severity()).isEqualTo(Severity.WARN);
    assertThat(first.severityText()).isEqualTo("WARN");
    assertThat(first.traceId()).contains("038a1ed5af73678e47809f854dabaae9");
    assertThat(first.spanId()).contains("00f067aa0ba902b7");
    assertThat(first.resource()).containsEntry("service.name", "sdk-a-6moiazbq");
    assertThat(first.resource()).containsEntry("service.version", "1.2.3");
    assertThat(first.attributes()).containsEntry("user_id", 42L).doesNotContainKey("body");
    assertThat(first.timestamp()).isEqualTo(Instant.parse("2026-11-18T20:20:00.453175Z"));
    assertThat(r.nextCursor()).contains(Integer.toString(4 + r.records().size()));
    LogSearchResult last = query.searchAsync("x", null).join();
    assertThat(last.nextCursor()).isEmpty();
    assertThat(last.total()).isEmpty();
  }

  @Test
  void searchValidation() {
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().cursor("abc").build()))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("cursor");
    assertThatThrownBy(() -> query.search("x", SearchOptions.builder().cursor("-1").build()))
        .isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> client.logQuery(" ")).isInstanceOf(ValidationException.class);
  }

  @Test
  void recordFromRowVariants() {
    Map<String, Object> row = new HashMap<>();
    row.put("_timestamp", 1795032000000000L);
    row.put("_o2_ingest_ts", 1L);
    row.put("message", "plain");
    row.put("level", "error");
    row.put("trace_id", "NOT-HEX");
    row.put("span_id", 5L);
    row.put("trace_flags", 1L);
    row.put("host_name", "h1");
    row.put("deployment_environment", "prod");
    LogRecord r = OpenObserveLogQuery.recordFromRow(row);
    assertThat(r.body()).isEqualTo("plain");
    assertThat(r.severity()).isEqualTo(Severity.ERROR);
    assertThat(r.severityText()).isEqualTo("error");
    assertThat(r.traceId()).isEmpty();
    assertThat(r.attributes()).containsEntry("trace_id", "NOT-HEX").containsEntry("span_id", 5L);
    assertThat(r.traceFlags()).hasValue(1);
    assertThat(r.resource())
        .containsEntry("host.name", "h1")
        .containsEntry("deployment.environment", "prod");
    LogRecord bare =
        OpenObserveLogQuery.recordFromRow(
            Map.of("severity_number", 99L, "trace_flags", 999L, "severity", "custom", "body", 3L));
    assertThat(bare.body()).isEqualTo("3");
    assertThat(bare.severity()).isEqualTo(Severity.INFO);
    assertThat(bare.severityText()).isEqualTo("custom");
    assertThat(bare.attributes()).containsKeys("severity_number", "trace_flags");
    assertThat(bare.timestamp()).isEqualTo(Instant.EPOCH);
    LogRecord numbered = OpenObserveLogQuery.recordFromRow(Map.of("severity_number", 17L));
    assertThat(numbered.severity()).isEqualTo(Severity.ERROR);
    assertThat(numbered.body()).isEmpty();
    LogRecord span =
        OpenObserveLogQuery.recordFromRow(
            Map.of("span_id", "ZZZZ", "trace_id", 7L, "_timestamp", "x"));
    assertThat(span.spanId()).isEmpty();
    assertThat(span.attributes()).containsKeys("span_id", "trace_id");
    assertThat(OpenObserveLogQuery.hash(Map.of("a", 1, "b", 2)))
        .isEqualTo(OpenObserveLogQuery.hash(new java.util.TreeMap<>(Map.of("b", 2, "a", 1))));
  }

  @Test
  void listStreamsAndLabels() {
    Fixtures.enqueue(server, "listStreams.logs");
    Fixtures.enqueue(server, "getStreamSchema.ok");
    List<String> streams = query.listStreams();
    assertThat(streams).contains("passcode_6moiazbq");
    List<String> labels = query.listLabels();
    assertThat(labels).contains("_timestamp", "message");
    assertThat(server.requests().get(1).uri())
        .isEqualTo("/api/default/streams/app_logs/schema?type=logs");
  }

  @Test
  void waitForLogUsesSearch() {
    server.enqueue(200, "{\"hits\":[]}");
    server.enqueue(200, Fixtures.body("search.otlp_logs"));
    LogRecord r =
        query.waitForLog(
            "needle",
            codes.grow.packages.openobserve.logging.WaitForLogOptions.of(
                Duration.ofSeconds(5), Duration.ofMillis(10)));
    assertThat(r.body()).contains("needle");
  }

  @Test
  void tailPollsDeduplicatesAndReportsErrors() throws Exception {
    long now = codes.grow.packages.openobserve.internal.Values.micros(Instant.now());
    String row1 = "{\"_timestamp\":" + now + ",\"body\":\"one\"}";
    String row2 = "{\"_timestamp\":" + (now + 5) + ",\"body\":\"two\"}";
    String old = "{\"_timestamp\":1,\"body\":\"old\"}";
    AtomicInteger polls = new AtomicInteger();
    server.fallback(
        ex -> {
          int n = polls.incrementAndGet();
          if (n == 1) {
            MockServer.send(ex, 200, "{\"hits\":[" + row1 + "," + old + "]}");
          } else if (n == 2) {
            MockServer.send(ex, 400, "{\"code\":400,\"message\":\"boom\"}");
          } else {
            MockServer.send(ex, 200, "{\"hits\":[" + row1 + "," + row2 + "]}");
          }
        });
    List<String> got = new CopyOnWriteArrayList<>();
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    TailOptions opts =
        new TailOptions(Optional.of(Instant.now().minusSeconds(1)), Duration.ofMillis(20), null)
            .withOnError(errors::add);
    TailHandle h =
        query.tail(
            "x",
            r -> {
              got.add(r.body());
              throw new IllegalStateException("callback errors are ignored");
            },
            opts);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (got.size() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    h.stop();
    h.close();
    assertThat(got).containsExactly("one", "two");
    assertThat(errors).isNotEmpty();
    assertThat(sql(0)).endsWith("ORDER BY _timestamp ASC");
    assertThatThrownBy(() -> query.tail("x", null, null)).isInstanceOf(ValidationException.class);
  }

  @Test
  void tailFollowsFullPagesAndDefaults() throws Exception {
    long now = codes.grow.packages.openobserve.internal.Values.micros(Instant.now());
    StringBuilder page = new StringBuilder("{\"hits\":[");
    for (int i = 0; i < OpenObserveLogQuery.TAIL_PAGE_SIZE; i++) {
      page.append(i == 0 ? "" : ",")
          .append("{\"_timestamp\":")
          .append(now + 10_000_000 + i)
          .append(",\"i\":")
          .append(i)
          .append('}');
    }
    String full = page.append("]}").toString();
    AtomicInteger calls = new AtomicInteger();
    server.fallback(
        ex -> MockServer.send(ex, 200, calls.incrementAndGet() == 1 ? full : "{\"hits\":[]}"));
    AtomicInteger got = new AtomicInteger();
    TailHandle h =
        query.tail("", r -> got.incrementAndGet(), new TailOptions(null, Duration.ZERO, null));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (calls.get() < 2 && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    h.stop();
    assertThat(got).hasValue(OpenObserveLogQuery.TAIL_PAGE_SIZE);
    TailHandle h2 = query.tail("", r -> {}, null);
    h2.stop();
  }
}
