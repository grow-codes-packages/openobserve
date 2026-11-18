package codes.grow.packages.openobserve.traces;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.Fixtures;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.UnsupportedException;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.tracing.query.Span;
import codes.grow.packages.openobserve.tracing.query.Trace;
import codes.grow.packages.openobserve.tracing.query.TraceSearchQuery;
import codes.grow.packages.openobserve.tracing.query.TraceSummary;
import codes.grow.packages.openobserve.tracing.query.WaitForTraceOptions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TraceQueryTest {
  private static final String ID = "038a1ed5af73678e47809f854dabaae9";

  private MockServer server;
  private OpenObserveClient client;
  private OpenObserveTraceQuery traces;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).tracesStream("Sdk-Traces").build();
    traces = client.traceQuery();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  @Test
  void getTraceMapsSpans() {
    Fixtures.enqueue(server, "getTrace.details");
    Optional<Trace> t = traces.getTrace(ID.toUpperCase());
    assertThat(t).isPresent();
    Trace trace = t.get();
    assertThat(trace.traceId()).isEqualTo(ID);
    assertThat(trace.spans()).hasSize(3);
    Span root = trace.root().orElseThrow();
    assertThat(root.name()).isEqualTo("GET /checkout");
    assertThat(root.kind()).isEqualTo(Span.Kind.SERVER);
    assertThat(root.status().code()).isEqualTo(Span.StatusCode.OK);
    assertThat(root.serviceName()).isEqualTo("sdk-a-6moiazbq");
    assertThat(root.durationMicros()).isEqualTo(50000);
    assertThat(root.attributes())
        .containsEntry("http_method", "GET")
        .containsEntry("service_service_version", "1.2.3")
        .doesNotContainKeys("_timestamp", "flags", "events", "duration");
    Span db = trace.spansNamed("db.query").get(0);
    assertThat(db.parentSpanId()).contains("b7ad6b7169203331");
    assertThat(db.kind()).isEqualTo(Span.Kind.CLIENT);
    assertThat(db.status().code()).isEqualTo(Span.StatusCode.ERROR);
    assertThat(db.status().message()).contains("boom");
    assertThat(db.attributes())
        .containsKey("db_system")
        .doesNotContainKey("infer_service_name")
        .doesNotContainKey("o2_db_env");
    assertThat(db.events()).hasSize(1);
    assertThat(db.events().get(0).name()).isEqualTo("cache.miss");
    assertThat(db.events().get(0).attributes()).containsEntry("key", "u1");
    assertThat(db.events().get(0).timeMicros()).isEqualTo(1795033200623151L);
    assertThat(db.links()).hasSize(1);
    assertThat(db.links().get(0).spanId()).isEqualTo("00f067aa0ba902b8");
    assertThat(db.links().get(0).attributes()).containsEntry("kind", "follows");
    Span decode = trace.spansNamed("row.decode").get(0);
    assertThat(decode.kind()).isEqualTo(Span.Kind.INTERNAL);
    assertThat(decode.status().code()).isEqualTo(Span.StatusCode.UNSET);
    assertThat(server.requests().get(0).uri())
        .startsWith("/api/default/sdk_traces/traces/" + ID + "/details?")
        .contains("start_time=")
        .contains("end_time=");
  }

  @Test
  void getTraceFollowsPagesAndReturnsEmpty() {
    Fixtures.enqueue(server, "getTrace.page1");
    Fixtures.enqueue(server, "getTrace.details");
    Fixtures.enqueue(server, "getTrace.not_found");
    Fixtures.enqueue(server, "getTrace.400_stream_not_found");
    Trace t = traces.getTraceAsync(ID).join().orElseThrow();
    assertThat(t.spans()).hasSize(4);
    assertThat(server.requests().get(1).uri())
        .contains("after_start_time=1795033200603151382")
        .contains("after_span_id=b7ad6b7169203331");
    assertThat(traces.getTrace(ID)).isEmpty();
    assertThatThrownBy(() -> traces.getTrace(ID)).isInstanceOf(NotFoundException.class);
    assertThatThrownBy(() -> traces.getTrace("xyz")).isInstanceOf(ValidationException.class);
  }

  @Test
  void waitForTrace() {
    Fixtures.enqueue(server, "getTrace.not_found");
    Fixtures.enqueue(server, "getTrace.details");
    Trace t =
        traces.waitForTrace(
            ID, new WaitForTraceOptions(Duration.ofSeconds(5), Duration.ofMillis(10), 3));
    assertThat(t.spans()).hasSize(3);
  }

  @Test
  void spanFromRowEdgeCases() {
    Map<String, Object> row = new HashMap<>();
    row.put("trace_id", ID);
    row.put("span_id", "00f067aa0ba902b7");
    row.put("operation_name", "op");
    row.put("span_kind", "SPAN_KIND_PRODUCER");
    row.put("start_time", "1795033200603151382");
    row.put("end_time", "not-a-number");
    row.put("events", "not json");
    row.put("links", "[{\"context\":{\"traceId\":\"zz\",\"spanId\":\"1\"}}]");
    row.put("reference_parent_span_id", "");
    row.put("ok", true);
    Span s = OpenObserveTraceQuery.spanFromRow(row);
    assertThat(s.kind()).isEqualTo(Span.Kind.PRODUCER);
    assertThat(s.startMicros()).isEqualTo(1795033200603151L);
    assertThat(s.endMicros()).isZero();
    assertThat(s.events()).isEmpty();
    assertThat(s.links()).isEmpty();
    assertThat(s.parentSpanId()).isEmpty();
    assertThat(s.attributes()).containsEntry("ok", true);
    row.put("events", "{\"not\":\"array\"}");
    row.put("links", "");
    row.put("span_kind", "5");
    row.put("start_time", 1.5);
    Span s2 = OpenObserveTraceQuery.spanFromRow(row);
    assertThat(s2.events()).isEmpty();
    assertThat(s2.kind()).isEqualTo(Span.Kind.CONSUMER);
    assertThat(s2.startMicros()).isZero();
  }

  @Test
  void searchTracesBuildsFilter() {
    Fixtures.enqueue(server, "searchTraces.latest");
    Fixtures.enqueue(server, "searchTraces.filter_no_match");
    Fixtures.enqueue(server, "searchTraces.filter_service");
    List<TraceSummary> all = traces.searchTraces(TraceSearchQuery.builder().limit(10).build());
    assertThat(all).hasSize(2);
    TraceSummary second = all.get(1);
    assertThat(second.traceId()).isEqualTo(ID);
    assertThat(second.rootServiceName()).contains("sdk-a-6moiazbq");
    assertThat(second.rootName()).contains("GET /checkout");
    assertThat(second.startMicros()).isEqualTo(1795033200603151L);
    assertThat(second.durationMicros()).isEqualTo(50000);
    assertThat(second.spanCount()).isEqualTo(3);
    String uri = server.requests().get(0).uri();
    assertThat(uri)
        .startsWith("/api/default/sdk_traces/traces/latest?")
        .contains("size=10")
        .contains("from=0")
        .doesNotContain("filter=");
    Instant end = Instant.parse("2026-11-18T21:00:00Z");
    List<TraceSummary> none =
        traces
            .searchTracesAsync(
                TraceSearchQuery.builder()
                    .service("it's")
                    .operation("op")
                    .tag("http.method", "GET")
                    .tag("..", "ignored-key-is-kept")
                    .minDurationMicros(40000L)
                    .maxDurationMicros(90000L)
                    .start(end.minusSeconds(60))
                    .end(end)
                    .build())
            .join();
    assertThat(none).isEmpty();
    String q = java.net.URLDecoder.decode(server.requests().get(1).uri(), StandardCharsets.UTF_8);
    assertThat(q)
        .contains(
            "filter=service_name = 'it''s' AND operation_name = 'op' AND duration >= 40000 AND"
                + " duration <= 90000 AND \"__\" = 'ignored-key-is-kept' AND \"http_method\" = 'GET'")
        .contains("start_time=1795035540000000")
        .contains("end_time=1795035600000000");
    assertThat(traces.searchTraces(null)).isNotEmpty();
    assertThatThrownBy(
            () ->
                traces.searchTraces(
                    TraceSearchQuery.builder().start(end).end(end.minusSeconds(1)).build()))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void summaryWithoutFirstEvent() {
    server.enqueue(200, "{\"hits\":[{\"trace_id\":\"" + ID.toUpperCase() + "\",\"spans\":[]}]}");
    TraceSummary s = traces.searchTraces(TraceSearchQuery.builder().build()).get(0);
    assertThat(s.traceId()).isEqualTo(ID);
    assertThat(s.rootServiceName()).isEmpty();
    assertThat(s.rootName()).isEmpty();
    assertThat(s.spanCount()).isZero();
  }

  @Test
  void servicesAndOperations() {
    Fixtures.enqueue(server, "listServices.ok");
    Fixtures.enqueue(server, "listOperations.ok");
    server.enqueue(200, "{\"hits\":[{\"service_name\":\"\"},{\"other\":1}]}");
    assertThat(traces.listServices()).containsExactly("sdk-a-6moiazbq", "sdk-b-6moiazbq");
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/_search?type=traces");
    String sql =
        Json.tree(server.requests().get(0).body().getBytes(StandardCharsets.UTF_8))
            .get("query")
            .get("sql")
            .asText();
    assertThat(sql)
        .isEqualTo("SELECT DISTINCT service_name FROM \"sdk_traces\" ORDER BY service_name");
    assertThat(traces.listOperations("sdk-a"))
        .containsExactly("GET /checkout", "db.query", "row.decode");
    String sql2 =
        Json.tree(server.requests().get(1).body().getBytes(StandardCharsets.UTF_8))
            .get("query")
            .get("sql")
            .asText();
    assertThat(sql2).contains("WHERE service_name = 'sdk-a'");
    assertThat(traces.listServicesAsync().join()).isEmpty();
    assertThatThrownBy(() -> traces.listOperations("")).isInstanceOf(ValidationException.class);
    assertThatThrownBy(() -> traces.listOperations(null)).isInstanceOf(ValidationException.class);
  }

  @Test
  void dependenciesUnsupported() {
    assertThatThrownBy(() -> traces.getDependencies(null, null))
        .isInstanceOf(UnsupportedException.class)
        .hasMessageContaining("enterprise");
    assertThatThrownBy(() -> traces.getDependenciesAsync(null, null).join())
        .hasCauseInstanceOf(UnsupportedException.class);
  }

  @Test
  void traceTimeRange() {
    Fixtures.enqueue(server, "traceTimeRange.ok");
    Fixtures.enqueue(server, "traceTimeRange.not_found");
    TraceTimeRange r = traces.traceTimeRange(ID);
    assertThat(r.found()).isTrue();
    assertThat(r.stream()).contains("sdk_traces_6moiazbq");
    assertThat(r.start()).contains(Instant.parse("2026-11-18T20:20:00.603151Z"));
    assertThat(r.end()).isPresent();
    assertThat(server.requests().get(0).uri())
        .isEqualTo("/api/default/traces/time_range?trace_id=" + ID);
    TraceTimeRange missing = traces.traceTimeRangeAsync("ffffffffffffffffffffffffffffffff").join();
    assertThat(missing.found()).isFalse();
    assertThat(missing.stream()).isEmpty();
    assertThat(missing.start()).isEmpty();
    assertThat(missing.end()).isEmpty();
    assertThatThrownBy(() -> traces.traceTimeRange("")).isInstanceOf(ValidationException.class);
  }
}
