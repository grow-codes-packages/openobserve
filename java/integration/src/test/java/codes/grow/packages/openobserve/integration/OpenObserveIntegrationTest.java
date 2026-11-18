package codes.grow.packages.openobserve.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import codes.grow.packages.openobserve.Admin;
import codes.grow.packages.openobserve.LogClientOptions;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.OpenObserveConfig;
import codes.grow.packages.openobserve.Organization;
import codes.grow.packages.openobserve.Passcode;
import codes.grow.packages.openobserve.ServiceAccount;
import codes.grow.packages.openobserve.ServiceAccountToken;
import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.ConflictException;
import codes.grow.packages.openobserve.core.EnvReader;
import codes.grow.packages.openobserve.core.HealthStatus;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.UnsupportedException;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.logging.BatchOptions;
import codes.grow.packages.openobserve.logging.BatchingLogClient;
import codes.grow.packages.openobserve.logging.LogInput;
import codes.grow.packages.openobserve.logging.LogRecord;
import codes.grow.packages.openobserve.logging.LogSearchResult;
import codes.grow.packages.openobserve.logging.SearchOptions;
import codes.grow.packages.openobserve.logging.Severity;
import codes.grow.packages.openobserve.logging.TailHandle;
import codes.grow.packages.openobserve.logging.TailOptions;
import codes.grow.packages.openobserve.logging.WaitForLogOptions;
import codes.grow.packages.openobserve.logs.BulkAction;
import codes.grow.packages.openobserve.logs.BulkResult;
import codes.grow.packages.openobserve.logs.CreateStreamOptions;
import codes.grow.packages.openobserve.logs.FieldValues;
import codes.grow.packages.openobserve.logs.HistogramOptions;
import codes.grow.packages.openobserve.logs.HistogramResult;
import codes.grow.packages.openobserve.logs.IngestResult;
import codes.grow.packages.openobserve.logs.Logs;
import codes.grow.packages.openobserve.logs.OpenObserveLogQuery;
import codes.grow.packages.openobserve.logs.PartitionKey;
import codes.grow.packages.openobserve.logs.SchemaField;
import codes.grow.packages.openobserve.logs.SqlSearchOptions;
import codes.grow.packages.openobserve.logs.SqlSearchResult;
import codes.grow.packages.openobserve.logs.StreamInfo;
import codes.grow.packages.openobserve.logs.StreamList;
import codes.grow.packages.openobserve.logs.StreamSettings;
import codes.grow.packages.openobserve.logs.StreamSettingsUpdate;
import codes.grow.packages.openobserve.logs.StreamType;
import codes.grow.packages.openobserve.logs.Streams;
import codes.grow.packages.openobserve.logs.ValuesOptions;
import codes.grow.packages.openobserve.traces.OpenObserveTraceQuery;
import codes.grow.packages.openobserve.traces.TraceTimeRange;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import codes.grow.packages.openobserve.tracing.query.Span;
import codes.grow.packages.openobserve.tracing.query.Trace;
import codes.grow.packages.openobserve.tracing.query.TraceSearchQuery;
import codes.grow.packages.openobserve.tracing.query.TraceSummary;
import codes.grow.packages.openobserve.tracing.query.WaitForTraceOptions;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Calls every public function of the Java package against the real OpenObserve from {@code
 * docker/compose.yml}, each with a round-trip assertion (write, then read back).
 */
class OpenObserveIntegrationTest {
  private static final Duration WAIT = Duration.ofSeconds(30);

  private static Map<String, String> env;
  private static OpenObserveClient client;
  private static String run;

  @BeforeAll
  static void setUp() throws Exception {
    env = ConformanceTest.environment();
    assumeTrue(
        env.containsKey(EnvReader.DEFAULT_PREFIX + "URL"),
        "GROW_OPENOBSERVE_URL is not set; use docker/compose.yml");
    client = OpenObserveClient.builder().fromEnv(EnvReader.of(env)).build();
    run = UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
  }

  @AfterAll
  static void tearDown() {
    if (client != null) {
      client.close();
    }
  }

  private static long micros(Instant t) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, t);
  }

  @Test
  void coreHealthPingVersionClose() {
    HealthStatus h = client.health();
    assertTrue(h.healthy());
    assertEquals("ok", client.healthAsync().join().status());
    assertFalse(client.ping().isNegative());
    assertFalse(client.pingAsync().join().isNegative());
    String version = client.serverVersion();
    assertTrue(version.startsWith("v"), version);
    assertEquals(version, client.serverVersionAsync().join());
    String expected = env.get("OPENOBSERVE_SERVER_VERSION");
    if (expected != null) {
      assertEquals(expected, version);
    }
    OpenObserveClient other = OpenObserveClient.builder().fromEnv(EnvReader.of(env)).build();
    other.close();
    other.close();
    assertTrue(other.isClosed());
    OpenObserveClient bad =
        OpenObserveClient.builder()
            .fromEnv(EnvReader.of(Map.of()))
            .url(client.config().url())
            .user("nobody@example.com")
            .password("wrong-password")
            .build();
    assertThrows(AuthenticationException.class, bad::ping);
    bad.close();
  }

  @Test
  void ingestAndSearch() {
    Logs logs = client.logs();
    String stream = "sdk_java_" + run;
    Instant now = Instant.now();
    List<Map<String, Object>> records = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      records.add(
          Map.of(
              "_timestamp",
              micros(now.plusMillis(i)),
              "message",
              "json needle" + run + " #" + i,
              "level",
              i == 0 ? "error" : "info",
              "run_id",
              run,
              "user",
              Map.of("id", i)));
    }
    IngestResult json = logs.ingestJson(stream, records);
    assertEquals(3, json.successful());
    assertEquals(0, json.failed());
    assertEquals(stream, json.streams().get(0).name());

    IngestResult multi =
        logs.ingestMultiAsync(
                stream,
                "{\"message\":\"multi needle"
                    + run
                    + " a\",\"run_id\":\""
                    + run
                    + "\",\"level\":\"debug\"}\n{\"message\":\"multi b\",\"run_id\":\""
                    + run
                    + "\",\"level\":\"debug\"}\n")
            .join();
    assertEquals(2, multi.successful());
    assertThrows(ValidationException.class, () -> logs.ingestMulti(stream, "{bad\n"));

    BulkResult bulk =
        logs.ingestBulk(
            List.of(
                BulkAction.index(stream, Map.of("message", "bulk index " + run, "run_id", run)),
                BulkAction.create(stream, Map.of("message", "bulk create " + run, "run_id", run))));
    assertFalse(bulk.errors());
    assertEquals(2, bulk.items().size());
    assertEquals(200, bulk.items().get(0).status());
    assertThrows(
        ValidationException.class,
        () -> logs.ingestBulk(List.of(new BulkAction("delete", stream, Map.of()))));

    String where = " WHERE run_id = '" + run + "'";
    SqlSearchResult all =
        logs.search(
            "SELECT * FROM \"" + stream + "\"" + where + " ORDER BY _timestamp DESC",
            SqlSearchOptions.builder()
                .start(now.minusSeconds(600))
                .end(Instant.now().plusSeconds(60))
                .size(100)
                .trackTotalHits(true)
                .build());
    assertEquals(7, all.hits().size());
    assertEquals(7L, all.total().orElseThrow());
    assertTrue(all.searchId().isPresent());
    Map<String, Object> flat =
        all.hits().stream()
            .filter(r -> String.valueOf(r.get("message")).endsWith("#1"))
            .findFirst()
            .orElseThrow();
    assertEquals(1L, flat.get("user_id"));
    assertEquals(
        2,
        logs.searchAsync(
                "SELECT * FROM \"" + stream + "\"" + where,
                SqlSearchOptions.builder().from(0).size(2).build())
            .join()
            .hits()
            .size());
    assertThrows(NotFoundException.class, () -> logs.search("SELECT * FROM nosuch_" + run));

    SqlSearchResult around = logs.searchAround(stream, now.plusMillis(1), 3);
    assertFalse(around.hits().isEmpty());
    assertTrue(around.hits().size() <= 3);

    List<FieldValues> values =
        logs.valuesOf(
            stream,
            List.of("level", "run_id"),
            ValuesOptions.of(now.minusSeconds(600), Instant.now().plusSeconds(60), 10));
    FieldValues level =
        values.stream().filter(v -> v.field().equals("level")).findFirst().orElseThrow();
    assertTrue(
        level.values().stream().anyMatch(v -> v.value().equals("error") && v.count() == 1),
        level.toString());

    HistogramResult hist =
        logs.histogram(
            stream,
            HistogramOptions.of(
                now.minusSeconds(600),
                Instant.now().plusSeconds(60),
                Duration.ofMinutes(1),
                "run_id = '" + run + "'"));
    assertEquals(Duration.ofSeconds(60), hist.interval());
    assertEquals(7L, hist.buckets().stream().mapToLong(b -> b.count()).sum());
  }

  @Test
  void streamsLifecycle() {
    Streams streams = client.streams();
    String stream = "sdk_java_streams_" + run;
    streams.createStream(
        stream,
        new CreateStreamOptions(
            StreamType.LOGS,
            List.of(new SchemaField("message", "Utf8")),
            new StreamSettings(null, null, null, null, null, 3, 0)));
    client.logs().ingestJson(stream, List.of(Map.of("message", "s", "run_id", run)));
    StreamInfo info = streams.getStreamSchema(stream);
    assertEquals(stream, info.name());
    assertTrue(info.schema().stream().anyMatch(f -> f.name().equals("run_id")));
    assertEquals(3, info.settings().retentionDays());

    streams.updateStreamSettings(
        stream,
        StreamSettingsUpdate.builder()
            .retentionDays(7)
            .fullTextSearchKeys(List.of("message"))
            .partitionKeys(List.of(PartitionKey.of("run_id")))
            .build());
    StreamInfo updated = streams.getStreamSchemaAsync(stream, StreamType.LOGS).join();
    assertEquals(7, updated.settings().retentionDays());
    assertEquals(List.of("message"), updated.settings().fullTextSearchKeys());
    assertTrue(
        updated.settings().partitionKeys().stream()
            .anyMatch(k -> k.field().equals("run_id") && !k.disabled()));
    streams.updateStreamSettings(
        stream,
        StreamSettingsUpdate.builder()
            .fullTextSearchKeys(List.of())
            .partitionKeys(List.of())
            .build());
    StreamInfo removed = streams.getStreamSchema(stream);
    assertTrue(removed.settings().fullTextSearchKeys().isEmpty());
    assertTrue(removed.settings().partitionKeys().stream().allMatch(PartitionKey::disabled));

    StreamList list =
        streams.listStreams(
            new codes.grow.packages.openobserve.logs.ListStreamsOptions(
                StreamType.LOGS, run, 0, 0));
    assertTrue(list.streams().stream().anyMatch(s -> s.name().equals(stream)));
    assertTrue(
        streams.listStreams().streams().stream().anyMatch(s -> s.name().equals(stream)));

    streams.deleteStream(stream);
    assertThrows(NotFoundException.class, () -> streams.getStreamSchema(stream));
    CompletionException second =
        assertThrows(
            CompletionException.class,
            () -> streams.deleteStreamAsync(stream, StreamType.LOGS).join(),
            "second delete");
    assertInstanceOf(NotFoundException.class, second.getCause());
  }

  @Test
  void logClientAndQueryClient() throws Exception {
    String stream = "sdk_java_lc_" + run;
    OpenObserveLogQuery query = client.logQuery(stream);
    assertEquals(stream, query.stream());

    // tail started first so it sees the new records
    List<LogRecord> tailed = new CopyOnWriteArrayList<>();
    TailHandle tail =
        query.tail(
            "tail" + run,
            tailed::add,
            new TailOptions(Optional.of(Instant.now().minusSeconds(1)), Duration.ofMillis(250), null));

    BatchingLogClient lc =
        client.logClient(
            LogClientOptions.builder()
                .stream(stream)
                .batchOptions(BatchOptions.builder().flushInterval(Duration.ofMillis(100)).build())
                .resource(Map.of("service.name", "sdk-java", "deployment.environment", "test"))
                .build());
    lc.send(
        LogInput.builder("lc hello " + run)
            .severity(Severity.WARN)
            .attribute("run", run)
            .attribute("password", "hunter2-" + run)
            .traceId("4bf92f3577b34da6a3ce929d0e0e4736")
            .spanId("00f067aa0ba902b7")
            .build());
    lc.logger("app", Map.of("component", "it")).info("tail" + run + " via logger " + run);
    lc.flush(Duration.ofSeconds(10));
    assertEquals(0, lc.stats().failed());

    LogRecord found =
        query.waitForLog("lc hello " + run, WaitForLogOptions.of(WAIT, Duration.ofMillis(250)));
    assertEquals(Severity.WARN, found.severity());
    assertEquals(Optional.of("4bf92f3577b34da6a3ce929d0e0e4736"), found.traceId());
    assertEquals(Optional.of("00f067aa0ba902b7"), found.spanId());
    assertEquals("sdk-java", found.resource().get("service.name"));
    assertEquals(run, found.attributes().get("run"));
    assertFalse(found.toString().contains("hunter2-" + run));

    LogSearchResult page = query.search(run, SearchOptions.builder().limit(1).build());
    assertEquals(1, page.records().size());
    assertTrue(page.nextCursor().isPresent());
    LogSearchResult next =
        query.searchAsync(
                run, SearchOptions.builder().limit(1).cursor(page.nextCursor().get()).build())
            .join();
    assertEquals(1, next.records().size());
    assertEquals(
        1,
        query
            .search("SELECT * FROM \"" + stream + "\" WHERE run = '" + run + "'")
            .records()
            .size());

    long deadline = System.nanoTime() + WAIT.toNanos();
    while (tailed.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(100);
    }
    tail.stop();
    assertTrue(tailed.stream().anyMatch(r -> r.body().contains("via logger")), tailed.toString());

    assertTrue(query.listStreams().contains(stream));
    assertTrue(query.listLabels().contains("run"));

    // OTLP exporter
    String otlpStream = "sdk_java_otlp_" + run;
    BatchingLogClient otlp =
        client.logClient(
            LogClientOptions.builder()
                .stream(otlpStream)
                .protocol(LogClientOptions.Protocol.OTLP)
                .resource(Map.of("service.name", "sdk-java-otlp"))
                .build());
    otlp.send(LogInput.of(Severity.ERROR, "otlp needle " + run, Map.of("run", run)));
    otlp.flush(Duration.ofSeconds(10));
    LogRecord viaOtlp =
        client
            .logQuery(otlpStream)
            .waitForLog("otlp needle " + run, WaitForLogOptions.of(WAIT, Duration.ofMillis(250)));
    assertEquals(Severity.ERROR, viaOtlp.severity());
    assertEquals("sdk-java-otlp", viaOtlp.resource().get("service.name"));
    otlp.close();
    lc.close();
  }

  @Test
  void tracingRoundTrip() {
    String service = "sdk-java-svc-" + run;
    TracingHandle handle =
        client.initTracing(
            TracingConfig.builder(service)
                .serviceVersion("1.2.3")
                .environment("test")
                .setGlobal(false)
                .build());
    Tracer tracer = handle.tracer("it");
    io.opentelemetry.api.trace.Span root =
        tracer.spanBuilder("GET /checkout").setSpanKind(SpanKind.SERVER).startSpan();
    String traceId = root.getSpanContext().getTraceId();
    io.opentelemetry.api.trace.Span child =
        tracer
            .spanBuilder("db.query")
            .setParent(Context.current().with(root))
            .setSpanKind(SpanKind.CLIENT)
            .setAttribute("db.system", "postgresql")
            .startSpan();
    child.addEvent("cache.miss");
    child.end();
    root.end();
    handle.forceFlush(Duration.ofSeconds(10));

    OpenObserveTraceQuery traces = client.traceQuery();
    Trace trace =
        traces.waitForTrace(traceId, new WaitForTraceOptions(WAIT, Duration.ofMillis(250), 2));
    assertEquals(2, trace.spans().size());
    Span db = trace.spansNamed("db.query").get(0);
    assertEquals(Span.Kind.CLIENT, db.kind());
    assertEquals("postgresql", db.attributes().get("db_system"));
    assertEquals("cache.miss", db.events().get(0).name());
    Span rootSpan = trace.root().orElseThrow();
    assertEquals(rootSpan.spanId(), db.parentSpanId().orElseThrow());
    assertEquals(service, rootSpan.serviceName());
    assertTrue(traces.getTrace(traceId).isPresent());
    assertTrue(traces.getTraceAsync("ffffffffffffffffffffffffffffffff").join().isEmpty());

    List<TraceSummary> summaries =
        traces.searchTraces(TraceSearchQuery.builder().service(service).limit(10).build());
    assertTrue(summaries.stream().anyMatch(s -> s.traceId().equals(traceId)), summaries.toString());
    List<TraceSummary> byTag =
        traces.searchTraces(
            TraceSearchQuery.builder().service(service).tag("db.system", "postgresql").build());
    assertTrue(byTag.stream().anyMatch(s -> s.traceId().equals(traceId)));
    assertTrue(
        traces
            .searchTraces(TraceSearchQuery.builder().service(service).minDurationMicros(3_600_000_000L).build())
            .isEmpty());

    assertTrue(traces.listServices().contains(service));
    assertTrue(traces.listOperations(service).containsAll(List.of("GET /checkout", "db.query")));
    assertThrows(UnsupportedException.class, () -> traces.getDependencies(null, null));

    TraceTimeRange range = traces.traceTimeRange(traceId);
    assertTrue(range.found());
    assertFalse(traces.traceTimeRange("ffffffffffffffffffffffffffffffff").found());
    handle.shutdown();
  }

  @Test
  void adminHelpers() {
    Admin admin = client.admin();
    List<Organization> orgs = admin.listOrganizations();
    assertTrue(orgs.stream().anyMatch(o -> o.identifier().equals(client.config().org())));

    String email = "sdk-java-" + run + "@stack.local";
    ServiceAccountToken created = admin.createServiceAccount(email, "SDK", "Java");
    assertEquals(email, created.email());
    assertThrows(ConflictException.class, () -> admin.createServiceAccount(email));
    List<ServiceAccount> accounts = admin.listServiceAccounts();
    assertTrue(accounts.stream().anyMatch(a -> a.email().equals(email)));

    // the new token authenticates as user + token
    OpenObserveConfig saConfig =
        OpenObserveConfig.builder()
            .url(client.config().url())
            .org(client.config().org())
            .user(email)
            .token(created.token())
            .build();
    try (OpenObserveClient sa = OpenObserveClient.create(saConfig)) {
      assertFalse(sa.ping().isNegative());
    }

    ServiceAccountToken rotated = admin.rotateServiceAccountToken(email);
    assertFalse(rotated.token().reveal().equals(created.token().reveal()));
    try (OpenObserveClient stale = OpenObserveClient.create(saConfig)) {
      assertThrows(AuthenticationException.class, stale::ping);
    }

    admin.deleteServiceAccount(email);
    assertThrows(NotFoundException.class, () -> admin.deleteServiceAccount(email));

    Passcode passcode = admin.getPasscode();
    assertFalse(passcode.passcode().reveal().isEmpty());
    // the passcode may ingest (ingest-only credential)
    OpenObserveConfig ingestOnly =
        OpenObserveConfig.builder()
            .url(client.config().url())
            .org(client.config().org())
            .user(passcode.user())
            .password(passcode.passcode())
            .build();
    try (OpenObserveClient ing = OpenObserveClient.create(ingestOnly)) {
      IngestResult r =
          ing.logs().ingestJson("sdk_java_passcode_" + run, List.of(Map.of("message", "p")));
      assertEquals(1, r.successful());
    }
    assertInstanceOf(String.class, passcode.user());
  }
}
