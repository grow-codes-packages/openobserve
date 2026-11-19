package codes.grow.packages.openobserve.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.Fixtures;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.logging.ExportOptions;
import codes.grow.packages.openobserve.logging.ExportResult;
import codes.grow.packages.openobserve.logging.LogRecord;
import codes.grow.packages.openobserve.logging.Severity;
import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExportersTest {
  private static final ExportOptions PLAIN = new ExportOptions(false, Duration.ofSeconds(5));
  private static final ExportOptions GZIP = new ExportOptions(true, Duration.ofSeconds(5));

  private MockServer server;
  private OpenObserveClient client;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
    client = Fixtures.client(server).build();
  }

  @AfterEach
  void stop() {
    client.close();
    server.close();
  }

  private static LogRecord record() {
    return LogRecord.builder()
        .timestamp(Instant.parse("2026-11-18T20:00:00.123456789Z"))
        .observedTimestamp(Instant.parse("2026-11-18T20:00:01Z"))
        .severity(Severity.WARN)
        .body("hello")
        .attribute("n", 1)
        .attribute("f", 1.5)
        .attribute("nan", Double.NaN)
        .attribute("ok", true)
        .attribute("list", List.of("a", 2))
        .attribute("map", Map.of("k", "v"))
        .resource(Map.of("service.name", "svc", "deployment.environment", "test"))
        .traceId("4bf92f3577b34da6a3ce929d0e0e4736")
        .spanId("00f067aa0ba902b7")
        .traceFlags(1)
        .build();
  }

  private JsonNode body(int i) {
    return Json.tree(server.requests().get(i).body().getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void jsonExporterRows() {
    Fixtures.enqueue(server, "ingestJson.ok");
    JsonLogExporter e = (JsonLogExporter) client.logExporter("Logs-A", false);
    assertThat(e.stream()).isEqualTo("logs_a");
    ExportResult r = e.export(List.of(record()), PLAIN).join();
    assertThat(r.accepted()).isEqualTo(3);
    assertThat(r.rejected()).isZero();
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/logs_a/_json");
    JsonNode row = body(0).get(0);
    assertThat(row.get("_timestamp").asLong()).isEqualTo(1795032000123456L);
    assertThat(row.get("body").asText()).isEqualTo("hello");
    assertThat(row.get("severity").asText()).isEqualTo("WARN");
    assertThat(row.get("severity_number").asInt()).isEqualTo(13);
    assertThat(row.get("trace_id").asText()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    assertThat(row.get("span_id").asText()).isEqualTo("00f067aa0ba902b7");
    assertThat(row.get("trace_flags").asInt()).isEqualTo(1);
    assertThat(row.get("service_name").asText()).isEqualTo("svc");
    assertThat(row.get("deployment_environment").asText()).isEqualTo("test");
    assertThat(e.shutdown().join()).isNull();
  }

  @Test
  void jsonExporterGzipAndPartialFailure() {
    Fixtures.enqueue(server, "ingestJson.200_partial_failure");
    server.enqueue(
        200, "{\"code\":200,\"status\":[{\"name\":\"s\",\"successful\":0,\"failed\":1}]}");
    Fixtures.enqueue(server, "ingestJson.401");
    LogRecord minimal = LogRecord.builder().body("x").build();
    JsonLogExporter e = new JsonLogExporter(clientApi(), "s");
    ExportResult r = e.export(List.of(minimal, minimal), GZIP).join();
    assertThat(server.requests().get(0).header("Content-Encoding")).isEqualTo("gzip");
    assertThat(r.rejected()).isEqualTo(1);
    assertThat(r.error())
        .hasValueSatisfying(x -> assertThat(x).isInstanceOf(ValidationException.class));
    assertThat(r.error().get().getMessage())
        .contains("1 of 2 records rejected")
        .contains("Too old");
    ExportResult noReason = e.export(List.of(minimal), PLAIN).join();
    assertThat(noReason.error().get().getMessage()).contains("records rejected");
    assertThatThrownBy(() -> e.export(List.of(minimal), PLAIN).join())
        .hasCauseInstanceOf(AuthenticationException.class);
  }

  private codes.grow.packages.openobserve.internal.Api clientApi() {
    return new codes.grow.packages.openobserve.internal.Api(client.config(), client.transport());
  }

  @Test
  void otlpExporter() {
    server.enqueue(200, "", "Content-Type", "application/x-protobuf");
    server.enqueue(200, "{}");
    server.enqueue(
        200, "{\"partialSuccess\":{\"rejectedLogRecords\":\"5\",\"errorMessage\":\"too old\"}}");
    Fixtures.enqueue(server, "otlpLogs.401");
    OtlpLogExporter e = (OtlpLogExporter) client.logExporter("Otlp", true);
    assertThat(e.stream()).isEqualTo("otlp");
    LogRecord other = record().toBuilder().resource(Map.of("service.name", "b")).build();
    LogRecord bare = LogRecord.builder().body("bare").build();
    ExportResult r = e.export(List.of(record(), other, bare), PLAIN).join();
    assertThat(r.accepted()).isEqualTo(3);
    MockServer.Recorded req = server.requests().get(0);
    assertThat(req.uri()).isEqualTo("/api/default/v1/logs");
    assertThat(req.header("stream-name")).isEqualTo("otlp");
    JsonNode rl = body(0).get("resourceLogs");
    assertThat(rl).hasSize(3);
    JsonNode lr = rl.get(0).get("scopeLogs").get(0).get("logRecords").get(0);
    assertThat(lr.get("timeUnixNano").asText()).isEqualTo("1795032000123456789");
    assertThat(lr.get("observedTimeUnixNano").asText()).isEqualTo("1795032001000000000");
    assertThat(lr.get("severityNumber").asInt()).isEqualTo(13);
    assertThat(lr.get("traceId").asText()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
    assertThat(lr.get("flags").asInt()).isEqualTo(1);
    String attrs = lr.get("attributes").toString();
    assertThat(attrs)
        .contains("{\"key\":\"n\",\"value\":{\"intValue\":\"1\"}}")
        .contains("{\"doubleValue\":1.5}")
        .contains("{\"stringValue\":\"NaN\"}")
        .contains("{\"boolValue\":true}")
        .contains("arrayValue")
        .contains("kvlistValue");
    assertThat(e.export(List.of(bare), GZIP).join().accepted()).isEqualTo(1);
    assertThat(server.requests().get(1).header("Content-Encoding")).isEqualTo("gzip");
    ExportResult partial = e.export(List.of(bare, bare), PLAIN).join();
    assertThat(partial.rejected()).isEqualTo(2);
    assertThat(partial.accepted()).isZero();
    assertThat(partial.error().get().getMessage()).contains("too old");
    assertThatThrownBy(() -> e.export(List.of(bare), PLAIN).join())
        .hasCauseInstanceOf(AuthenticationException.class);
    assertThat(e.shutdown().join()).isNull();
  }
}
