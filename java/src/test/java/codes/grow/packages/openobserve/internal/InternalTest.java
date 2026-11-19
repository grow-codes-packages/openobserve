package codes.grow.packages.openobserve.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.LogClientOptions;
import codes.grow.packages.openobserve.core.ConflictException;
import codes.grow.packages.openobserve.core.Json;
import codes.grow.packages.openobserve.core.NotFoundException;
import codes.grow.packages.openobserve.core.OpenObserveException;
import codes.grow.packages.openobserve.core.PermissionDeniedException;
import codes.grow.packages.openobserve.core.TransportResponse;
import codes.grow.packages.openobserve.core.ValidationException;
import codes.grow.packages.openobserve.logging.BatchOptions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InternalTest {
  private static TransportResponse resp(int status, String body, Map<String, List<String>> h) {
    return new TransportResponse(
        status, h, body.getBytes(StandardCharsets.UTF_8), 1, Duration.ZERO, "op");
  }

  private static OpenObserveException map(int status, String body) {
    return Api.mapError("op", resp(status, body, Map.of()), List.of("hidden-secret"));
  }

  @Test
  void mapErrorShapes() {
    OpenObserveException plain = map(400, "Stream not found: x\nsecond line");
    assertThat(plain).isInstanceOf(NotFoundException.class);
    assertThat(plain.getMessage()).isEqualTo("op: bad request: Stream not found: x (HTTP 400)");
    assertThat(map(400, "{\"code\":400,\"message\":\"Stream already exists\"}"))
        .isInstanceOf(ConflictException.class);
    OpenObserveException noMessage = map(403, "{\"code\":403,\"trace_id\":\"\"}");
    assertThat(noMessage).isInstanceOf(PermissionDeniedException.class);
    assertThat(noMessage.requestId()).isEmpty();
    assertThat(noMessage.getMessage()).isEqualTo("op: permission denied (HTTP 403)");
    assertThat(map(400, "[1,2]").getMessage()).isEqualTo("op: bad request (HTTP 400)");
    assertThat(map(400, "{broken").getMessage()).isEqualTo("op: bad request (HTTP 400)");
    assertThat(map(422, "token hidden-secret leaked").getMessage())
        .doesNotContain("hidden-secret")
        .contains("[REDACTED]");
    assertThat(map(404, "stream not found")).isInstanceOf(NotFoundException.class);
    OpenObserveException withHeaderId =
        Api.mapError(
            "op",
            resp(400, "{\"trace_id\":\"abc\"}", Map.of("x-request-id", List.of("rid"))),
            List.of());
    assertThat(withHeaderId.requestId()).contains("rid");
    assertThat(map(400, "{\"code\":20002,\"message\":\"x\"}"))
        .isInstanceOf(NotFoundException.class);
  }

  @Test
  void values() {
    assertThat(Values.normalizeKey(" Sdk-Mixed.X ")).isEqualTo("sdk_mixed_x");
    assertThat(Values.sqlIdent("a\"b")).isEqualTo("\"a\"\"b\"");
    assertThat(Values.isSql("SeLeCt 1")).isTrue();
    assertThat(Values.isSql("sel")).isFalse();
    assertThat(Values.isSql("selection of words")).isTrue();
    Instant t = Instant.parse("2026-11-18T20:00:00.000001999Z");
    assertThat(Values.fromMicros(Values.micros(t)))
        .isEqualTo(Instant.parse("2026-11-18T20:00:00.000001Z"));
    Instant[] w = Values.window("op", null, t, Duration.ofMinutes(1));
    assertThat(w[0]).isEqualTo(t.minusSeconds(60));
    assertThat(Values.asLong(null, 7)).isEqualTo(7);
    assertThat(Values.asLong(Json.mapper().valueToTree("12"), 0)).isEqualTo(12);
    assertThat(Values.asLong(Json.mapper().valueToTree("x"), 3)).isEqualTo(3);
    assertThat(Values.asLong(Json.mapper().valueToTree(true), 4)).isEqualTo(4);
    assertThat(Values.text(null)).isEmpty();
    assertThat(Values.text(Json.mapper().valueToTree(Map.of("a", 1)))).isEqualTo("{\"a\":1}");
    assertThat(Values.row(null)).isEmpty();
    assertThat(Values.row(Json.mapper().valueToTree(List.of()))).isEmpty();
    assertThat(Values.row(Json.tree("{\"a\":null,\"b\":1}".getBytes(StandardCharsets.UTF_8))))
        .containsExactly(Map.entry("b", 1L));
    assertThat(Values.strings(null)).isEmpty();
    assertThat(Values.gzip("x".getBytes(StandardCharsets.UTF_8))).isNotEmpty();
    assertThatThrownBy(() -> Values.stream("op", null)).isInstanceOf(ValidationException.class);
  }

  @Test
  void logClientOptions() {
    LogClientOptions o =
        LogClientOptions.builder().protocol(null).batchOptions(null).redaction(null).stream("s")
            .build();
    assertThat(o.protocol()).isEqualTo(LogClientOptions.Protocol.JSON);
    assertThat(o.batchOptions().maxBatchSize()).isEqualTo(BatchOptions.defaults().maxBatchSize());
    assertThat(o.redaction()).isNull();
    assertThat(o.stream()).contains("s");
    assertThat(LogClientOptions.defaults().stream()).isEmpty();
  }
}
