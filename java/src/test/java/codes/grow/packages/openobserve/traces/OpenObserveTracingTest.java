package codes.grow.packages.openobserve.traces;

import static org.assertj.core.api.Assertions.assertThat;

import codes.grow.packages.openobserve.OpenObserveConfig;
import codes.grow.packages.openobserve.core.TlsOptions;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.junit.jupiter.api.Test;

class OpenObserveTracingTest {
  private static OpenObserveConfig cfg() {
    return OpenObserveConfig.builder()
        .url("https://oo.example.com:5080/")
        .org("acme")
        .user("u@example.com")
        .password("pw-1234")
        .tracesStream("My.Traces")
        .build();
  }

  @Test
  void httpDefaults() {
    TracingConfig tc = OpenObserveTracing.configure(cfg(), TracingConfig.builder("svc").build());
    assertThat(tc.endpoint()).contains("https://oo.example.com:5080/api/acme/v1/traces");
    assertThat(tc.headers()).containsKey("Authorization").containsEntry("stream-name", "my_traces");
    assertThat(tc.headers()).doesNotContainKey("organization");
    assertThat(tc.tls()).isPresent();
    assertThat(tc.toString()).doesNotContain("pw-1234");
  }

  @Test
  void grpcDefaultsAndOverrides() {
    TracingConfig tc =
        OpenObserveTracing.configure(
            cfg(),
            TracingConfig.builder("svc")
                .protocol(TracingConfig.Protocol.GRPC)
                .header("authorization", "Basic custom")
                .tls(TlsOptions.defaults())
                .build());
    assertThat(tc.endpoint()).contains("https://oo.example.com:5081");
    assertThat(tc.headers())
        .containsEntry("authorization", "Basic custom")
        .doesNotContainKey("Authorization")
        .containsEntry("organization", "acme");
    TracingConfig explicit =
        OpenObserveTracing.configure(
            cfg(),
            TracingConfig.builder("svc").endpoint("http://collector:4318/v1/traces").build());
    assertThat(explicit.endpoint()).contains("http://collector:4318/v1/traces");
  }

  @Test
  void nativeProtocolBecomesOtlpHttpProtobuf() {
    TracingConfig tc =
        OpenObserveTracing.configure(
            cfg(), TracingConfig.builder("svc").protocol(TracingConfig.Protocol.NATIVE).build());
    assertThat(tc.protocol()).isEqualTo(TracingConfig.Protocol.HTTP_PROTOBUF);
    assertThat(tc.endpoint()).hasValueSatisfying(e -> assertThat(e).endsWith("/v1/traces"));
  }

  @Test
  void factoryCreatesExporters() {
    SpanExporter http =
        OpenObserveTracing.exporterFactory(cfg()).create(TracingConfig.builder("svc").build());
    SpanExporter grpc =
        OpenObserveTracing.exporterFactory(cfg())
            .create(TracingConfig.builder("svc").protocol(TracingConfig.Protocol.GRPC).build());
    assertThat(http).isNotNull();
    assertThat(grpc).isNotNull();
    http.shutdown();
    grpc.shutdown();
  }
}
