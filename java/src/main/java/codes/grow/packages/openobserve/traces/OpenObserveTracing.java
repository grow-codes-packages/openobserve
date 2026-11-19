package codes.grow.packages.openobserve.traces;

import codes.grow.packages.openobserve.OpenObserveConfig;
import codes.grow.packages.openobserve.core.ConfigException;
import codes.grow.packages.openobserve.internal.Values;
import codes.grow.packages.openobserve.tracing.OtlpExporters;
import codes.grow.packages.openobserve.tracing.SpanExporterFactory;
import codes.grow.packages.openobserve.tracing.Tracing;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

/**
 * OpenTelemetry tracing that exports to OpenObserve: OTLP/HTTP protobuf to {@code
 * <url>/api/<org>/v1/traces} (default) or OTLP/gRPC to port 5081 of the same host, with the Basic
 * {@code Authorization} header, the {@code stream-name} header (the configured traces stream) and,
 * for gRPC, the {@code organization} header. Values set in the {@link TracingConfig} (endpoint,
 * headers, TLS) win.
 */
public final class OpenObserveTracing {
  /** OTLP/gRPC port of OpenObserve. */
  public static final int DEFAULT_OTLP_GRPC_PORT = 5081;

  private OpenObserveTracing() {}

  /**
   * Returns the exporter factory for {@link Tracing#initWith}.
   *
   * @param config client configuration (URL, org, credentials, traces stream)
   * @return the factory
   */
  public static SpanExporterFactory exporterFactory(OpenObserveConfig config) {
    Objects.requireNonNull(config, "config");
    return tc -> OtlpExporters.create(configure(config, tc));
  }

  /**
   * Fills endpoint, headers and TLS of a tracing configuration from the client configuration.
   *
   * @param config client configuration
   * @param tc tracing configuration
   * @return the completed tracing configuration
   * @throws ConfigException for an invalid client configuration; {@code native} becomes OTLP/HTTP
   *     protobuf
   */
  public static TracingConfig configure(OpenObserveConfig config, TracingConfig tc) {
    config.validate();
    boolean grpc = tc.protocol() == TracingConfig.Protocol.GRPC;
    TracingConfig.Builder b = tc.toBuilder();
    if (tc.protocol() == TracingConfig.Protocol.NATIVE) {
      // OpenObserve's own ingestion format is OTLP over HTTP; the official Java exporter encodes
      // it as protobuf (OTLP/JSON is TypeScript/Rust only).
      b.protocol(TracingConfig.Protocol.HTTP_PROTOBUF);
    }
    if (tc.endpoint().isEmpty()) {
      if (grpc) {
        URI base = URI.create(config.url());
        b.endpoint(base.getScheme() + "://" + base.getHost() + ":" + DEFAULT_OTLP_GRPC_PORT);
      } else {
        b.endpoint(config.url().replaceAll("/+$", "") + "/api/" + config.org() + "/v1/traces");
      }
    }
    Map<String, String> headers = tc.headers();
    setIfAbsent(b, headers, "Authorization", config.authorization());
    setIfAbsent(b, headers, "stream-name", Values.normalizeKey(config.tracesStream()));
    if (grpc) {
      setIfAbsent(b, headers, "organization", config.org());
    }
    if (tc.tls().isEmpty()) {
      b.tls(config.tls());
    }
    return b.build();
  }

  private static void setIfAbsent(
      TracingConfig.Builder b, Map<String, String> headers, String name, String value) {
    for (String k : headers.keySet()) {
      if (k.equalsIgnoreCase(name)) {
        return;
      }
    }
    b.header(name, value);
  }

  /**
   * Starts OpenTelemetry tracing that exports to OpenObserve; call {@link TracingHandle#shutdown()}
   * before exit.
   *
   * @param config client configuration
   * @param tracing tracing configuration (service name, sampler, batch, …)
   * @return the handle
   */
  public static TracingHandle initTracing(OpenObserveConfig config, TracingConfig tracing) {
    return Tracing.initWith(tracing, exporterFactory(config));
  }
}
