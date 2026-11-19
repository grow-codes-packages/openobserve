package codes.grow.packages.openobserve.integration;

import codes.grow.packages.openobserve.LogClientOptions;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.core.EnvReader;
import codes.grow.packages.openobserve.logging.BatchOptions;
import codes.grow.packages.openobserve.logging.BatchingLogClient;
import codes.grow.packages.openobserve.testing.ConformanceRunner;
import codes.grow.packages.openobserve.testing.logging.LoggingTarget;
import codes.grow.packages.openobserve.testing.tracing.TracingTarget;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import java.util.Map;

/**
 * Conformance targets of this service (owned by the service repository).
 *
 * <p>{@code core} is the client, {@code logging} a batching {@code LogClient} on the bootstrap
 * stream ({@code OPENOBSERVE_STREAM}) with the stream's {@code LogQueryClient}, {@code tracing} an
 * OTLP/HTTP tracer provider exporting to the traces stream ({@code OPENOBSERVE_TRACES_STREAM})
 * with its {@code TraceQueryClient}. {@code dependencies} is left out: the service graph is an
 * enterprise feature of OpenObserve (docs/compatibility.md).
 */
final class Factories {
  /** Capabilities the deployment under test offers (see the scenario {@code requires}). */
  static final String[] CAPABILITIES = {"version", "query", "tail", "labels", "streams", "services"};

  /** Service name of the tracing scenarios. */
  static final String SERVICE_NAME = "grow-conformance-java";

  private Factories() {}

  static void register(ConformanceRunner.Builder runner, EnvReader env) {
    runner.serviceName(SERVICE_NAME);
    runner.target("core", scenario -> OpenObserveClient.builder().fromEnv(env).build());
    runner.target(
        "logging",
        scenario -> {
          OpenObserveClient client = OpenObserveClient.builder().fromEnv(env).build();
          BatchingLogClient logs =
              client.logClient(
                  LogClientOptions.builder()
                      .batchOptions(LoggingTarget.batchOptions(scenario, BatchOptions.defaults()))
                      .resource(Map.of("service.name", SERVICE_NAME))
                      .build());
          return LoggingTarget.of(logs, client.logQuery());
        });
    runner.target(
        "tracing",
        scenario -> {
          OpenObserveClient client = OpenObserveClient.builder().fromEnv(env).build();
          TracingHandle handle =
              client.initTracing(TracingConfig.builder(SERVICE_NAME).setGlobal(false).build());
          return TracingTarget.of(handle, client.traceQuery());
        });
  }
}
