// Created for this repository.
package codes.grow.packages.openobserve.examples;

import codes.grow.packages.openobserve.LogClientOptions;
import codes.grow.packages.openobserve.OpenObserveClient;
import codes.grow.packages.openobserve.logging.BatchingLogClient;
import codes.grow.packages.openobserve.logging.LogRecord;
import codes.grow.packages.openobserve.logging.Logger;
import codes.grow.packages.openobserve.logging.WaitForLogOptions;
import codes.grow.packages.openobserve.logs.StreamInfo;
import codes.grow.packages.openobserve.tracing.SpanOptions;
import codes.grow.packages.openobserve.tracing.Spans;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import codes.grow.packages.openobserve.tracing.query.Trace;
import codes.grow.packages.openobserve.tracing.query.TraceSearchQuery;
import java.time.Duration;
import java.util.Map;

/**
 * The category APIs: a batching log client with a logger, the log query client, OpenTelemetry
 * tracing exported over OTLP/HTTP and the trace query client.
 */
public final class LogsAndTracesExample {
  private LogsAndTracesExample() {}

  /**
   * Runs the example.
   *
   * @param args unused
   */
  public static void main(String[] args) {
    try (var client = OpenObserveClient.fromEnv()) {
      // Logs: batched, redacted (password/token attributes), flushed by client.close().
      BatchingLogClient logs =
          client.logClient(
              LogClientOptions.builder()
                  .stream("example_app")
                  .resource(Map.of("service.name", "example-app"))
                  .build());
      Logger log = logs.logger("checkout", Map.of("region", "eu"));
      log.info("checkout started", Map.of("cart", 3, "password", "never-sent"));
      logs.flush(Duration.ofSeconds(5));
      LogRecord found =
          client
              .logQuery("example_app")
              .waitForLog(
                  "checkout", WaitForLogOptions.of(Duration.ofSeconds(10), Duration.ofMillis(250)));
      System.out.println("found: " + found);
      StreamInfo info = client.streams().getStreamSchema("example_app");
      System.out.println("fields: " + info.schema());

      // Traces: OTLP/HTTP to /api/{org}/v1/traces with Basic auth and the stream-name header.
      TracingHandle tracing =
          client.initTracing(TracingConfig.builder("example-app").setGlobal(false).build());
      String traceId =
          Spans.withSpan(
              "checkout",
              SpanOptions.builder().tracer(tracing.tracer("example")).build(),
              () -> Spans.currentTraceId().orElseThrow());
      tracing.forceFlush(Duration.ofSeconds(5));
      Trace trace = client.traceQuery().waitForTrace(traceId);
      System.out.println("trace " + trace.traceId() + " has " + trace.spans().size() + " span(s)");
      System.out.println(
          "recent: " + client.traceQuery().searchTraces(TraceSearchQuery.forService("example-app")));
      tracing.shutdown();
    }
  }
}
