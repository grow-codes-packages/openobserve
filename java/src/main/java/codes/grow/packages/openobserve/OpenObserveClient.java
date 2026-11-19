package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.CancelledException;
import codes.grow.packages.openobserve.core.ConfigException;
import codes.grow.packages.openobserve.core.EnvReader;
import codes.grow.packages.openobserve.core.HealthCheck;
import codes.grow.packages.openobserve.core.HealthStatus;
import codes.grow.packages.openobserve.core.Hooks;
import codes.grow.packages.openobserve.core.HttpTransport;
import codes.grow.packages.openobserve.core.Logger;
import codes.grow.packages.openobserve.core.RetryPolicy;
import codes.grow.packages.openobserve.core.Secret;
import codes.grow.packages.openobserve.core.TlsOptions;
import codes.grow.packages.openobserve.core.TransportRequest;
import codes.grow.packages.openobserve.core.UnsupportedException;
import codes.grow.packages.openobserve.internal.Api;
import codes.grow.packages.openobserve.logging.BatchingLogClient;
import codes.grow.packages.openobserve.logging.LogExporter;
import codes.grow.packages.openobserve.logs.JsonLogExporter;
import codes.grow.packages.openobserve.logs.Logs;
import codes.grow.packages.openobserve.logs.OpenObserveLogQuery;
import codes.grow.packages.openobserve.logs.OtlpLogExporter;
import codes.grow.packages.openobserve.logs.Streams;
import codes.grow.packages.openobserve.traces.OpenObserveTraceQuery;
import codes.grow.packages.openobserve.traces.OpenObserveTracing;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Client for OpenObserve.
 *
 * <p>Implements SPEC section 2.4. Thread-safe; create one per endpoint and close it when done.
 *
 * <pre>{@code
 * try (OpenObserveClient client = OpenObserveClient.fromEnv()) {
 *   HealthStatus health = client.health();
 * }
 * }</pre>
 *
 * <p>Every I/O method has a blocking form and an {@code …Async} form returning {@link
 * CompletableFuture}. Service modules: {@link #logs()} (ingest and SQL search), {@link #streams()},
 * {@link #admin()}, {@link #logQuery()} (the {@code LogQueryClient}), {@link #traceQuery()} (the
 * {@code TraceQueryClient}), {@link #logClient()} (batching {@code LogClient}) and {@link
 * #initTracing(TracingConfig)}.
 */
public final class OpenObserveClient implements AutoCloseable {
  private final OpenObserveConfig config;
  private final HttpTransport transport;
  private final Api api;
  private final Logs logs;
  private final Streams streams;
  private final Admin admin;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final List<AutoCloseable> resources = new ArrayList<>();

  private OpenObserveClient(OpenObserveConfig config) {
    this.config = config;
    Set<String> runtimeSecrets = ConcurrentHashMap.newKeySet();
    this.transport = new HttpTransport(config.toTransportConfig(() -> List.copyOf(runtimeSecrets)));
    this.api = new Api(config, transport, runtimeSecrets);
    this.logs = new Logs(api);
    this.streams = new Streams(api);
    this.admin = new Admin(api);
  }

  /**
   * Creates a client.
   *
   * @param config validated configuration
   * @return a new client
   * @throws ConfigException when TLS or proxy settings are invalid
   */
  public static OpenObserveClient create(OpenObserveConfig config) {
    config.validate();
    return new OpenObserveClient(config);
  }

  /**
   * Creates a client from the SDK and vendor environment variables.
   *
   * @return a new client
   */
  public static OpenObserveClient fromEnv() {
    return create(OpenObserveConfig.fromEnv());
  }

  /**
   * Starts a client builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the configuration.
   *
   * @return the configuration
   */
  public OpenObserveConfig config() {
    return config;
  }

  /**
   * Returns the HTTP transport, for service modules built on this client.
   *
   * @return the transport
   */
  public HttpTransport transport() {
    return transport;
  }

  /**
   * Registers a resource (watcher, renewer, batch processor) closed with the client.
   *
   * @param resource the resource
   */
  public void register(AutoCloseable resource) {
    synchronized (resources) {
      resources.add(resource);
    }
  }

  /**
   * Returns the log ingestion and SQL search API.
   *
   * @return the logs module
   */
  public Logs logs() {
    return logs;
  }

  /**
   * Returns the stream management API.
   *
   * @return the streams module
   */
  public Streams streams() {
    return streams;
  }

  /**
   * Returns the organization and service-account helpers.
   *
   * @return the admin module
   */
  public Admin admin() {
    return admin;
  }

  /**
   * Returns the {@code LogQueryClient} for the configured stream.
   *
   * @return the query client
   */
  public OpenObserveLogQuery logQuery() {
    return logQuery(config.stream());
  }

  /**
   * Returns the {@code LogQueryClient} for a stream.
   *
   * @param stream stream name
   * @return the query client
   */
  public OpenObserveLogQuery logQuery(String stream) {
    return new OpenObserveLogQuery(api, stream == null ? config.stream() : stream);
  }

  /**
   * Returns the trace query module on the configured traces stream (same as {@link #traceQuery()}).
   *
   * @return the query client
   */
  public OpenObserveTraceQuery traces() {
    return traceQuery();
  }

  /**
   * Returns the {@code TraceQueryClient} for the configured traces stream.
   *
   * @return the query client
   */
  public OpenObserveTraceQuery traceQuery() {
    return traceQuery(config.tracesStream());
  }

  /**
   * Returns the {@code TraceQueryClient} for a traces stream.
   *
   * @param stream traces stream name
   * @return the query client
   */
  public OpenObserveTraceQuery traceQuery(String stream) {
    return new OpenObserveTraceQuery(api, stream == null ? config.tracesStream() : stream);
  }

  /**
   * Returns a log exporter for a stream: {@code _json} ingest, or OTLP/HTTP JSON when {@code otlp}.
   *
   * @param stream stream name (null = the configured stream)
   * @param otlp true for the OTLP exporter
   * @return the exporter
   */
  public LogExporter logExporter(String stream, boolean otlp) {
    String s = stream == null ? config.stream() : stream;
    return otlp ? new OtlpLogExporter(api, s) : new JsonLogExporter(api, s);
  }

  /**
   * Starts a batching {@code LogClient} that writes to the configured stream through {@code _json}.
   * {@link #close()} flushes and closes it.
   *
   * @return the log client
   */
  public BatchingLogClient logClient() {
    return logClient(LogClientOptions.defaults());
  }

  /**
   * Starts a batching {@code LogClient}. {@link #close()} flushes and closes it.
   *
   * @param options stream, protocol, batching, resource and redaction
   * @return the log client
   */
  public BatchingLogClient logClient(LogClientOptions options) {
    LogClientOptions o = options == null ? LogClientOptions.defaults() : options;
    BatchingLogClient client =
        BatchingLogClient.builder(
                logExporter(
                    o.stream().orElse(null), o.protocol() == LogClientOptions.Protocol.OTLP))
            .batchOptions(o.batchOptions())
            .resource(o.resource())
            .redaction(o.redaction())
            .build();
    register(client);
    return client;
  }

  /**
   * Starts OpenTelemetry tracing that exports to OpenObserve with this client's configuration (see
   * {@link OpenObserveTracing}); call {@link TracingHandle#shutdown()} before exit.
   *
   * @param tracing tracing configuration
   * @return the handle
   */
  public TracingHandle initTracing(TracingConfig tracing) {
    return OpenObserveTracing.initTracing(config, tracing);
  }

  /**
   * Checks liveness without credentials ({@code GET /healthz}). Never throws for an unhealthy or
   * unreachable service.
   *
   * @return the health status
   * @throws CancelledException when interrupted or closed
   */
  public HealthStatus health() {
    return HealthCheck.check(transport, "/healthz");
  }

  /**
   * Asynchronous {@link #health()}.
   *
   * @return a future with the health status
   */
  public CompletableFuture<HealthStatus> healthAsync() {
    return HealthCheck.checkAsync(transport, "/healthz");
  }

  /**
   * Performs an authenticated round trip ({@code GET /api/{org}/config}).
   *
   * @return the latency
   * @throws AuthenticationException on bad credentials
   */
  public Duration ping() {
    return HttpTransport.await(pingAsync(), "ping");
  }

  /**
   * Asynchronous {@link #ping()}.
   *
   * @return a future with the latency
   */
  public CompletableFuture<Duration> pingAsync() {
    long start = System.nanoTime();
    return api.sendAsync(TransportRequest.get(api.orgPath("/config")).operation("ping"))
        .thenApply(r -> Duration.ofNanos(System.nanoTime() - start));
  }

  /**
   * Returns the server version, for example {@code v1.1.2} ({@code GET /api/{org}/config},
   * authenticated).
   *
   * @return the version string
   * @throws UnsupportedException when the server exposes none
   */
  public String serverVersion() {
    return HttpTransport.await(serverVersionAsync(), "serverVersion");
  }

  /**
   * Asynchronous {@link #serverVersion()}.
   *
   * @return a future with the version
   */
  public CompletableFuture<String> serverVersionAsync() {
    return api.jsonAsync(TransportRequest.get(api.orgPath("/config")).operation("serverVersion"))
        .thenApply(OpenObserveClient::version);
  }

  private static String version(JsonNode body) {
    JsonNode v = body.get("version");
    if (v == null || !v.isTextual() || v.asText().isEmpty()) {
      throw new UnsupportedException("serverVersion: the server reports no version");
    }
    return v.asText();
  }

  /**
   * Returns whether {@link #close()} was called.
   *
   * @return true once closed
   */
  public boolean isClosed() {
    return closed.get();
  }

  /**
   * Flushes and closes every log client and registered resource, then the transport. Idempotent.
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    List<AutoCloseable> copy;
    synchronized (resources) {
      copy = new ArrayList<>(resources);
      resources.clear();
    }
    for (AutoCloseable r : copy) {
      try {
        r.close();
      } catch (Exception ignored) {
        // best effort: close must not throw
      }
    }
    transport.close();
  }

  @Override
  public String toString() {
    return "OpenObserveClient{" + config + "}";
  }

  /** Builder that configures and creates a client in one chain. */
  public static final class Builder {
    private final OpenObserveConfig.Builder config = OpenObserveConfig.builder();

    private Builder() {}

    /**
     * Fills unset fields from an environment reader.
     *
     * @param env reader, e.g. {@link EnvReader#system()}
     * @return this builder
     */
    public Builder fromEnv(EnvReader env) {
      config.fromEnv(env);
      return this;
    }

    /**
     * Sets the endpoint base URL.
     *
     * @param url URL
     * @return this builder
     */
    public Builder url(String url) {
      config.url(url);
      return this;
    }

    /**
     * Sets the organization.
     *
     * @param org organization
     * @return this builder
     */
    public Builder org(String org) {
      config.org(org);
      return this;
    }

    /**
     * Sets the user or service-account e-mail.
     *
     * @param user user
     * @return this builder
     */
    public Builder user(String user) {
      config.user(user);
      return this;
    }

    /**
     * Sets the user password.
     *
     * @param password password
     * @return this builder
     */
    public Builder password(String password) {
      config.password(password);
      return this;
    }

    /**
     * Sets the user password.
     *
     * @param password password
     * @return this builder
     */
    public Builder password(Secret password) {
      config.password(password);
      return this;
    }

    /**
     * Sets the default log stream.
     *
     * @param stream stream name
     * @return this builder
     */
    public Builder stream(String stream) {
      config.stream(stream);
      return this;
    }

    /**
     * Sets the traces stream.
     *
     * @param stream stream name
     * @return this builder
     */
    public Builder tracesStream(String stream) {
      config.tracesStream(stream);
      return this;
    }

    /**
     * Sets the service-account token (or a pre-encoded {@code Basic ...} value).
     *
     * @param token token
     * @return this builder
     */
    public Builder token(String token) {
      config.token(token);
      return this;
    }

    /**
     * Sets the service-account token (or a pre-encoded {@code Basic ...} value).
     *
     * @param token token
     * @return this builder
     */
    public Builder token(Secret token) {
      config.token(token);
      return this;
    }

    /**
     * Sets the connect timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      config.connectTimeout(d);
      return this;
    }

    /**
     * Sets the per-attempt timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      config.requestTimeout(d);
      return this;
    }

    /**
     * Sets the total timeout.
     *
     * @param d duration
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      config.totalTimeout(d);
      return this;
    }

    /**
     * Sets the retry policy.
     *
     * @param retry policy
     * @return this builder
     */
    public Builder retry(RetryPolicy retry) {
      config.retry(retry);
      return this;
    }

    /**
     * Sets the TLS options.
     *
     * @param tls options
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      config.tls(tls);
      return this;
    }

    /**
     * Sets the proxy URL.
     *
     * @param proxy URL
     * @return this builder
     */
    public Builder proxy(String proxy) {
      config.proxy(proxy);
      return this;
    }

    /**
     * Adds a header.
     *
     * @param name name
     * @param value value
     * @return this builder
     */
    public Builder header(String name, String value) {
      config.header(name, value);
      return this;
    }

    /**
     * Sets the user agent suffix.
     *
     * @param suffix suffix
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      config.userAgentSuffix(suffix);
      return this;
    }

    /**
     * Sets hooks.
     *
     * @param hooks hooks
     * @return this builder
     */
    public Builder hooks(Hooks hooks) {
      config.hooks(hooks);
      return this;
    }

    /**
     * Sets the logger.
     *
     * @param logger logger
     * @return this builder
     */
    public Builder logger(Logger logger) {
      config.logger(logger);
      return this;
    }

    /**
     * Builds the configuration and the client.
     *
     * @return a new client
     * @throws ConfigException when the configuration is invalid
     */
    public OpenObserveClient build() {
      return create(config.build());
    }
  }
}
