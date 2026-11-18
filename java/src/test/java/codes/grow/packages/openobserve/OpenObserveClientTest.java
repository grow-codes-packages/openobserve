package codes.grow.packages.openobserve;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import codes.grow.packages.openobserve.core.AuthenticationException;
import codes.grow.packages.openobserve.core.CancelledException;
import codes.grow.packages.openobserve.core.ConfigException;
import codes.grow.packages.openobserve.core.EnvReader;
import codes.grow.packages.openobserve.core.HealthStatus;
import codes.grow.packages.openobserve.core.Hooks;
import codes.grow.packages.openobserve.core.Logger;
import codes.grow.packages.openobserve.core.MockServer;
import codes.grow.packages.openobserve.core.RetryPolicy;
import codes.grow.packages.openobserve.core.Secret;
import codes.grow.packages.openobserve.core.TlsOptions;
import codes.grow.packages.openobserve.core.UnsupportedException;
import codes.grow.packages.openobserve.logging.BatchingLogClient;
import codes.grow.packages.openobserve.logging.LogInput;
import codes.grow.packages.openobserve.logs.JsonLogExporter;
import codes.grow.packages.openobserve.logs.OtlpLogExporter;
import codes.grow.packages.openobserve.testing.ConformanceReport;
import codes.grow.packages.openobserve.testing.ConformanceRunner;
import codes.grow.packages.openobserve.tracing.TracingConfig;
import codes.grow.packages.openobserve.tracing.TracingHandle;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenObserveClientTest {
  private static final String P = EnvReader.DEFAULT_PREFIX;

  private MockServer server;

  @BeforeEach
  void start() throws Exception {
    server = MockServer.start();
  }

  @AfterEach
  void stop() {
    server.close();
  }

  private static OpenObserveConfig.Builder cfg() {
    return OpenObserveConfig.builder().user(Fixtures.USER).password(Fixtures.PASSWORD);
  }

  private OpenObserveClient client() {
    return Fixtures.client(server).build();
  }

  private static String basic(String user, String secret) {
    return "Basic "
        + Base64.getEncoder()
            .encodeToString((user + ":" + secret).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void healthUsesHealthzWithoutCredentials() {
    Fixtures.enqueue(server, "health.ok");
    server.enqueue(503, "{\"status\":\"down\"}");
    try (var c = client()) {
      HealthStatus h = c.health();
      assertThat(h.healthy()).isTrue();
      assertThat(h.status()).isEqualTo("ok");
      assertThat(c.healthAsync().join().healthy()).isFalse();
      assertThat(server.requests().get(0).uri()).isEqualTo("/healthz");
      assertThat(server.requests().get(0).header("Authorization")).isNull();
    }
  }

  @Test
  void core_health_unreachable() throws Exception {
    int port;
    try (ServerSocket s = new ServerSocket(0)) {
      port = s.getLocalPort();
    }
    try (var c = OpenObserveClient.create(cfg().url("http://127.0.0.1:" + port).build())) {
      HealthStatus h = c.health();
      assertThat(h.healthy()).isFalse();
      assertThat(h.status()).isEqualTo("unreachable");
    }
  }

  @Test
  void healthThrowsOnlyForCancel() {
    OpenObserveClient c = client();
    c.close();
    assertThatThrownBy(c::health).isInstanceOf(CancelledException.class);
  }

  @Test
  void pingSendsBasicAuthToConfig() {
    Fixtures.enqueue(server, "ping.ok");
    Fixtures.enqueue(server, "ping.401");
    Fixtures.enqueue(server, "ping.ok");
    try (var c = client()) {
      assertThat(c.ping()).isPositive();
      MockServer.Recorded r = server.requests().get(0);
      assertThat(r.uri()).isEqualTo("/api/default/config");
      assertThat(r.header("Authorization")).isEqualTo(basic(Fixtures.USER, Fixtures.PASSWORD));
      assertThatThrownBy(c::ping)
          .isInstanceOf(AuthenticationException.class)
          .hasMessageContaining("Unauthorized Access")
          .hasMessageContaining("HTTP 401");
      assertThat(c.pingAsync().join()).isPositive();
    }
  }

  @Test
  void serverVersion() {
    Fixtures.enqueue(server, "ping.ok");
    server.enqueue(200, "{\"build_type\":\"opensource\"}");
    server.enqueue(200, "{\"version\":\"\"}");
    Fixtures.enqueue(server, "ping.ok");
    try (var c = client()) {
      assertThat(c.serverVersion()).isEqualTo("v1.1.2");
      assertThatThrownBy(c::serverVersion).isInstanceOf(UnsupportedException.class);
      assertThatThrownBy(c::serverVersion).isInstanceOf(UnsupportedException.class);
      assertThat(c.serverVersionAsync().join()).isEqualTo("v1.1.2");
    }
  }

  @Test
  void tokenAuthentication() {
    server.enqueue(200, "{}").enqueue(200, "{}");
    try (var c =
        OpenObserveClient.builder()
            .url(server.url())
            .user("sa@example.com")
            .token("sa-token-1234")
            .build()) {
      c.ping();
      assertThat(server.requests().get(0).header("Authorization"))
          .isEqualTo(basic("sa@example.com", "sa-token-1234"));
    }
    String pre = basic("x@example.com", "pre-encoded");
    try (var c = OpenObserveClient.builder().url(server.url()).token(Secret.of(pre)).build()) {
      c.ping();
      assertThat(server.requests().get(1).header("Authorization")).isEqualTo(pre);
      assertThat(c.config().knownSecrets()).contains(pre);
    }
  }

  @Test
  void core_close_idempotent() throws Exception {
    AtomicInteger closed = new AtomicInteger();
    OpenObserveClient c = client();
    c.register(closed::incrementAndGet);
    c.register(
        () -> {
          throw new IllegalStateException("ignored");
        });
    c.close();
    c.close();
    assertThat(c.isClosed()).isTrue();
    assertThat(closed).hasValue(1);
    assertThat(c.transport().isClosed()).isTrue();
    assertThatThrownBy(c::ping).isInstanceOf(CancelledException.class);
  }

  @Test
  void closeFlushesLogClients() {
    Fixtures.enqueue(server, "ingestJson.ok");
    OpenObserveClient c = client();
    BatchingLogClient lc = c.logClient();
    lc.send(LogInput.of("closing"));
    c.close();
    assertThat(lc.stats().sent()).isPositive();
    assertThat(server.requests().get(0).uri()).isEqualTo("/api/default/default/_json");
  }

  @Test
  void modulesAndExporters() {
    try (var c = client()) {
      assertThat(c.logs()).isSameAs(c.logs());
      assertThat(c.streams()).isNotNull();
      assertThat(c.admin()).isNotNull();
      assertThat(c.logQuery().stream()).isEqualTo("default");
      assertThat(c.logQuery("My-Stream").stream()).isEqualTo("my_stream");
      assertThat(c.logQuery(null).stream()).isEqualTo("default");
      assertThat(c.traceQuery().stream()).isEqualTo("default");
      assertThat(c.traceQuery("T.x").stream()).isEqualTo("t_x");
      assertThat(c.traceQuery(null).stream()).isEqualTo("default");
      assertThat(c.logExporter(null, false)).isInstanceOf(JsonLogExporter.class);
      assertThat(c.logExporter("s", true)).isInstanceOf(OtlpLogExporter.class);
      BatchingLogClient otlp =
          c.logClient(
              LogClientOptions.builder().stream("other")
                  .protocol(LogClientOptions.Protocol.OTLP)
                  .resource(Map.of("service.name", "svc"))
                  .build());
      assertThat(otlp.resource()).containsEntry("service.name", "svc");
      assertThat(c.logClient(null)).isNotNull();
    }
  }

  @Test
  void initTracingExportsToOpenObserve() {
    server.fallback(ex -> MockServer.send(ex, 200, ""));
    try (var c = client()) {
      TracingHandle h =
          c.initTracing(
              TracingConfig.builder("svc")
                  .setGlobal(false)
                  .compression(TracingConfig.Compression.NONE)
                  .build());
      h.tracer("t").spanBuilder("op").startSpan().end();
      h.forceFlush(Duration.ofSeconds(10));
      h.shutdown();
      MockServer.Recorded r = server.requests().get(0);
      assertThat(r.uri()).isEqualTo("/api/default/v1/traces");
      assertThat(r.header("Authorization")).isEqualTo(basic(Fixtures.USER, Fixtures.PASSWORD));
      assertThat(r.header("stream-name")).isEqualTo("default");
    }
  }

  @Test
  void builderSetsEveryField() {
    Hooks hooks = new Hooks() {};
    Logger logger = new Logger() {};
    try (var c =
        OpenObserveClient.builder()
            .url(server.url())
            .org("acme")
            .user("u@example.com")
            .password(Secret.of("s3cr3t-value"))
            .stream("logs-a")
            .tracesStream("traces-a")
            .connectTimeout(Duration.ofSeconds(1))
            .requestTimeout(Duration.ofSeconds(2))
            .totalTimeout(Duration.ofSeconds(3))
            .retry(RetryPolicy.none())
            .tls(TlsOptions.defaults())
            .proxy("http://proxy.invalid:3128")
            .header("X-A", "1")
            .userAgentSuffix("app/1")
            .hooks(hooks)
            .logger(logger)
            .build()) {
      OpenObserveConfig cfg = c.config();
      assertThat(cfg.url()).isEqualTo(server.url());
      assertThat(cfg.org()).isEqualTo("acme");
      assertThat(cfg.user()).contains("u@example.com");
      assertThat(cfg.password()).isPresent();
      assertThat(cfg.token()).isEmpty();
      assertThat(cfg.stream()).isEqualTo("logs-a");
      assertThat(cfg.tracesStream()).isEqualTo("traces-a");
      assertThat(cfg.connectTimeout()).isEqualTo(Duration.ofSeconds(1));
      assertThat(cfg.requestTimeout()).isEqualTo(Duration.ofSeconds(2));
      assertThat(cfg.totalTimeout()).isEqualTo(Duration.ofSeconds(3));
      assertThat(cfg.retry().maxRetries()).isZero();
      assertThat(cfg.tls().isCustom()).isFalse();
      assertThat(cfg.proxy()).contains("http://proxy.invalid:3128");
      assertThat(cfg.headers()).containsEntry("X-A", "1");
      assertThat(cfg.userAgentSuffix()).contains("app/1");
      assertThat(cfg.hooks()).isSameAs(hooks);
      assertThat(cfg.logger()).isSameAs(logger);
      assertThat(c.toString()).doesNotContain("s3cr3t-value");
      assertThat(c.transport().config().knownSecrets()).contains("s3cr3t-value");
      cfg.password().get().clear();
      assertThat(c.transport().config().knownSecrets()).isEmpty();
    }
    try (var c =
        OpenObserveClient.builder()
            .url(server.url())
            .user("u@example.com")
            .password("pw-1234")
            .token((String) null)
            .token((Secret) null)
            .build()) {
      assertThat(c.config().password()).isPresent();
    }
  }

  @Test
  void core_redaction_config() {
    OpenObserveConfig cfg =
        OpenObserveConfig.builder()
            .url("http://user:pw@h:1")
            .user("sa@example.com")
            .token("tok-very-secret")
            .header("Authorization", "Bearer tok-very-secret")
            .proxy("http://u:proxypass@p:3128")
            .build();
    assertThat(cfg.toString())
        .doesNotContain("tok-very-secret")
        .doesNotContain("proxypass")
        .doesNotContain(":pw@")
        .contains("token=[REDACTED]")
        .contains("password=null");
    assertThat(cfg().build().toString())
        .contains("password=[REDACTED]")
        .contains("token=null")
        .contains("proxy=null")
        .doesNotContain(Fixtures.PASSWORD);
  }

  @Test
  void core_config_env_precedence() {
    Map<String, String> env = new HashMap<>();
    env.put(P + "URL", "http://from-env:1");
    env.put("OPENOBSERVE_URL", "http://vendor:1");
    env.put(P + "ORG", "grow-org");
    env.put("OPENOBSERVE_ORG", "vendor-org");
    env.put("OPENOBSERVE_USER", "vendor@example.com");
    env.put("OPENOBSERVE_PASSWORD", "vendor-pw");
    env.put("OPENOBSERVE_STREAM", "vendor_stream");
    env.put(P + "TRACES_STREAM", "grow_traces");
    env.put(P + "CONNECT_TIMEOUT", "1500");
    env.put(P + "REQUEST_TIMEOUT", "2s");
    env.put(P + "TOTAL_TIMEOUT", "1m");
    env.put(P + "MAX_RETRIES", "5");
    env.put(P + "CA_CERT", "/tmp/ca.pem");
    env.put(P + "CLIENT_CERT", "/tmp/c.pem");
    env.put(P + "CLIENT_KEY", "/tmp/k.pem");
    env.put(P + "INSECURE_SKIP_VERIFY", "on");
    env.put(P + "PROXY", "http://proxy:1");
    OpenObserveConfig fromEnv = OpenObserveConfig.fromEnv(env);
    assertThat(fromEnv.url()).isEqualTo("http://from-env:1");
    assertThat(fromEnv.org()).isEqualTo("grow-org");
    assertThat(fromEnv.user()).contains("vendor@example.com");
    assertThat(fromEnv.password().map(Secret::reveal)).contains("vendor-pw");
    assertThat(fromEnv.stream()).isEqualTo("vendor_stream");
    assertThat(fromEnv.tracesStream()).isEqualTo("grow_traces");
    assertThat(fromEnv.connectTimeout()).isEqualTo(Duration.ofMillis(1500));
    assertThat(fromEnv.requestTimeout()).isEqualTo(Duration.ofSeconds(2));
    assertThat(fromEnv.totalTimeout()).isEqualTo(Duration.ofMinutes(1));
    assertThat(fromEnv.retry().maxRetries()).isEqualTo(5);
    assertThat(fromEnv.tls().insecureSkipVerify()).isTrue();
    assertThat(fromEnv.tls().caCertPath()).isPresent();
    assertThat(fromEnv.proxy()).contains("http://proxy:1");
    env.remove(P + "URL");
    assertThat(OpenObserveConfig.fromEnv(env).url()).isEqualTo("http://vendor:1");
    // explicit values win over the environment
    OpenObserveConfig explicit =
        OpenObserveConfig.builder()
            .url("http://explicit:2")
            .org("o2")
            .user("e@example.com")
            .token("explicit-token")
            .stream("s2")
            .tracesStream("t2")
            .connectTimeout(Duration.ofSeconds(9))
            .requestTimeout(Duration.ofSeconds(9))
            .totalTimeout(Duration.ofSeconds(9))
            .retry(RetryPolicy.none())
            .tls(TlsOptions.defaults())
            .proxy("http://p2:1")
            .fromEnv(EnvReader.of(env))
            .build();
    assertThat(explicit.url()).isEqualTo("http://explicit:2");
    assertThat(explicit.org()).isEqualTo("o2");
    assertThat(explicit.token().map(Secret::reveal)).contains("explicit-token");
    assertThat(explicit.password()).isEmpty();
    assertThat(explicit.stream()).isEqualTo("s2");
    assertThat(explicit.tracesStream()).isEqualTo("t2");
    assertThat(explicit.retry().maxRetries()).isZero();
    assertThat(explicit.tls().insecureSkipVerify()).isFalse();
    assertThat(explicit.proxy()).contains("http://p2:1");
    // defaults when only credentials are set
    OpenObserveConfig defaults =
        OpenObserveConfig.fromEnv(Map.of(P + "USER", "u@example.com", P + "TOKEN", "tok-1234"));
    assertThat(defaults.url()).isEqualTo(OpenObserveConfig.DEFAULT_URL);
    assertThat(defaults.org()).isEqualTo("default");
    assertThat(defaults.stream()).isEqualTo("default");
    assertThat(defaults.tracesStream()).isEqualTo("default");
    assertThat(defaults.token().map(Secret::reveal)).contains("tok-1234");
    assertThat(defaults.connectTimeout()).isEqualTo(Duration.ofSeconds(10));
    assertThat(defaults.requestTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(defaults.totalTimeout()).isEqualTo(Duration.ofSeconds(120));
    assertThat(defaults.retry().maxRetries()).isEqualTo(3);
    // invalid values name the variable, never the value
    Map<String, String> badTimeout = Map.of(P + "TOTAL_TIMEOUT", "forever");
    assertThatThrownBy(() -> OpenObserveConfig.fromEnv(badTimeout))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(P + "TOTAL_TIMEOUT")
        .message()
        .doesNotContain("forever");
    Map<String, String> badRetries = Map.of(P + "MAX_RETRIES", "-1");
    assertThatThrownBy(() -> OpenObserveConfig.fromEnv(badRetries))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(P + "MAX_RETRIES");
    try {
      assertThat(OpenObserveConfig.fromEnv()).isNotNull();
    } catch (ConfigException e) {
      assertThat(e).hasMessageContaining("config:");
    }
  }

  @Test
  void validation() {
    assertThatThrownBy(() -> cfg().url("ftp://x").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().url("http://[").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().url("mailto:x").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().connectTimeout(Duration.ZERO).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().requestTimeout(Duration.ofSeconds(-1)).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().totalTimeout(Duration.ZERO).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().tls(TlsOptions.builder().clientCertPem("x").build()).build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().proxy("relative").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().proxy("http://[").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().org("a/b").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("org");
    assertThatThrownBy(() -> cfg().org(" ").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().token("t").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("not both");
    assertThatThrownBy(() -> OpenObserveConfig.builder().user("u").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("password or token is required");
    assertThatThrownBy(() -> OpenObserveConfig.builder().password("p").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("user is required");
    assertThatThrownBy(() -> OpenObserveConfig.builder().user(" ").token("raw").build())
        .isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().stream(" ").build()).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> cfg().tracesStream("").build()).isInstanceOf(ConfigException.class);
    assertThat(cfg().password((String) null).token("t-1234").build().token()).isPresent();
    assertThat(cfg().password((Secret) null).token("t-1234").build().password()).isEmpty();
  }

  @Test
  void createAndFromEnv() {
    try (var c = OpenObserveClient.create(cfg().url(server.url()).build())) {
      assertThat(c.isClosed()).isFalse();
    }
    try (var c = OpenObserveClient.fromEnv()) {
      assertThat(c.config()).isNotNull();
    } catch (ConfigException e) {
      assertThat(e).hasMessageContaining("config:");
    }
    EnvReader env =
        EnvReader.of(
            Map.of(
                P + "URL", server.url(), P + "USER", "u@example.com", P + "PASSWORD", "pw-1234"));
    try (var c = OpenObserveClient.builder().fromEnv(env).build()) {
      assertThat(c.config().url()).isEqualTo(server.url());
    }
  }

  @Test
  void coreConformanceAgainstMock() {
    server.fallback(
        ex -> {
          String path = ex.getRequestURI().getPath();
          if (path.equals("/healthz")) {
            MockServer.send(ex, 200, Fixtures.body("health.ok"));
          } else if (path.equals("/api/default/config")) {
            MockServer.send(ex, 200, Fixtures.body("ping.ok"));
          } else {
            MockServer.send(ex, 404, "{}");
          }
        });
    ConformanceReport report =
        ConformanceRunner.builder()
            .target("core", s -> client())
            .capabilities("version")
            .only("core.health")
            .build()
            .run();
    report.assertPassed();
    assertThat(report.with(ConformanceReport.Status.PASSED)).hasSize(1);
  }
}
