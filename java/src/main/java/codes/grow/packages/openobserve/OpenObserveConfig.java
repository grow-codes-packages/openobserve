package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.core.ConfigException;
import codes.grow.packages.openobserve.core.EnvReader;
import codes.grow.packages.openobserve.core.Hooks;
import codes.grow.packages.openobserve.core.Logger;
import codes.grow.packages.openobserve.core.Redaction;
import codes.grow.packages.openobserve.core.RetryPolicy;
import codes.grow.packages.openobserve.core.Secret;
import codes.grow.packages.openobserve.core.TlsOptions;
import codes.grow.packages.openobserve.core.TransportConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Immutable configuration of a {@link OpenObserveClient} (SPEC section 2.2).
 *
 * <p>Build one with {@link #builder()} or read it from the environment with {@link #fromEnv()}.
 * Precedence: explicit builder value &gt; SDK variable ({@link EnvReader#DEFAULT_PREFIX} + name)
 * &gt; vendor variables &gt; default. {@link #toString()} never prints credentials.
 *
 * <p>OpenObserve accepts HTTP Basic authentication only. Give {@code user} plus exactly one of
 * {@code password} (a user password) or {@code token} (a service-account token, sent as {@code
 * Basic base64(user:token)}). A token that starts with {@code "Basic "} is a pre-encoded {@code
 * Authorization} value and is sent verbatim; {@code user} is then optional.
 *
 * <p>Environment variables (SDK name first, then the vendor name): {@code URL} / {@code
 * OPENOBSERVE_URL}, {@code ORG} / {@code OPENOBSERVE_ORG}, {@code USER} / {@code OPENOBSERVE_USER},
 * {@code PASSWORD} / {@code OPENOBSERVE_PASSWORD}, {@code TOKEN} / {@code OPENOBSERVE_TOKEN},
 * {@code STREAM} / {@code OPENOBSERVE_STREAM}, {@code TRACES_STREAM} / {@code
 * OPENOBSERVE_TRACES_STREAM}, each SDK name prefixed with {@code GROW_OPENOBSERVE_}.
 */
public final class OpenObserveConfig {
  /** Default endpoint. */
  public static final String DEFAULT_URL = "http://127.0.0.1:5080";

  /** Default organization. */
  public static final String DEFAULT_ORG = "default";

  /** Default log stream and traces stream (the server's own default for OTLP data). */
  public static final String DEFAULT_STREAM = "default";

  /** Vendor variables read for {@code url} after the SDK variable {@code URL}, in order. */
  static final String[] URL_VENDOR_VARS = {"OPENOBSERVE_URL"};

  /** Vendor variables read for {@code token} after the SDK variable {@code TOKEN}, in order. */
  static final String[] TOKEN_VENDOR_VARS = {"OPENOBSERVE_TOKEN"};

  /** Vendor variables read for the CA bundle after the SDK variable {@code CA_CERT}. */
  static final String[] CA_CERT_VENDOR_VARS = {};

  private final String url;
  private final String org;
  private final String user;
  private final Secret password;
  private final Secret token;
  private final String stream;
  private final String tracesStream;
  private final Duration connectTimeout;
  private final Duration requestTimeout;
  private final Duration totalTimeout;
  private final RetryPolicy retry;
  private final TlsOptions tls;
  private final String proxy;
  private final Map<String, String> headers;
  private final String userAgentSuffix;
  private final Hooks hooks;
  private final Logger logger;

  private OpenObserveConfig(Builder b) {
    this.url = b.url != null ? b.url : DEFAULT_URL;
    this.org = b.org != null ? b.org : DEFAULT_ORG;
    this.user = b.user;
    this.password = b.password;
    this.token = b.token;
    this.stream = b.stream != null ? b.stream : DEFAULT_STREAM;
    this.tracesStream = b.tracesStream != null ? b.tracesStream : DEFAULT_STREAM;
    this.connectTimeout = b.connectTimeout != null ? b.connectTimeout : Duration.ofSeconds(10);
    this.requestTimeout = b.requestTimeout != null ? b.requestTimeout : Duration.ofSeconds(30);
    this.totalTimeout = b.totalTimeout != null ? b.totalTimeout : Duration.ofSeconds(120);
    this.retry = b.retry != null ? b.retry : RetryPolicy.defaults();
    this.tls = b.tls != null ? b.tls : TlsOptions.defaults();
    this.proxy = b.proxy;
    this.headers = Map.copyOf(b.headers);
    this.userAgentSuffix = b.userAgentSuffix;
    this.hooks = b.hooks != null ? b.hooks : Hooks.NONE;
    this.logger = b.logger != null ? b.logger : Logger.NOOP;
  }

  /**
   * Starts an empty builder (every field at its default).
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Reads the configuration from the process environment.
   *
   * @return the validated configuration
   * @throws ConfigException when a variable holds an invalid value (the message names the variable,
   *     never the value)
   */
  public static OpenObserveConfig fromEnv() {
    return builder().fromEnv(EnvReader.system()).build();
  }

  /**
   * Reads the configuration from an explicit variable map (tests).
   *
   * @param env variable map
   * @return the validated configuration
   */
  public static OpenObserveConfig fromEnv(Map<String, String> env) {
    return builder().fromEnv(EnvReader.of(env)).build();
  }

  /**
   * Checks URL scheme, positive timeouts, retry limits, TLS pairs and the OpenObserve rules:
   * exactly one of password or token, a user unless the token is pre-encoded, an organization
   * without path characters and non-blank stream names.
   *
   * @throws ConfigException when a rule is violated
   */
  public void validate() {
    URI u;
    try {
      u = new URI(url);
    } catch (URISyntaxException e) {
      throw new ConfigException("config: url is not a valid URL");
    }
    String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
    if (!(scheme.equals("http") || scheme.equals("https")) || u.getHost() == null) {
      throw new ConfigException("config: url must use http or https and include a host");
    }
    requirePositive("connectTimeout", connectTimeout);
    requirePositive("requestTimeout", requestTimeout);
    requirePositive("totalTimeout", totalTimeout);
    tls.validate();
    if (proxy != null) {
      try {
        URI p = new URI(proxy);
        if (p.getHost() == null) {
          throw new ConfigException("config: proxy must be an absolute URL");
        }
      } catch (URISyntaxException e) {
        throw new ConfigException("config: proxy is not a valid URL");
      }
    }
    if (org.isBlank() || org.chars().anyMatch(c -> "/?#% \t\r\n".indexOf(c) >= 0)) {
      throw new ConfigException("config: org must not be blank or contain path characters");
    }
    if (password != null && token != null) {
      throw new ConfigException("config: set either password or token, not both");
    }
    if (password == null && token == null) {
      throw new ConfigException(
          "config: password or token is required (OpenObserve uses HTTP Basic authentication)");
    }
    if ((user == null || user.isBlank()) && !preEncoded()) {
      throw new ConfigException(
          "config: user is required with a password or a raw service-account token");
    }
    if (stream.isBlank() || tracesStream.isBlank()) {
      throw new ConfigException("config: stream names must not be blank");
    }
  }

  private boolean preEncoded() {
    return token != null && !token.isCleared() && token.reveal().startsWith("Basic ");
  }

  /**
   * Returns the {@code Authorization} header value: the pre-encoded token as is, otherwise {@code
   * Basic base64(user:password)} or {@code Basic base64(user:token)}.
   *
   * @return the header value (a credential: never log it)
   */
  public String authorization() {
    if (preEncoded()) {
      return token.reveal();
    }
    Secret secret = password != null ? password : token;
    String pair = (user == null ? "" : user) + ":" + secret.reveal();
    return "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the credential values that must never appear in messages: password, token and the
   * encoded {@code Authorization} value.
   *
   * @return secret values
   */
  public List<String> knownSecrets() {
    List<String> out = new ArrayList<>();
    for (Secret s : new Secret[] {password, token}) {
      if (s != null && !s.isCleared() && !s.isEmpty()) {
        out.add(s.reveal());
      }
    }
    if (!out.isEmpty()) {
      String auth = authorization();
      out.add(auth);
      out.add(auth.substring(auth.indexOf(' ') + 1));
    }
    return out;
  }

  private static void requirePositive(String name, Duration d) {
    if (d.isZero() || d.isNegative()) {
      throw new ConfigException("config: " + name + " must be positive");
    }
  }

  /**
   * Translates the common fields into transport settings.
   *
   * @return the transport configuration
   */
  public TransportConfig toTransportConfig() {
    return toTransportConfig(List::of);
  }

  /**
   * Translates the common fields into transport settings with extra secrets to redact.
   *
   * @param extraSecrets secrets learned at run time
   * @return the transport configuration
   */
  TransportConfig toTransportConfig(Supplier<Collection<String>> extraSecrets) {
    return TransportConfig.builder(url)
        .connectTimeout(connectTimeout)
        .requestTimeout(requestTimeout)
        .totalTimeout(totalTimeout)
        .retry(retry)
        .tls(tls)
        .proxy(proxy)
        .headers(headers)
        .userAgentSuffix(userAgentSuffix)
        .hooks(hooks)
        .logger(logger)
        .knownSecrets(
            () -> {
              List<String> all = knownSecrets();
              all.addAll(extraSecrets.get());
              return all;
            })
        .build();
  }

  /**
   * Returns the endpoint base URL.
   *
   * @return the URL
   */
  public String url() {
    return url;
  }

  /**
   * Returns the organization identifier (default {@code default}).
   *
   * @return the organization
   */
  public String org() {
    return org;
  }

  /**
   * Returns the user or service-account e-mail.
   *
   * @return the user, or empty
   */
  public Optional<String> user() {
    return Optional.ofNullable(user);
  }

  /**
   * Returns the user password.
   *
   * @return the password, or empty
   */
  public Optional<Secret> password() {
    return Optional.ofNullable(password);
  }

  /**
   * Returns the service-account token (or pre-encoded {@code Basic ...} value).
   *
   * @return the token, or empty
   */
  public Optional<Secret> token() {
    return Optional.ofNullable(token);
  }

  /**
   * Returns the default log stream (default {@code default}).
   *
   * @return the stream name
   */
  public String stream() {
    return stream;
  }

  /**
   * Returns the stream spans are exported to and queried from (default {@code default}).
   *
   * @return the traces stream name
   */
  public String tracesStream() {
    return tracesStream;
  }

  /**
   * Returns the connect timeout.
   *
   * @return the timeout
   */
  public Duration connectTimeout() {
    return connectTimeout;
  }

  /**
   * Returns the per-attempt timeout.
   *
   * @return the timeout
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /**
   * Returns the total timeout per call including retries.
   *
   * @return the timeout
   */
  public Duration totalTimeout() {
    return totalTimeout;
  }

  /**
   * Returns the retry policy.
   *
   * @return the policy
   */
  public RetryPolicy retry() {
    return retry;
  }

  /**
   * Returns the TLS options.
   *
   * @return the options
   */
  public TlsOptions tls() {
    return tls;
  }

  /**
   * Returns the explicit proxy URL.
   *
   * @return the proxy, or empty
   */
  public Optional<String> proxy() {
    return Optional.ofNullable(proxy);
  }

  /**
   * Returns the extra request headers.
   *
   * @return immutable map
   */
  public Map<String, String> headers() {
    return headers;
  }

  /**
   * Returns the user agent suffix.
   *
   * @return the suffix, or empty
   */
  public Optional<String> userAgentSuffix() {
    return Optional.ofNullable(userAgentSuffix);
  }

  /**
   * Returns the hooks.
   *
   * @return the hooks
   */
  public Hooks hooks() {
    return hooks;
  }

  /**
   * Returns the logger.
   *
   * @return the logger
   */
  public Logger logger() {
    return logger;
  }

  @Override
  public String toString() {
    return "OpenObserveConfig{url="
        + Redaction.redactUrl(url)
        + ", org="
        + org
        + ", user="
        + user
        + ", password="
        + (password == null ? "null" : Redaction.REDACTED)
        + ", token="
        + (token == null ? "null" : Redaction.REDACTED)
        + ", stream="
        + stream
        + ", tracesStream="
        + tracesStream
        + ", connectTimeout="
        + connectTimeout
        + ", requestTimeout="
        + requestTimeout
        + ", totalTimeout="
        + totalTimeout
        + ", retry="
        + retry
        + ", tls="
        + tls
        + ", proxy="
        + (proxy == null ? "null" : Redaction.redactUrl(proxy))
        + ", headers="
        + Redaction.redactHeaders(headers)
        + ", userAgentSuffix="
        + userAgentSuffix
        + "}";
  }

  /**
   * Builder of the configuration.
   *
   * <p>Unset fields take the environment (when {@link #fromEnv(EnvReader)} is called) or the
   * default.
   */
  public static final class Builder {
    private String url;
    private String org;
    private String user;
    private Secret password;
    private Secret token;
    private String stream;
    private String tracesStream;
    private Duration connectTimeout;
    private Duration requestTimeout;
    private Duration totalTimeout;
    private RetryPolicy retry;
    private TlsOptions tls;
    private String proxy;
    private final Map<String, String> headers = new LinkedHashMap<>();
    private String userAgentSuffix;
    private Hooks hooks;
    private Logger logger;

    private Builder() {}

    /**
     * Fills every field that was not set explicitly from the environment.
     *
     * @param env environment reader
     * @return this builder
     * @throws ConfigException when a variable holds an invalid value
     */
    public Builder fromEnv(EnvReader env) {
      if (url == null) {
        url = env.string("URL", URL_VENDOR_VARS).orElse(null);
      }
      if (org == null) {
        org = env.string("ORG", "OPENOBSERVE_ORG").orElse(null);
      }
      if (user == null) {
        user = env.string("USER", "OPENOBSERVE_USER").orElse(null);
      }
      if (password == null && token == null) {
        password = env.string("PASSWORD", "OPENOBSERVE_PASSWORD").map(Secret::of).orElse(null);
        token = env.string("TOKEN", TOKEN_VENDOR_VARS).map(Secret::of).orElse(null);
      }
      if (stream == null) {
        stream = env.string("STREAM", "OPENOBSERVE_STREAM").orElse(null);
      }
      if (tracesStream == null) {
        tracesStream = env.string("TRACES_STREAM", "OPENOBSERVE_TRACES_STREAM").orElse(null);
      }
      if (connectTimeout == null) {
        connectTimeout = env.duration("CONNECT_TIMEOUT").orElse(null);
      }
      if (requestTimeout == null) {
        requestTimeout = env.duration("REQUEST_TIMEOUT").orElse(null);
      }
      if (totalTimeout == null) {
        totalTimeout = env.duration("TOTAL_TIMEOUT").orElse(null);
      }
      if (retry == null && env.integer("MAX_RETRIES").isPresent()) {
        int n = env.integer("MAX_RETRIES").getAsInt();
        if (n < 0) {
          String variable = EnvReader.DEFAULT_PREFIX + "MAX_RETRIES";
          throw new ConfigException("config: invalid value in " + variable + " (expected >= 0)");
        }
        retry = RetryPolicy.builder().maxRetries(n).build();
      }
      if (tls == null) {
        TlsOptions.Builder t = TlsOptions.builder();
        env.string("CA_CERT", CA_CERT_VENDOR_VARS).ifPresent(p -> t.caCertPath(Paths.get(p)));
        env.string("CLIENT_CERT").ifPresent(p -> t.clientCertPath(Paths.get(p)));
        env.string("CLIENT_KEY").ifPresent(p -> t.clientKeyPath(Paths.get(p)));
        env.bool("INSECURE_SKIP_VERIFY").ifPresent(t::insecureSkipVerify);
        tls = t.build();
      }
      if (proxy == null) {
        proxy = env.string("PROXY").orElse(null);
      }
      return this;
    }

    /**
     * Sets the endpoint base URL.
     *
     * @param url {@code http} or {@code https} URL
     * @return this builder
     */
    public Builder url(String url) {
      this.url = url;
      return this;
    }

    /**
     * Sets the organization identifier (default {@code default}).
     *
     * @param org organization
     * @return this builder
     */
    public Builder org(String org) {
      this.org = org;
      return this;
    }

    /**
     * Sets the user or service-account e-mail.
     *
     * @param user user
     * @return this builder
     */
    public Builder user(String user) {
      this.user = user;
      return this;
    }

    /**
     * Sets the user password.
     *
     * @param password password
     * @return this builder
     */
    public Builder password(Secret password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the user password.
     *
     * @param password password
     * @return this builder
     */
    public Builder password(String password) {
      this.password = password == null ? null : Secret.of(password);
      return this;
    }

    /**
     * Sets the default log stream (default {@code default}).
     *
     * @param stream stream name
     * @return this builder
     */
    public Builder stream(String stream) {
      this.stream = stream;
      return this;
    }

    /**
     * Sets the traces stream (default {@code default}).
     *
     * @param tracesStream stream name
     * @return this builder
     */
    public Builder tracesStream(String tracesStream) {
      this.tracesStream = tracesStream;
      return this;
    }

    /**
     * Sets the service-account token (sent as {@code Basic base64(user:token)}) or a pre-encoded
     * {@code Basic ...} value.
     *
     * @param token token
     * @return this builder
     */
    public Builder token(Secret token) {
      this.token = token;
      return this;
    }

    /**
     * Sets the service-account token (sent as {@code Basic base64(user:token)}) or a pre-encoded
     * {@code Basic ...} value.
     *
     * @param token token
     * @return this builder
     */
    public Builder token(String token) {
      this.token = token == null ? null : Secret.of(token);
      return this;
    }

    /**
     * Sets the connect timeout (default 10 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder connectTimeout(Duration d) {
      this.connectTimeout = d;
      return this;
    }

    /**
     * Sets the per-attempt timeout (default 30 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder requestTimeout(Duration d) {
      this.requestTimeout = d;
      return this;
    }

    /**
     * Sets the total timeout per call including retries (default 120 s).
     *
     * @param d positive duration
     * @return this builder
     */
    public Builder totalTimeout(Duration d) {
      this.totalTimeout = d;
      return this;
    }

    /**
     * Sets the retry policy (default {@link RetryPolicy#defaults()}).
     *
     * @param retry policy
     * @return this builder
     */
    public Builder retry(RetryPolicy retry) {
      this.retry = retry;
      return this;
    }

    /**
     * Sets the TLS options; environment TLS variables are then ignored.
     *
     * @param tls options
     * @return this builder
     */
    public Builder tls(TlsOptions tls) {
      this.tls = tls;
      return this;
    }

    /**
     * Sets an explicit proxy URL; environment proxies ({@code HTTPS_PROXY}) are never used.
     *
     * @param proxy proxy URL
     * @return this builder
     */
    public Builder proxy(String proxy) {
      this.proxy = proxy;
      return this;
    }

    /**
     * Adds a header sent with every request.
     *
     * @param name header name
     * @param value header value
     * @return this builder
     */
    public Builder header(String name, String value) {
      this.headers.put(name, value);
      return this;
    }

    /**
     * Sets the text appended to the {@code User-Agent}.
     *
     * @param suffix suffix
     * @return this builder
     */
    public Builder userAgentSuffix(String suffix) {
      this.userAgentSuffix = suffix;
      return this;
    }

    /**
     * Sets request hooks.
     *
     * @param hooks hooks
     * @return this builder
     */
    public Builder hooks(Hooks hooks) {
      this.hooks = hooks;
      return this;
    }

    /**
     * Sets the logger (default silent).
     *
     * @param logger logger
     * @return this builder
     */
    public Builder logger(Logger logger) {
      this.logger = logger;
      return this;
    }

    /**
     * Builds and validates the configuration.
     *
     * @return the configuration
     * @throws ConfigException when a rule is violated
     */
    public OpenObserveConfig build() {
      OpenObserveConfig c = new OpenObserveConfig(this);
      c.validate();
      return c;
    }
  }
}
