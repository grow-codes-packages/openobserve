package codes.grow.packages.openobserve;

import codes.grow.packages.openobserve.logging.BatchOptions;
import codes.grow.packages.openobserve.logging.RedactionProcessor;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Options of {@link OpenObserveClient#logClient(LogClientOptions)}: target stream (default the
 * configured stream), exporter protocol ({@code _json} by default, or OTLP/HTTP JSON), batching,
 * resource attributes and redaction.
 */
public final class LogClientOptions {
  /** Exporter protocol of a log client. */
  public enum Protocol {
    /** The {@code _json} ingest API (default; per-stream counters). */
    JSON,
    /** OTLP/HTTP JSON to {@code /api/{org}/v1/logs}. */
    OTLP
  }

  private final String stream;
  private final Protocol protocol;
  private final BatchOptions batchOptions;
  private final Map<String, Object> resource;
  private final RedactionProcessor redaction;

  private LogClientOptions(Builder b) {
    this.stream = b.stream;
    this.protocol = b.protocol;
    this.batchOptions = b.batchOptions;
    this.resource = Map.copyOf(b.resource);
    this.redaction = b.redaction;
  }

  /**
   * Defaults: configured stream, {@code _json}, default batching, no resource, default redaction.
   *
   * @return defaults
   */
  public static LogClientOptions defaults() {
    return builder().build();
  }

  /**
   * Starts a builder.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the target stream.
   *
   * @return the stream, or empty for the configured stream
   */
  public Optional<String> stream() {
    return Optional.ofNullable(stream);
  }

  /**
   * Returns the exporter protocol.
   *
   * @return the protocol
   */
  public Protocol protocol() {
    return protocol;
  }

  /**
   * Returns the batching options.
   *
   * @return the options
   */
  public BatchOptions batchOptions() {
    return batchOptions;
  }

  /**
   * Returns the resource attributes added to every record.
   *
   * @return immutable map
   */
  public Map<String, Object> resource() {
    return resource;
  }

  /**
   * Returns the redaction processor.
   *
   * @return the processor
   */
  public RedactionProcessor redaction() {
    return redaction;
  }

  /** Builder of {@link LogClientOptions}. */
  public static final class Builder {
    private String stream;
    private Protocol protocol = Protocol.JSON;
    private BatchOptions batchOptions = BatchOptions.defaults();
    private final Map<String, Object> resource = new LinkedHashMap<>();
    private RedactionProcessor redaction = RedactionProcessor.defaults();

    private Builder() {}

    /**
     * Sets the target stream.
     *
     * @param stream stream name
     * @return this builder
     */
    public Builder stream(String stream) {
      this.stream = stream;
      return this;
    }

    /**
     * Sets the exporter protocol.
     *
     * @param protocol protocol
     * @return this builder
     */
    public Builder protocol(Protocol protocol) {
      this.protocol = protocol == null ? Protocol.JSON : protocol;
      return this;
    }

    /**
     * Sets the batching options.
     *
     * @param options options
     * @return this builder
     */
    public Builder batchOptions(BatchOptions options) {
      this.batchOptions = options == null ? BatchOptions.defaults() : options;
      return this;
    }

    /**
     * Adds resource attributes ({@code service.name}, {@code deployment.environment}, …).
     *
     * @param resource attributes
     * @return this builder
     */
    public Builder resource(Map<String, ?> resource) {
      this.resource.putAll(resource);
      return this;
    }

    /**
     * Sets the redaction processor (null disables redaction).
     *
     * @param redaction processor
     * @return this builder
     */
    public Builder redaction(RedactionProcessor redaction) {
      this.redaction = redaction;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public LogClientOptions build() {
      return new LogClientOptions(this);
    }
  }
}
