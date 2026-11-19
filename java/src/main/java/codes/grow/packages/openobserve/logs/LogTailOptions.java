package codes.grow.packages.openobserve.logs;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Options of {@link Logs#tail}. Defaults: start now, poll every second, re-read a 1 s overlap
 * (never before the start), 1000 rows per request, logs stream, errors ignored.
 */
public final class LogTailOptions {
  private final String filter;
  private final Instant start;
  private final Duration interval;
  private final Duration overlap;
  private final int pageSize;
  private final StreamType streamType;
  private final Consumer<Throwable> onError;

  private LogTailOptions(Builder b) {
    this.filter = b.filter;
    this.start = b.start;
    this.interval = positive(b.interval, Duration.ofSeconds(1));
    this.overlap = b.overlap == null || b.overlap.isNegative() ? RowTailer.OVERLAP : b.overlap;
    this.pageSize = b.pageSize <= 0 ? RowTailer.PAGE_SIZE : b.pageSize;
    this.streamType = StreamType.orLogs(b.streamType);
    this.onError = b.onError == null ? e -> {} : b.onError;
  }

  private static Duration positive(Duration d, Duration fallback) {
    return d == null || d.isNegative() || d.isZero() ? fallback : d;
  }

  /**
   * Returns the defaults.
   *
   * @return defaults
   */
  public static LogTailOptions defaults() {
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
   * Returns the SQL condition.
   *
   * @return the condition, or empty for every row
   */
  public Optional<String> filter() {
    return Optional.ofNullable(filter).map(String::strip).filter(f -> !f.isEmpty());
  }

  /**
   * Returns the earliest delivered row time.
   *
   * @return start, or empty for now
   */
  public Optional<Instant> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns the poll interval.
   *
   * @return the interval
   */
  public Duration interval() {
    return interval;
  }

  /**
   * Returns the re-read window before the cursor.
   *
   * @return the overlap
   */
  public Duration overlap() {
    return overlap;
  }

  /**
   * Returns the rows per poll request.
   *
   * @return the page size
   */
  public int pageSize() {
    return pageSize;
  }

  /**
   * Returns the stream type.
   *
   * @return the stream type
   */
  public StreamType streamType() {
    return streamType;
  }

  /**
   * Returns the error callback.
   *
   * @return the callback
   */
  public Consumer<Throwable> onError() {
    return onError;
  }

  /** Builder of {@link LogTailOptions}. */
  public static final class Builder {
    private String filter;
    private Instant start;
    private Duration interval;
    private Duration overlap;
    private int pageSize;
    private StreamType streamType;
    private Consumer<Throwable> onError;

    private Builder() {}

    /**
     * Sets a SQL condition such as {@code match_all('error')}.
     *
     * @param filter condition
     * @return this builder
     */
    public Builder filter(String filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Sets the earliest delivered row time.
     *
     * @param start start
     * @return this builder
     */
    public Builder start(Instant start) {
      this.start = start;
      return this;
    }

    /**
     * Sets the poll interval.
     *
     * @param interval interval
     * @return this builder
     */
    public Builder interval(Duration interval) {
      this.interval = interval;
      return this;
    }

    /**
     * Sets the re-read window that catches late rows (duplicates are dropped).
     *
     * @param overlap overlap
     * @return this builder
     */
    public Builder overlap(Duration overlap) {
      this.overlap = overlap;
      return this;
    }

    /**
     * Sets the rows per poll request.
     *
     * @param pageSize page size
     * @return this builder
     */
    public Builder pageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    /**
     * Sets the stream type.
     *
     * @param streamType stream type
     * @return this builder
     */
    public Builder streamType(StreamType streamType) {
      this.streamType = streamType;
      return this;
    }

    /**
     * Sets the callback for failed polls (tailing continues).
     *
     * @param onError callback
     * @return this builder
     */
    public Builder onError(Consumer<Throwable> onError) {
      this.onError = onError;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public LogTailOptions build() {
      return new LogTailOptions(this);
    }
  }
}
