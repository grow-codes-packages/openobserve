package codes.grow.packages.openobserve.logs;

import java.time.Instant;
import java.util.Optional;

/**
 * Options of {@link Logs#search}. Times default to the last 15 minutes; the SDK converts them to
 * Unix microseconds (the unit OpenObserve expects).
 */
public final class SqlSearchOptions {
  private final Instant start;
  private final Instant end;
  private final int from;
  private final int size;
  private final boolean trackTotalHits;
  private final StreamType streamType;

  private SqlSearchOptions(Builder b) {
    this.start = b.start;
    this.end = b.end;
    this.from = b.from;
    this.size = b.size;
    this.trackTotalHits = b.trackTotalHits;
    this.streamType = StreamType.orLogs(b.streamType);
  }

  /**
   * Defaults: last 15 minutes, offset 0, server page size, logs stream.
   *
   * @return defaults
   */
  public static SqlSearchOptions defaults() {
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
   * Returns the window start.
   *
   * @return start, or empty for {@code end - 15 min}
   */
  public Optional<Instant> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns the window end.
   *
   * @return end, or empty for now
   */
  public Optional<Instant> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns the row offset.
   *
   * @return offset
   */
  public int from() {
    return from;
  }

  /**
   * Returns the page size (0 = server default; a {@code LIMIT} in the SQL wins).
   *
   * @return page size
   */
  public int size() {
    return size;
  }

  /**
   * Returns whether a count request also fills {@link SqlSearchResult#total()}.
   *
   * @return true to count
   */
  public boolean trackTotalHits() {
    return trackTotalHits;
  }

  /**
   * Returns the type of the queried stream.
   *
   * @return stream type (default logs)
   */
  public StreamType streamType() {
    return streamType;
  }

  /** Builder of {@link SqlSearchOptions}. */
  public static final class Builder {
    private Instant start;
    private Instant end;
    private int from;
    private int size;
    private boolean trackTotalHits;
    private StreamType streamType;

    private Builder() {}

    /**
     * Sets the window start.
     *
     * @param start start
     * @return this builder
     */
    public Builder start(Instant start) {
      this.start = start;
      return this;
    }

    /**
     * Sets the window end.
     *
     * @param end end
     * @return this builder
     */
    public Builder end(Instant end) {
      this.end = end;
      return this;
    }

    /**
     * Sets the row offset.
     *
     * @param from offset (&gt;= 0)
     * @return this builder
     */
    public Builder from(int from) {
      this.from = from;
      return this;
    }

    /**
     * Sets the page size.
     *
     * @param size page size (0 = server default)
     * @return this builder
     */
    public Builder size(int size) {
      this.size = size;
      return this;
    }

    /**
     * Also counts every matching row (a second request).
     *
     * @param trackTotalHits true to count
     * @return this builder
     */
    public Builder trackTotalHits(boolean trackTotalHits) {
      this.trackTotalHits = trackTotalHits;
      return this;
    }

    /**
     * Sets the type of the queried stream.
     *
     * @param streamType type
     * @return this builder
     */
    public Builder streamType(StreamType streamType) {
      this.streamType = streamType;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return the options
     */
    public SqlSearchOptions build() {
      return new SqlSearchOptions(this);
    }
  }
}
