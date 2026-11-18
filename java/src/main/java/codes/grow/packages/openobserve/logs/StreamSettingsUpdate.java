package codes.grow.packages.openobserve.logs;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Changes to stream settings ({@link Streams#updateStreamSettings}). An empty field is left
 * unchanged; a present list replaces the current one (the SDK sends the add/remove deltas
 * OpenObserve expects). Removed partition keys stay in the schema as disabled.
 */
public final class StreamSettingsUpdate {
  private final StreamType type;
  private final Integer retentionDays;
  private final List<PartitionKey> partitionKeys;
  private final List<String> fullTextSearchKeys;

  private StreamSettingsUpdate(Builder b) {
    this.type = StreamType.orLogs(b.type);
    this.retentionDays = b.retentionDays;
    this.partitionKeys = b.partitionKeys == null ? null : List.copyOf(b.partitionKeys);
    this.fullTextSearchKeys =
        b.fullTextSearchKeys == null ? null : List.copyOf(b.fullTextSearchKeys);
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
   * Returns the stream type.
   *
   * @return type (default logs)
   */
  public StreamType type() {
    return type;
  }

  /**
   * Returns the new retention.
   *
   * @return days, or empty to keep it
   */
  public OptionalInt retentionDays() {
    return retentionDays == null ? OptionalInt.empty() : OptionalInt.of(retentionDays);
  }

  /**
   * Returns the wanted enabled partition keys.
   *
   * @return keys, or empty to keep them
   */
  public Optional<List<PartitionKey>> partitionKeys() {
    return Optional.ofNullable(partitionKeys);
  }

  /**
   * Returns the wanted full-text search keys.
   *
   * @return keys, or empty to keep them
   */
  public Optional<List<String>> fullTextSearchKeys() {
    return Optional.ofNullable(fullTextSearchKeys);
  }

  /** Builder of {@link StreamSettingsUpdate}. */
  public static final class Builder {
    private StreamType type;
    private Integer retentionDays;
    private List<PartitionKey> partitionKeys;
    private List<String> fullTextSearchKeys;

    private Builder() {}

    /**
     * Sets the stream type (default logs).
     *
     * @param type type
     * @return this builder
     */
    public Builder type(StreamType type) {
      this.type = type;
      return this;
    }

    /**
     * Sets the data retention in days.
     *
     * @param days days (&gt;= 0)
     * @return this builder
     */
    public Builder retentionDays(int days) {
      this.retentionDays = days;
      return this;
    }

    /**
     * Replaces the enabled partition keys.
     *
     * @param keys keys
     * @return this builder
     */
    public Builder partitionKeys(List<PartitionKey> keys) {
      this.partitionKeys = keys;
      return this;
    }

    /**
     * Replaces the full-text search keys.
     *
     * @param keys keys
     * @return this builder
     */
    public Builder fullTextSearchKeys(List<String> keys) {
      this.fullTextSearchKeys = keys;
      return this;
    }

    /**
     * Builds the update.
     *
     * @return the update
     */
    public StreamSettingsUpdate build() {
      return new StreamSettingsUpdate(this);
    }
  }
}
