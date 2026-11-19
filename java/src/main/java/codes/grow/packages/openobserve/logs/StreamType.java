package codes.grow.packages.openobserve.logs;

/** Type of an OpenObserve stream (the {@code type} query parameter). */
public enum StreamType {
  /** Log streams (default). */
  LOGS("logs"),
  /** Trace (span) streams. */
  TRACES("traces"),
  /** Metric streams. */
  METRICS("metrics"),
  /** Internal metadata streams. */
  METADATA("metadata");

  private final String wire;

  StreamType(String wire) {
    this.wire = wire;
  }

  /**
   * Returns the wire value ({@code logs}, {@code traces}, …).
   *
   * @return the wire value
   */
  public String wireValue() {
    return wire;
  }

  /**
   * Parses a wire value; unknown values map to {@link #LOGS}.
   *
   * @param text wire value
   * @return the type
   */
  public static StreamType parse(String text) {
    for (StreamType t : values()) {
      if (t.wire.equalsIgnoreCase(text)) {
        return t;
      }
    }
    return LOGS;
  }

  /**
   * Returns {@code type}, or {@link #LOGS} when null.
   *
   * @param type type or null
   * @return the type
   */
  static StreamType orLogs(StreamType type) {
    return type == null ? LOGS : type;
  }
}
