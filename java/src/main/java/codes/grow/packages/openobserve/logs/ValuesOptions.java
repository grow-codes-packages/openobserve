package codes.grow.packages.openobserve.logs;

import java.time.Instant;
import java.util.Optional;

/**
 * Options of {@link Logs#valuesOf}: time window (default the last 15 minutes) and the maximum
 * number of distinct values per field (default 10).
 *
 * @param start window start or empty
 * @param end window end or empty
 * @param size values per field (&lt;= 0 means 10)
 */
public record ValuesOptions(Optional<Instant> start, Optional<Instant> end, int size) {
  /**
   * Normalizes null optionals.
   *
   * @param start start
   * @param end end
   * @param size size
   */
  public ValuesOptions {
    start = start == null ? Optional.empty() : start;
    end = end == null ? Optional.empty() : end;
  }

  /**
   * Defaults: last 15 minutes, 10 values per field.
   *
   * @return defaults
   */
  public static ValuesOptions defaults() {
    return new ValuesOptions(null, null, 10);
  }

  /**
   * Options for a window.
   *
   * @param start start or null
   * @param end end or null
   * @param size values per field
   * @return options
   */
  public static ValuesOptions of(Instant start, Instant end, int size) {
    return new ValuesOptions(Optional.ofNullable(start), Optional.ofNullable(end), size);
  }
}
