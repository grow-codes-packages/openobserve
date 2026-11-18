package codes.grow.packages.openobserve.logs;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Options of {@link Logs#histogram}.
 *
 * @param start window start (default {@code end - 15 min})
 * @param end window end (default now)
 * @param interval bucket width (empty = chosen by the server)
 * @param filter SQL {@code WHERE} condition, for example {@code level = 'error'}
 */
public record HistogramOptions(
    Optional<Instant> start,
    Optional<Instant> end,
    Optional<Duration> interval,
    Optional<String> filter) {
  /**
   * Normalizes null optionals.
   *
   * @param start start
   * @param end end
   * @param interval interval
   * @param filter filter
   */
  public HistogramOptions {
    start = start == null ? Optional.empty() : start;
    end = end == null ? Optional.empty() : end;
    interval = interval == null ? Optional.empty() : interval;
    filter = filter == null ? Optional.empty() : filter;
  }

  /**
   * Defaults: last 15 minutes, server interval, no filter.
   *
   * @return defaults
   */
  public static HistogramOptions defaults() {
    return new HistogramOptions(null, null, null, null);
  }

  /**
   * Options from nullable values.
   *
   * @param start start or null
   * @param end end or null
   * @param interval interval or null
   * @param filter filter or null
   * @return options
   */
  public static HistogramOptions of(Instant start, Instant end, Duration interval, String filter) {
    return new HistogramOptions(
        Optional.ofNullable(start),
        Optional.ofNullable(end),
        Optional.ofNullable(interval),
        Optional.ofNullable(filter));
  }
}
