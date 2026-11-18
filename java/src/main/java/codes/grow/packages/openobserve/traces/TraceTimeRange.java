package codes.grow.packages.openobserve.traces;

import java.time.Instant;
import java.util.Optional;

/**
 * Where and when the server's trace index saw a trace.
 *
 * @param traceId trace id
 * @param found false when the index has no entry for the trace
 * @param stream traces stream holding the trace
 * @param start first span time
 * @param end last span time
 */
public record TraceTimeRange(
    String traceId,
    boolean found,
    Optional<String> stream,
    Optional<Instant> start,
    Optional<Instant> end) {
  /**
   * Normalizes null optionals.
   *
   * @param traceId trace id
   * @param found found flag
   * @param stream stream
   * @param start start
   * @param end end
   */
  public TraceTimeRange {
    stream = stream == null ? Optional.empty() : stream;
    start = start == null ? Optional.empty() : start;
    end = end == null ? Optional.empty() : end;
  }
}
