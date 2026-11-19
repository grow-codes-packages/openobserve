package codes.grow.packages.openobserve.logs;

import java.time.Duration;
import java.util.List;

/**
 * Record count per time bucket.
 *
 * @param interval bucket width reported by the server
 * @param buckets buckets in time order
 */
public record HistogramResult(Duration interval, List<HistogramBucket> buckets) {
  /**
   * Copies the buckets.
   *
   * @param interval interval
   * @param buckets buckets
   */
  public HistogramResult {
    buckets = List.copyOf(buckets);
  }
}
