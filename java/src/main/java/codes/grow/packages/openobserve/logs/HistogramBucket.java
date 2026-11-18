package codes.grow.packages.openobserve.logs;

import java.time.Instant;

/**
 * One histogram bucket.
 *
 * @param start bucket start (UTC)
 * @param count number of records
 */
public record HistogramBucket(Instant start, long count) {}
