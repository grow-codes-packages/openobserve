package codes.grow.packages.openobserve.logs;

import java.time.Instant;
import java.util.Optional;

/**
 * Storage statistics of a stream.
 *
 * @param createdAt creation time
 * @param docCount number of records
 * @param fileCount number of files
 * @param storageSize storage size (MB, as reported)
 * @param compressedSize compressed size (MB, as reported)
 */
public record StreamStats(
    Optional<Instant> createdAt,
    long docCount,
    long fileCount,
    double storageSize,
    double compressedSize) {
  /**
   * Normalizes a null creation time.
   *
   * @param createdAt creation time
   * @param docCount records
   * @param fileCount files
   * @param storageSize size
   * @param compressedSize compressed size
   */
  public StreamStats {
    createdAt = createdAt == null ? Optional.empty() : createdAt;
  }
}
